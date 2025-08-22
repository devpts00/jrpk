#[cfg(test)]
use crate::init_tracing;
use std::cmp::min;
use futures::stream::{SplitSink, SplitStream};
use rand::{Rng, SeedableRng};
use random_data::{DataGenerator, DataType};
use std::io::{Read, Write};
use std::ops::Add;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use bytes::{BufMut, BytesMut};
use serde::Deserialize;
use serde_json::Value;
use tokio::net::{TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncBufReadExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace};
use crate::errors::JrpkError;
use crate::kafka::KfkResId;
use crate::util::handle_future;

fn generate_send_req(buf: &mut Vec<u8>) {
    let mut rng = rand::rng();
    let mut gen = DataGenerator::new();
    let size: usize = rng.random_range(10..20);
    let mut comma = false;
    let id: u32 = rng.random();
    write!(buf, r#"{{ "jsonrpc": "2.0", "id": {}, "method": "send", "params": {{ "topic": "posts", "partition": 0, "records": ["#, id).unwrap();
    for n in 0..size {
        if comma {
            buf.push(b',');
        } else {
            comma = true;
        }
        let key: String = DataType::Country.random(&mut gen);
        let first: String = DataType::FirstName.random(&mut gen);
        let last: String = DataType::LastName.random(&mut gen);
        let age: u8 = rng.random_range(10..80);
        write!(buf, r#"{{"key": "{}", "value": {{ "first": "{}", "last": "{}", "age": {} }}}}"#, key, first, last, age).unwrap();
    }
    buf.extend_from_slice(b"] }}");
}

async fn producer_writer(wh: OwnedWriteHalf) -> Result<(), anyhow::Error> {
    info!("produce - write, start");
    let mut buf: Vec<u8> = Vec::with_capacity(4 * 1024);
    let mut writer = tokio::io::BufWriter::with_capacity(4 * 1024, wh);
    let total: usize = rand::rng().random_range(100..200);
    for n in 0..total {
        buf.clear();
        generate_send_req(&mut buf);
        writer.write_all(&buf).await?;
        writer.flush().await?;
    }
    info!("produce - write, end");
    Ok(())
}

async fn producer_reader(rh: OwnedReadHalf) -> Result<(), anyhow::Error> {
    info!("produce - read, start");
    use tokio::io::BufReader;
    let mut reader = BufReader::with_capacity(4 * 1024, rh);
    let mut line = String::with_capacity(4 * 1024);
    while let Ok(n) = reader.read_line(&mut line).await {
        if n > 0 {
            line.pop();
            info!("produce - read, response: {}", line);
            line.clear()
        } else {
            break;
        }
    }
    info!("produce - read, end");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_produce() -> Result<(), JrpkError> {
    info!("produce, start");
    init_tracing();
    let parallelism = 50;
    let mut tasks: Vec<JoinHandle<()>> = Vec::with_capacity(2 * parallelism);
    for _ in 0..parallelism {
        let addr = "jrpk:1133";
        let stream = TcpStream::connect(addr).await?;
        let (rh, wh) = stream.into_split();
        let wh = tokio::spawn(async move {
            producer_writer(wh).await.unwrap();
        });
        tasks.push(wh);
        let rh = tokio::spawn(async move {
            producer_reader(rh).await.unwrap();
        });
        tasks.push(rh);
    }
    info!("produce, wait...");
    for h in tasks {
        h.await?;
    }
    info!("produce, end");
    Ok(())
}

fn get_offset_from_offset(line: &str) -> Result<Option<i64>, serde_json::Error> {
    serde_json::from_str(line).map(|json: Value| {
        json.as_object()
            .and_then(|obj| obj.get("result"))
            .and_then(|res| res.as_object())
            .and_then(|obj| obj.get("offset"))
            .and_then(|offset| offset.as_i64())
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TestJrpRspData {
    Fetch {
        next_offset: Option<i64>,
        high_watermark: i64,
    }
}

#[derive(Debug, Deserialize)]
struct TestJrpRsp {
    result: TestJrpRspData,
}

fn get_offset_from_fetch(line: &str) -> serde_json::Result<TestJrpRsp> {
    serde_json::from_str::<TestJrpRsp>(line)
}

async fn consumer_reader(rh: OwnedReadHalf, snd: Sender<i64>) -> Result<(), JrpkError> {
    info!("consume - read, start");
    let mut reader = BufReader::with_capacity(4 * 1024, rh);
    let mut line = String::with_capacity(4 * 1024);
    let mut offset_read = false;
    while let Ok(n) = reader.read_line(&mut line).await {
        if n > 0 {
            line.pop();
            if !offset_read {
                info!("consume - read, len: {}, response: {}", line.len(), line);
                if let Some(offset) = get_offset_from_offset(&line)? {
                    info!("reader, offset: {}", offset);
                    snd.send(offset).await?;
                    offset_read = true;
                }
            } else {
                trace!("consume - read, len: {}, response: {}", line.len(), &line);
                let rsp = get_offset_from_fetch(&line)?;
                match rsp.result {
                    TestJrpRspData::Fetch { next_offset: Some(offset), high_watermark } => {
                        trace!("consume - read, offset: {}, high_watermark: {}", offset, high_watermark);
                        if (offset < high_watermark) {
                            snd.send(offset).await?;
                        }
                    }
                    TestJrpRspData::Fetch { next_offset: None, high_watermark } => {
                        trace!("reader, offset: None, high_watermark: {}", high_watermark);
                    }
                }
            }
            line.clear()
        }
    }
    info!("consume - read, end");
    Ok(())
}

async fn consumer_writer(wh: OwnedWriteHalf, mut rcv: Receiver<i64>) -> Result<(), anyhow::Error> {
    info!("writer, start");
    let mut buf: Vec<u8> = Vec::with_capacity(4 * 1024);
    let mut writer = tokio::io::BufWriter::with_capacity(4 * 1024, wh);

    // write earliest offset request
    write!(&mut buf, r#"{{ "jsonrpc": "2.0", "id": 0, "method": "offset", "params": {{ "topic": "posts", "partition": 0, "at": "earliest" }} }}"#)?;
    writer.write_all(&buf).await?;
    writer.flush().await?;
    buf.clear();

    // receive offsets and write fetch requests
    while let Some(offset) = rcv.recv().await {
        trace!("writer, offset: {}", offset);
        write!(&mut buf, r#"{{ "jsonrpc": "2.0", "id": 0, "method": "fetch", "params": {{ "topic": "posts", "partition": 0, "offset": {}, "bytes": {{ "start": 10000, "end": 100000 }}, "max_wait_ms": 1000 }} }}"#, offset)?;
        writer.write_all(&buf).await?;
        writer.flush().await?;
        buf.clear();
    }

    info!("writer, end");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_consume() -> Result<(), JrpkError> {
    use tokio::net::{TcpStream};
    info!("consume, start");
    init_tracing();
    let parallelism = 50;
    let mut tasks: Vec<JoinHandle<()>> = Vec::with_capacity(2 * parallelism);
    for _ in 0..parallelism {
        let addr = "jrpk:1133";
        let stream = TcpStream::connect(addr).await?;
        let (rh, wh) = stream.into_split();
        let (snd, rcv) = tokio::sync::mpsc::channel::<i64>(8);
        let wh = tokio::spawn(async move {
            consumer_writer(wh, rcv).await.unwrap();
        });
        tasks.push(wh);
        let rh = tokio::spawn(async move {
            consumer_reader(rh, snd).await.unwrap();
        });
        tasks.push(rh);
    }
    info!("consume, wait...");
    for h in tasks {
        h.await?;
    }
    info!("consume, end");
    Ok(())
}
