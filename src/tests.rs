#[cfg(test)]
use crate::init_tracing;
use std::cmp::min;
use futures::stream::{SplitSink, SplitStream};
use rand::{Rng, SeedableRng};
use random_data::{DataGenerator, DataType};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::ops::{Add, Range};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use bytes::{BufMut, BytesMut};
use rand::rngs::ThreadRng;
use serde::Deserialize;
use serde_json::Value;
use tokio::net::{TcpSocket, TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncBufReadExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, error, info, trace};
use log::warn;
use crate::kafka::KfkResCtx;
use crate::util::handle_future_result;

fn generate_send_req_valid(buf: &mut Vec<u8>, rng: &mut ThreadRng, gen: &mut DataGenerator) {
    let key: String = DataType::Country.random(gen);
    let first: String = DataType::FirstName.random(gen);
    let last: String = DataType::LastName.random(gen);
    let age: u8 = rng.random_range(10..80);
    write!(buf, r#"{{"key": {{ "json": "{}" }}, "value": {{ "json": {{ "first": "{}", "last": "{}", "age": {} }} }} }}"#, key, first, last, age).unwrap();
}

fn generate_send_req_invalid(buf: &mut Vec<u8>, rng: &mut ThreadRng, gen: &mut DataGenerator) {
    let key: String = DataType::FirstName.random(gen);
    let first: String = DataType::FirstName.random(gen);
    let last: String = DataType::LastName.random(gen);
    let age: u8 = rng.random_range(10..80);
    write!(buf, r#"{{"key": "{}", "value": {{ "json": {{ "first": "{}", "last": "{}", "age": {} }} }} }}"#, key, first, last, age).unwrap();
}

fn generate_send_reqs(buf: &mut Vec<u8>, rec_count_per_send: u16, partition_count: u8) {
    buf.clear();
    let partitions = 120;
    let mut rng = rand::rng();
    let mut gen = DataGenerator::new();
    let mut comma = false;
    let id: u32 = rng.random();
    let partition = rng.random_range(0..partition_count);
    write!(buf, r#"{{ "jsonrpc": "2.0", "id": {}, "method": "send", "params": {{ "topic": "posts", "partition": {}, "records": ["#, id, partition).unwrap();
    for n in 0..rec_count_per_send {
        if comma {
            buf.push(b',');
        } else {
            comma = true;
        }
        generate_send_req_invalid(buf, &mut rng, &mut gen);
    }
    buf.extend_from_slice(b"] }}");
}

async fn producer_writer(wh: OwnedWriteHalf, partition_count: u8, send_count_per_producer: u16, rec_count_per_send: u16) -> Result<(), anyhow::Error> {
    info!("produce - write, start");
    let mut buf: Vec<u8> = Vec::with_capacity(4 * 1024);
    let mut writer = tokio::io::BufWriter::with_capacity(4 * 1024, wh);
    let total: usize = 1;
    for n in 0..send_count_per_producer {
        generate_send_reqs(&mut buf, rec_count_per_send, partition_count);
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
async fn test_produce() -> Result<(), anyhow::Error> {
    info!("produce, start");
    init_tracing();
    let partition_count = 120;
    let send_count_per_producer = 100;
    let rec_count_per_send = 100;
    let parallelism = 120;
    let mut tasks: Vec<JoinHandle<()>> = Vec::with_capacity(2 * parallelism);
    for _ in 0..parallelism {
        let addr = "jrpk:1133";
        let stream = TcpStream::connect(addr).await?;
        let (rh, wh) = stream.into_split();
        let wh = tokio::spawn(async move {
            producer_writer(wh, partition_count, send_count_per_producer, rec_count_per_send).await.unwrap();
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
enum TestJrpRspResult {
    Send {
        offsets: Vec<i64>,
    },
    Fetch {
        next_offset: Option<i64>,
        high_watermark: i64,
    },
    Offset(i64)
}

#[derive(Debug, Deserialize)]
struct TestJrpRsp {
    id: usize,
    result: TestJrpRspResult,
}

async fn consumer_read(
    addr: SocketAddr,
    rh: OwnedReadHalf,
    snd: Sender<i64>
) -> Result<(), anyhow::Error> {
    let mut reader = BufReader::with_capacity(4 * 1024, rh);
    let mut line = String::with_capacity(4 * 1024);
    while let Ok(n) = reader.read_line(&mut line).await {
        if n > 0 {
            break;
        }
        line.pop();
        trace!("read, ctx: {}, line: {}", addr, &line);
        let rsp = serde_json::from_str::<TestJrpRsp>(&line)?;
        match rsp.result {
            TestJrpRspResult::Send { offsets } => {
                warn!("read, ctx: {}, offsets: {:?}", addr, offsets);
            }
            TestJrpRspResult::Fetch { next_offset: Some(offset), high_watermark } => {
                info!("read, ctx: {}, offset: {}, high_watermark: {}", addr, offset, high_watermark);
                if offset < high_watermark {
                    snd.send(offset).await?;
                } else {
                    break;
                }
            }
            TestJrpRspResult::Fetch { next_offset: None, high_watermark } => {
                info!("read, ctx: {}, offset: None, high_watermark: {}", addr, high_watermark);
                break;
            }
            TestJrpRspResult::Offset(offset) => {
                snd.send(offset).await?;
            }
        }
        line.clear()
    }
    Ok(())
}

async fn consumer_write(
    addr: SocketAddr,
    wh: OwnedWriteHalf,
    mut rcv: Receiver<i64>,
    partition: u8,
    byte_size_min: u32,
    byte_size_max: u32
) -> Result<(), anyhow::Error> {
    let mut buf: Vec<u8> = Vec::with_capacity(4 * 1024);
    let mut writer = tokio::io::BufWriter::with_capacity(4 * 1024, wh);

    // write earliest offset request
    write!(&mut buf, r#"{{ "jsonrpc": "2.0", "id": 0, "method": "offset", "params": {{ "topic": "posts", "partition": {}, "at": "earliest" }} }}"#, partition)?;
    writer.write_all(&buf).await?;
    writer.flush().await?;
    buf.clear();

    let mut id: usize = 1;
    // receive offsets and write fetch requests
    while let Some(offset) = rcv.recv().await {
        id = id + 1;
        info!("writer, ctx: {}, offset: {}", addr, offset);
        write!(&mut buf, r#"{{ "jsonrpc": "2.0", "id": {}, "method": "fetch", "params": {{ "topic": "posts", "partition": {}, "codecs": {{ "key": "json", "value": "json" }}, "offset": {}, "bytes": {{ "start": {}, "end": {} }}, "max_wait_ms": 1000 }} }}"#,
               id, partition, offset, byte_size_min, byte_size_max)?;
        writer.write_all(&buf).await?;
        writer.flush().await?;
        buf.clear();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_consume() {
    init_tracing();
    info!("consume, main - START");
    let partition_count: u8 = 120;
    let byte_size_min: u32 = 1000;
    let byte_size_max: u32 = 1000000;
    let mut tasks: Vec<JoinHandle<()>> = Vec::with_capacity(2 * partition_count as usize);
    for partition in 0..partition_count {
        let stream = TcpStream::connect("jrpk:1133").await.unwrap();
        let addr = stream.peer_addr().unwrap();
        let (rh, wh) = stream.into_split();
        let (snd, rcv) = tokio::sync::mpsc::channel::<i64>(8);
        let wh = tokio::spawn(
            handle_future_result("write", addr, consumer_write(addr, wh, rcv, partition, byte_size_min, byte_size_max))
        );
        tasks.push(wh);
        let rh = tokio::spawn(
            handle_future_result("read", addr, consumer_read(addr, rh, snd))
        );
        tasks.push(rh);
    }
    info!("consume, main - WAIT");
    for h in tasks {
        h.await.ok();
    }
    info!("consume, main - END");
}
