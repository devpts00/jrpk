use crate::args::HostPort;
use crate::jsonrpc::{JrpError, JrpFramer, JrpRequest, JrpResponse};
use crate::kafka::{KfkClientCache, KfkError, KfkRequest, KfkResponse};
use crate::util::{handle_future, Response};
use actson::options::JsonParserOptionsBuilder;
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::{anyhow, Result};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rskafka::chrono::{DateTime, NaiveDateTime, Utc};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use serde_json::Number;
use std::alloc::{Layout, System};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::ptr::NonNull;
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::Framed;
use tracing::{info, trace, warn};

impl From<JrpRequest> for (String, i32, usize, KfkRequest) {
    fn from(req: JrpRequest) -> Self {
        match req {
            JrpRequest::Send { jsonrpc, id, params } => {
                (params.topic, params.partition, id, KfkRequest::Send { records: vec!() } )
            }
            JrpRequest::Fetch { jsonrpc, id, params } => {
                (params.topic, params.partition, id, KfkRequest::Fetch {
                    offset: params.offset,
                    bytes: params.bytes,
                    max_wait_ms: params.max_wait_ms.unwrap_or(0)
                })
            },
        }
    }
}

impl From<KfkError> for JrpError {
    fn from(err: KfkError) -> Self {
        JrpError::new(err.to_string())
    }
}

impl From<Response<KfkResponse, KfkError>> for JrpResponse {
    fn from(value: Response<KfkResponse, KfkError>) -> Self {
        let (id, result) = value;
        match result {
            Ok(KfkResponse::Send { offsets } ) => {
                let json: serde_json::Value = offsets.into();
                JrpResponse::new(id, Ok(json))
            }
            Ok(KfkResponse::Fetch { records_and_offsets, high_watermark}) => {
                let x = serde_json::Value::Null;
                JrpResponse::new(id, Ok(x))
            }
            Err(error) => {
                JrpResponse::error(id, error.into())
            }
        }
    }
}

async fn run_input_loop(addr: SocketAddr, mut stream: SplitStream<Framed<TcpStream, JrpFramer>>, ctx: Arc<KfkClientCache>, snd: Sender<JrpResponse>) -> Result<()> {
    info!("input, addr: {} - START", addr);
    while let Some(result) = stream.next().await {
        let jrp_req = result?;
        let (topic, partition, id, kfk_req) = jrp_req.into();
        let snd = ctx.lookup_kafka_sender(topic, partition, 64).await?;
        //snd.send().unwrap()
        //snd.send(Msg::new(123, rsp.clone())).await?;
    }
    info!("input, addr: {} - END", addr);
    Ok(())
}

async fn run_output_loop(addr: SocketAddr, mut sink: SplitSink<Framed<TcpStream, JrpFramer>, JrpResponse>, mut rcv: Receiver<JrpResponse>) -> Result<()> {
    info!("output, addr: {} - START", addr);
    while let Some(rsp) = rcv.recv().await {
        sink.send(rsp).await?;
    }
    info!("output, addr: {} - END", addr);
    Ok(())
}

#[derive(Copy, Clone)]
enum Context {
    None,
    Jsonrpc,
    Id,
    Method,
    Params,
    Topic,
    Partition,
    Key,
    Data,
}

type JP = JsonParser<AsyncBufReaderJsonFeeder<OwnedReadHalf>>;

async fn test_1(jp: &mut JP, mut rcv: Receiver<JsonEvent>) -> Result<()> {
    let s: String = "test".into();
    let v: Vec<u8> = Vec::from(s);

    while let Some(event) = rcv.recv().await {
        let x = jp.current_str();
    }
    Ok(())
}

/*
async fn run_input_loop2(addr: SocketAddr, rh: OwnedReadHalf, ctx: Arc<KfkClientCache>, snd: Sender<JrpResponse>) -> Result<()> {
    let br = BufReader::new(rh);
    let abr = actson::tokio::AsyncBufReaderJsonFeeder::new(br);
    let opts = JsonParserOptionsBuilder::default().with_streaming(true).build();

    let jp = JsonParser::new_with_options(abr, opts);
    let rjp = RefCell::new(jp);

    let mut field = Context::None;
    let mut object = Context::None;



    let (sender, receiver) = mpsc::channel::<JsonEvent>(64);



    while let Some(event) = rjp.borrow_mut().next_event()? {
        match event {
            JsonEvent::NeedMoreInput => {
                trace!("need more input");
                rjp.borrow_mut().feeder.fill_buf().await?;
            }
            _ => {
                sender.send(event).await?;
            }
            JsonEvent::StartObject => {
                trace!("start object");
                object = field;
            }
            JsonEvent::EndObject => {
                trace!("end object");
                object = Context::None;
            }
            JsonEvent::StartArray => {
                trace!("start array");
            }
            JsonEvent::EndArray => {
                trace!("end array");
            }
            JsonEvent::FieldName => {
                let name = jp.current_str()?;
                trace!("field name: {}", name);
                match object {
                    Context::Params => {
                        if name == "topic" {
                            field = Context::Topic;

                        } else if name == "partition" {
                            field = Context::Partition
                        } else if name == "key" {
                            field = Context::Key
                        } else if name == "data" {
                            field = Context::Data
                        }
                    }
                    Context::Data => {
                        // TODO
                    }
                    _ => {
                        warn!("unexpected field {}", name);
                    }
                }
            }
            JsonEvent::ValueString => {
                let text = jp.current_str()?;
                trace!("value string: {}", text);
            }
            JsonEvent::ValueInt => {
                let n: i64 = jp.current_int()?;
                trace!("value int: {}", n);
            }
            JsonEvent::ValueFloat => {
                let f: f64 = jp.current_float()?;
                trace!("value float: {}", f);
            }
            JsonEvent::ValueTrue => {
                let b: bool = true;
                trace!("value bool: {}", b);
            }
            JsonEvent::ValueFalse => {
                let b = false;
                trace!("value bool: {}", b);
            }
            JsonEvent::ValueNull => {
                trace!("value null");
            }
        }
    }
    Ok(())
}
 */

pub async fn listen(bind: SocketAddr, brokers: Vec<HostPort>) -> Result<()> {

    info!("server, kafka: {:?}", brokers);
    let bs: Vec<String> = brokers.iter().map(|hp| { hp.to_string() }).collect();
    let client = ClientBuilder::new(bs).build().await?;
    let ctx = Arc::new(KfkClientCache::new(client, 1024));

    info!("server, bind: {:?}", bind);
    let listener = TcpListener::bind(bind).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("accepted: {:?}", addr);
        let (rh, wh) = stream.into_split();
        let (snd, rcv) = mpsc::channel::<JrpResponse>(1024);
        // tokio::spawn(
        //     handle_future(
        //         run_input_loop2(addr, rh, ctx.clone(), snd)
        //     )
        // );
        //
        // tokio::spawn(
        //     handle_future(
        //         run_output_loop(addr, sink, rcv)
        //     )
        // );
    }
}
