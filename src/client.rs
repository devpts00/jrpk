use std::ops::Range;
use std::path::PathBuf;
use base64::DecodeError;
use bytes::Bytes;
use bytesize::ByteSize;
use thiserror::Error;
use futures::stream::{SplitSink, SplitStream};
use futures::{Sink, SinkExt, StreamExt, TryFutureExt};
use log::warn;
use tokio::io::AsyncWriteExt;
use tokio::fs::File;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinError;
use tokio_util::codec::Framed;
use tracing::{error, info, trace};
use crate::args::Offset;
use crate::codec::{BytesFrameDecoderError, JsonCodec, JsonEncoderError};
use crate::jsonrpc::{JrpData, JrpDataCodec, JrpDataCodecs, JrpErrorMsg, JrpMethod, JrpOffset, JrpParams, JrpRecFetch, JrpRecSend, JrpReq, JrpResult, JrpRsp, JrpRspData};
use crate::util::handle_future_result;

fn a2j_offset(ao: Offset) -> JrpOffset {
    match ao {
        Offset::Earliest => JrpOffset::Earliest,
        Offset::Latest => JrpOffset::Latest,
        Offset::Timestamp(ts) => JrpOffset::Timestamp(ts),
        Offset::Offset(pos) => JrpOffset::Offset(pos),
    }
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("decoder: {0}")]
    Frame(#[from] BytesFrameDecoderError),
    #[error("io: {0}")]
    IO(#[from] std::io::Error),
    #[error("encoder: {0}")]
    Encoder(#[from] JsonEncoderError),
    #[error("send: {0}")]
    Send(#[from] SendError<Offset>),
    #[error("json: {0}")]
    Json(#[from] serde_json::error::Error),
    #[error("jsonrpc: {0}")]
    Jrp(JrpErrorMsg),
    #[error("unexpected: {0}")]
    UnexpectedResponse(String),
    #[error("base64: {0}")]
    Base64(#[from] DecodeError),
    #[error("join: {0}")]
    Join(#[from] JoinError),
}

async fn consumer_req_writer<'a>(
    topic: String,
    partition: i32,
    batch_size: ByteSize,
    max_wait_ms: i32,
    mut offset_rcv: Receiver<Offset>,
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpReq<'a>>,
) -> Result<(), ClientError> {
    let mut id = 0;
    while let Some(offset) = offset_rcv.recv().await {
        // TODO: support all codecs
        let codecs = JrpDataCodecs::new(JrpDataCodec::Str, JrpDataCodec::Json);
        let bytes = 1..batch_size.as_u64() as i32;
        let jrp_req_fetch = JrpReq::fetch(id, topic.clone(), partition, a2j_offset(offset), codecs, bytes, max_wait_ms);
        tcp_sink.send(jrp_req_fetch).await?;
        id = id + 1;
    }
    tcp_sink.flush().await?;
    Ok(())
}

async fn write_records_to_file<'a>(file: &mut File, id: usize, records: Vec<JrpRecFetch<'a>>) -> Result<(), ClientError> {
    for record in records {
        match record.value {
            Some(data) => {
                trace!("record, id: {}, offset: {}", id, record.offset);
                // TODO: differentiate between binary and text data
                let buf = data.as_bytes()?;
                file.write(&buf).await?;
                file.write(b"\n").await?;
            }
            None => {
                warn!("record, id: {}, offset: {} - EMPTY", id, record.offset);
            }
        }
    }
    Ok(())
}

async fn consumer_rsp_reader(
    path: PathBuf,
    from: Offset,
    until: Offset,
    offset_snd: Sender<Offset>,
    mut tcp_stream: SplitStream<Framed<TcpStream, JsonCodec>>,
) -> Result<(), ClientError> {
    let mut file = File::create(path).await?;
    offset_snd.send(from).await?;
    while let Some(result) = tcp_stream.next().await {
        let frame = result?;
        let jrp_rsp = serde_json::from_slice::<JrpRsp>(frame.as_ref())?;
        let id = jrp_rsp.id;
        match jrp_rsp.result {
            JrpResult::Result(data) => {
                match data {
                    JrpRspData::Fetch { next_offset, high_watermark, records } => {
                        // if more data is available request it right away
                        if let Some(pos) = next_offset {
                            if pos < high_watermark {
                                offset_snd.send(Offset::Offset(pos)).await?;
                            }
                        }
                        write_records_to_file(&mut file, id, records).await?;
                    }
                    JrpRspData::Send { .. } => {
                        error!("error, id: {}, response: send", id);
                        return Err(ClientError::UnexpectedResponse("send".to_owned()));
                    }
                    JrpRspData::Offset(_) => {
                        error!("error, id: {}, response: offset", id);
                        return Err(ClientError::UnexpectedResponse("offset".to_owned()));
                    }
                }
            }
            JrpResult::Error(err) => {
                error!("error, id: {}, message: {}", id, err.message);
                return Err(ClientError::Jrp(err))
            }
        }
    }
    file.flush().await?;
    Ok(())
}

pub async fn consume(
    path: PathBuf,
    address: String,
    topic: String,
    partition: i32,
    from: Offset,
    until: Offset,
    batch_size: ByteSize,
    max_wait_ms: i32,
    max_frame_size: ByteSize,

) -> Result<(), ClientError> {

    info!("consume, address: {}, topic: {}, partition: {}, from: {:?}, until: {:?}, file: {:?}, max_frame_size: {}",
        address, topic, partition, from, until, path, max_frame_size
    );
    let stream = TcpStream::connect(address).await?;
    let addr = stream.peer_addr()?;
    info!("connected: {}", addr);
    let codec = JsonCodec::new(max_frame_size.as_u64() as usize);
    let framed = Framed::new(stream, codec);
    let (tcp_sink, tcp_stream) = framed.split();
    let (offset_snd, offset_rcv) = mpsc::channel::<Offset>(32);

    let wh = tokio::spawn(
        handle_future_result(
            "consumer-writer",
            addr,
            consumer_req_writer(topic, partition, batch_size, max_wait_ms, offset_rcv, tcp_sink)
        )
    );

    let rh = tokio::spawn(
        handle_future_result(
            "consumer-reader",
            addr,
            consumer_rsp_reader(path, from, until, offset_snd, tcp_stream)
        )
    );

    wh.await?;
    rh.await?;

    Ok(())
}

pub async fn producer_writer() -> Result<(), ClientError> {
    Ok(())
}

pub async fn producer_reader() -> Result<(), ClientError> {
    Ok(())
}

pub async fn produce(address: String, topic: String, partition: i32, file: PathBuf, max_frame_size: ByteSize) -> Result<(), ClientError> {
    Ok(())
}
