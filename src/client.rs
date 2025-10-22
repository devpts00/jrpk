use std::borrow::Cow;
use std::future::Future;
use std::ops::Range;
use std::path::PathBuf;
use std::str::from_utf8;
use base64::DecodeError;
use bytes::Bytes;
use bytesize::ByteSize;
use thiserror::Error;
use futures::stream::{SplitSink, SplitStream};
use futures::{Sink, SinkExt, StreamExt, TryFutureExt};
use log::warn;
use serde_json::value::RawValue;
use tokio::io::AsyncWriteExt;
use tokio::fs::File;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinError;
use tokio_util::codec::{Framed, FramedRead};
use tracing::{debug, error, info, trace};
use ustr::Ustr;
use crate::args::Offset;
use crate::codec::{BytesFrameDecoderError, JsonCodec, JsonEncoderError};
use crate::jsonrpc::{JrpBytes, JrpData, JrpDataCodec, JrpDataCodecs, JrpErrorMsg, JrpMethod, JrpOffset, JrpParams, JrpRecFetch, JrpRecSend, JrpReq, JrpRsp, JrpRspData};
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
    topic: Ustr,
    partition: i32,
    max_batch_byte_size: ByteSize,
    max_wait_ms: i32,
    mut offset_rcv: Receiver<Offset>,
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpReq<'a>>,
) -> Result<(), ClientError> {
    let mut id = 0;
    while let Some(offset) = offset_rcv.recv().await {
        // TODO: support all codecs
        let codecs = JrpDataCodecs::new(JrpDataCodec::Str, JrpDataCodec::Json);
        let bytes = 1..max_batch_byte_size.as_u64() as i32;
        let jrp_req_fetch = JrpReq::fetch(id, topic, partition, a2j_offset(offset), codecs, bytes, max_wait_ms);
        tcp_sink.send(jrp_req_fetch).await?;
        id = id + 1;
    }
    tcp_sink.flush().await?;
    Ok(())
}

#[inline]
fn less_record_offset(record: &JrpRecFetch, offset: Offset) -> bool {
    match offset {
        Offset::Earliest => false,
        Offset::Latest => true,
        Offset::Timestamp(timestamp) => true, //record.timestamp < timestamp,
        Offset::Offset(pos) => record.offset < pos,
    }
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
        match jrp_rsp.take_result() {
            Ok(jrp_rsp_data) => {
                match jrp_rsp_data {
                    JrpRspData::Fetch { high_watermark, records } => {
                        // if more data is available
                        if let Some(last) = records.last() {
                            // if the last item is not out of bounds
                            if less_record_offset(last, until) && last.offset < high_watermark {
                                offset_snd.send(Offset::Offset(last.offset + 1)).await?;
                            }
                        }
                        for record in records {
                            if less_record_offset(&record, until) {
                                match record.value {
                                    Some(data) => {
                                        trace!("record, id: {}, timestamp: {}, offset: {}", id, 0, record.offset);
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
                        }
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
            Err(err) => {
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
    address: Ustr,
    topic: Ustr,
    partition: i32,
    from: Offset,
    until: Offset,
    batch_size: ByteSize,
    max_wait_ms: i32,
    max_frame_size: ByteSize,

) -> Result<(), ClientError> {
    info!("consume, path: {:?}, address: {}, topic: {}, partition: {}, from: {}, until: {}, max_frame_size: {}",
        path, address, topic, partition, from, until, max_frame_size
    );
    let stream = TcpStream::connect(address.as_str()).await?;
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

pub async fn producer_req_writer(
    path: PathBuf,
    topic: Ustr,
    partition: i32,
    max_frame_size: usize,
    max_batch_rec_count: usize,
    max_batch_byte_size: usize,
    max_rec_byte_size: usize,
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpBytes<JrpReq<'_>>>,
) -> Result<(), ClientError> {

    let file = File::open(path).await?;
    let codec = JsonCodec::new(max_frame_size);
    let mut file_stream = FramedRead::with_capacity(file, codec, max_frame_size);
    let mut id: usize = 0;

    let mut frames: Vec<Bytes> = Vec::with_capacity(max_batch_rec_count);
    let mut records: Vec<JrpRecSend> = Vec::with_capacity(max_batch_rec_count);
    let mut size: usize = 0;

    while let Some(result) = file_stream.next().await {
        let frame = result?;
        let json: &RawValue = unsafe { JrpBytes::from_bytes(&frame)? };
        let jrp_data: JrpData = JrpData::Json(Cow::Borrowed(json));
        let jrp_rec = JrpRecSend::new(None, Some(jrp_data));
        size += frame.len();
        frames.push(frame);
        records.push(jrp_rec);
        if size > max_batch_byte_size - max_rec_byte_size || records.len() >= max_batch_rec_count {
            let jrp_req = JrpReq::send(id, topic, partition, records);
            records = Vec::with_capacity(max_batch_rec_count);
            let jrp_bytes = JrpBytes::new(jrp_req, frames);
            frames = Vec::with_capacity(max_batch_rec_count);
            tcp_sink.send(jrp_bytes).await?;
            id += 1;
            size = 0;
        }
    }

    if !records.is_empty() && !frames.is_empty() {
        let jrp_req = JrpReq::send(id, topic.clone(), partition, records);
        let jrp_bytes = JrpBytes::new(jrp_req, frames);
        tcp_sink.send(jrp_bytes).await?;
    }

    tcp_sink.flush().await?;
    tcp_sink.close().await?;

    Ok(())
}

pub async fn producer_rsp_reader(mut tcp_stream: SplitStream<Framed<TcpStream, JsonCodec>>) -> Result<(), ClientError> {
    while let Some(result) = tcp_stream.next().await {
        let frame = result?;
        let jrp_rsp = serde_json::from_slice::<JrpRsp>(frame.as_ref())?;
        let id = jrp_rsp.id;
        match jrp_rsp.take_result() {
            Ok(data) => {
                debug!("success, id: {}, {:?}", id, data);
            }
            Err(err) => {
                error!("error, id: {}, message: {}", id, err);
            }
        }
    }
    Ok(())
}

pub async fn produce(
    path: PathBuf,
    address: Ustr,
    topic: Ustr,
    partition: i32,
    max_frame_byte_size: usize,
    max_batch_rec_count: usize,
    max_batch_byte_size: usize,
    max_rec_byte_size: usize,
) -> Result<(), ClientError> {

    info!("produce, path: {:?}, address: {}, topic: {}, partition: {}, max_frame_byte_size: {}, max_batch_rec_count: {}, max_batch_byte_size: {}, max_rec_byte_size: {}",
        path, address, topic, partition, max_frame_byte_size, max_batch_rec_count, max_batch_byte_size, max_rec_byte_size);

    let stream = TcpStream::connect(address.as_str()).await?;
    let addr = stream.peer_addr()?;
    let codec = JsonCodec::new(max_frame_byte_size);
    let framed = Framed::with_capacity(stream, codec, max_frame_byte_size);
    let (tcp_sink, tcp_stream) = framed.split();

    let wh = tokio::spawn(
        handle_future_result(
            "producer-requests",
            addr,
            producer_req_writer(
                path,
                topic,
                partition,
                max_frame_byte_size,
                max_batch_rec_count,
                max_batch_byte_size,
                max_rec_byte_size,
                tcp_sink
            )
        )
    );

    let rh = tokio::spawn(
        handle_future_result(
            "producer-responses",
            addr,
            producer_rsp_reader(tcp_stream)
        )
    );

    wh.await?;
    rh.await?;

    Ok(())
}
