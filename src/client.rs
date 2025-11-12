use std::borrow::Cow;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Duration;
use base64::DecodeError;
use bytes::Bytes;
use bytesize::ByteSize;
use thiserror::Error;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt, TryStreamExt};
use log::{info, warn};
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use serde_json::value::RawValue;
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tokio::task::{block_in_place, JoinError};
use tokio_util::codec::{Framed, FramedRead};
use tracing::{debug, error, instrument, trace, Instrument, Level, Span};
use ustr::Ustr;
use crate::args::Offset;
use crate::async_clean_return;
use crate::codec::{BytesFrameDecoderError, JsonCodec, JsonEncoderError};
use crate::jsonrpc::{JrpBytes, JrpData, JrpDataCodec, JrpDataCodecs, JrpErrorMsg, JrpOffset, JrpParams, JrpRecFetch, JrpRecSend, JrpReq, JrpRsp, JrpRspData};
use crate::metrics::{prometheus_pushgateway, Metrics};
use crate::util::logh;

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

#[instrument(ret, skip(metrics, offset_rcv, tcp_sink))]
async fn consumer_req_writer<'a>(
    topic: Ustr,
    partition: i32,
    max_batch_byte_size: ByteSize,
    max_wait_ms: i32,
    metrics: Metrics,
    mut offset_rcv: Receiver<Offset>,
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpReq<'a>>,
) -> Result<(), ClientError> {
    let tcp_write_count = metrics.count("client", "consume", "tcp", "write");
    let mut id = 0;
    while let Some(offset) = offset_rcv.recv().await {
        // TODO: support all codecs
        let codecs = JrpDataCodecs::new(JrpDataCodec::Str, JrpDataCodec::Json);
        let bytes = 1..max_batch_byte_size.as_u64() as i32;
        let jrp_req_fetch = JrpReq::fetch(id, topic, partition, a2j_offset(offset), codecs, bytes, max_wait_ms);
        tcp_sink.send(jrp_req_fetch).await?;
        tcp_write_count.inc();
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
        Offset::Timestamp(timestamp) => record.timestamp < timestamp,
        Offset::Offset(pos) => record.offset < pos,
    }
}

#[allow(clippy::needless_lifetimes)]
#[instrument(ret, level="trace", skip(writer, records))]
async fn write_records<'a>(
    id: usize,
    records: Vec<JrpRecFetch<'a>>,
    until: Offset,
    file_write_count: &Counter,
    file_write_bytes: &Counter,
    writer: &mut BufWriter<File>,
) -> Result<(), ClientError> {
    for record in records {
        if less_record_offset(&record, until) {
            match record.value {
                Some(data) => {
                    trace!("record, id: {}, timestamp: {}, offset: {}", id, 0, record.offset);
                    // TODO: differentiate between binary and text data
                    let buf = data.as_bytes()?;
                    block_in_place(|| {
                         writer.write_all(buf.as_ref())
                             .and_then(|_|
                                 writer.write_all(b"\n")
                             )
                    })?;
                    file_write_count.inc();
                    file_write_bytes.inc_by(buf.len() as u64);
                }
                None => {
                    warn!("record, id: {}, offset: {} - EMPTY", id, record.offset);
                }
            }
        }
    }
    Ok(())
}


#[instrument(ret, skip(metrics, offset_snd, tcp_stream))]
async fn consumer_rsp_reader(
    path: Ustr,
    from: Offset,
    until: Offset,
    metrics: Metrics,
    offset_snd: Sender<Offset>,
    mut tcp_stream: SplitStream<Framed<TcpStream, JsonCodec>>,
) -> Result<(), ClientError> {
    let tcp_read_count = metrics.count("client", "consume", "tcp", "read");
    let tcp_read_bytes = metrics.bytes("client", "consume", "tcp", "read");
    let file_write_count = metrics.count("client", "consume", "file", "write");
    let file_write_bytes = metrics.bytes("client", "consume", "file", "write");
    let file = File::create(path)?;
    let mut writer = BufWriter::with_capacity(32 * 1024 * 1024, file);
    offset_snd.send(from).await?;
    while let Some(result) = tcp_stream.next().await {
        let frame = result?;
        tcp_read_count.inc();
        tcp_read_bytes.inc_by(frame.len() as u64);
        let jrp_rsp = serde_json::from_slice::<JrpRsp>(frame.as_ref())?;
        let id = jrp_rsp.id;
        match jrp_rsp.take_result() {
            Ok(jrp_rsp_data) => {
                match jrp_rsp_data {
                    JrpRspData::Fetch { high_watermark, mut records } => {
                        records.sort_by_key(|r| r.offset);
                        let mut done = true;
                        // if more data is available
                        if let Some(last) = records.last() {
                            // if the last item is not out of bounds
                            if less_record_offset(last, until) && last.offset < high_watermark {
                                done = false;
                                offset_snd.send(Offset::Offset(last.offset + 1)).await?;
                            }
                        }
                        write_records(id, records, until, &file_write_count, &file_write_bytes,  &mut writer).await?;
                        if done {
                            break;
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
    writer.flush()?;
    Ok(())
}

#[instrument(ret)]
pub async fn consume(
    path: Ustr,
    address: Ustr,
    topic: Ustr,
    partition: i32,
    from: Offset,
    until: Offset,
    batch_size: ByteSize,
    max_wait_ms: i32,
    max_frame_size: ByteSize,
) -> Result<(), ClientError> {

    let mut registry = Registry::default();
    let metrics = Metrics::new(&mut registry);

    let stream = TcpStream::connect(address.as_str()).await?;
    let codec = JsonCodec::new(max_frame_size.as_u64() as usize);
    let framed = Framed::new(stream, codec);
    let (tcp_sink, tcp_stream) = framed.split();
    let (offset_snd, offset_rcv) = mpsc::channel::<Offset>(2);

    let mut registry = Registry::default();
    let metrics = Metrics::new(&mut registry);
    let c = metrics.count("client", "consume", "tcp", "read");
    c.inc();

    let wh = spawn(
        consumer_req_writer(
            topic,
            partition,
            batch_size,
            max_wait_ms,
            metrics.clone(),
            offset_rcv,
            tcp_sink
        )
    );

    let rh = spawn(
        consumer_rsp_reader(
            path,
            from,
            until,
            metrics,
            offset_snd,
            tcp_stream
        )
    );

    let (done_snd, done_rcv) = tokio::sync::oneshot::channel();
    let ph = spawn(async move{
        let address = "pmg:9091".to_string();
        let period = Duration::from_secs(1);
        prometheus_pushgateway(address, period, registry, done_rcv)
    }.await);

    logh("consumer_req_writer", wh).await;
    logh("consumer_rsp_reader", rh).await;

    let _ = done_snd.send(());
    logh("consumer_prometheus_gateway", ph).await;

    Ok(())
}

#[instrument(ret, skip(tcp_sink))]
pub async fn producer_req_writer(
    path: Ustr,
    topic: Ustr,
    partition: i32,
    max_frame_size: usize,
    max_batch_rec_count: usize,
    max_batch_byte_size: usize,
    max_rec_byte_size: usize,
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpBytes<JrpReq<'_>>>,
) -> Result<(), ClientError> {
    let file = async_clean_return!(tokio::fs::File::open(path).await, tcp_sink.close().await);
    let reader = tokio::io::BufReader::with_capacity(32 * 1024 * 1024, file);
    let codec = JsonCodec::new(max_frame_size);
    let mut file_stream = FramedRead::with_capacity(reader, codec, max_frame_size);
    let mut id: usize = 0;
    let mut frames: Vec<Bytes> = Vec::with_capacity(max_batch_rec_count);
    let mut records: Vec<JrpRecSend> = Vec::with_capacity(max_batch_rec_count);
    let mut size: usize = 0;
    while let Some(result) = file_stream.next().await {
        let frame = result?;
        let json: &RawValue = unsafe { async_clean_return!( JrpBytes::from_bytes(&frame), tcp_sink.close().await) };
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
            async_clean_return!(tcp_sink.send(jrp_bytes).await, tcp_sink.close().await);
            id += 1;
            size = 0;
        }
    }

    if !records.is_empty() && !frames.is_empty() {
        let jrp_req = JrpReq::send(id, topic.clone(), partition, records);
        let jrp_bytes = JrpBytes::new(jrp_req, frames);
        async_clean_return!(tcp_sink.send(jrp_bytes).await, tcp_sink.close().await);
    }

    async_clean_return!(tcp_sink.flush().await, tcp_sink.close().await);
    tcp_sink.close().await?;
    Ok(())
}

#[instrument(ret, skip(tcp_stream))]
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

#[instrument(ret)]
pub async fn produce(
    path: Ustr,
    address: Ustr,
    topic: Ustr,
    partition: i32,
    max_frame_byte_size: usize,
    max_batch_rec_count: usize,
    max_batch_byte_size: usize,
    max_rec_byte_size: usize,
) -> Result<(), ClientError> {

    let stream = TcpStream::connect(address.as_str()).await?;
    let codec = JsonCodec::new(max_frame_byte_size);
    let framed = Framed::with_capacity(stream, codec, max_frame_byte_size);
    let (tcp_sink, tcp_stream) = framed.split();

    let wh = tokio::spawn(
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
    );
    let rh = tokio::spawn(
        producer_rsp_reader(tcp_stream)
    );

    logh("producer_req_writer", wh).await;
    logh("producer_rsp_reader", rh).await;

    Ok(())
}
