use crate::args::Offset;
use crate::async_clean_return;
use crate::codec::{JsonCodec, MeteredItem};
use crate::error::JrpkError;
use crate::jsonrpc::{JrpBytes, JrpData, JrpDataCodec, JrpDataCodecs, JrpOffset, JrpRecFetch, JrpRecSend, JrpReq, JrpRsp, JrpRspData};
use crate::metrics::{spawn_push_prometheus, JrpkMeters, LBL_CLIENT, LBL_FETCH, LBL_TCP, LBL_READ, LBL_WRITE, LBL_SEND};
use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use log::warn;
use prometheus_client::registry::Registry;
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use hyper::Uri;
use moka::future::Cache;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::block_in_place;
use tokio::{spawn, try_join};
use tokio_util::codec::{Framed, FramedRead};
use tracing::{debug, error, instrument, trace};
use ustr::Ustr;
use crate::kafka::KfkKey;

fn a2j_offset(ao: Offset) -> JrpOffset {
    match ao {
        Offset::Earliest => JrpOffset::Earliest,
        Offset::Latest => JrpOffset::Latest,
        Offset::Timestamp(ts) => JrpOffset::Timestamp(ts),
        Offset::Offset(pos) => JrpOffset::Offset(pos),
    }
}

type JrpkMeteredConsReq<'a> = MeteredItem<JrpReq<'a>>;

#[instrument(ret, err, skip(metrics, offset_rcv, tcp_sink))]
async fn consumer_req_writer<'a>(
    key: KfkKey,
    max_batch_size: i32,
    max_wait_ms: i32,
    metrics: JrpkMeters,
    times: Arc<Cache<usize, Instant>>,
    mut offset_rcv: Receiver<Offset>,
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpkMeteredConsReq<'a>>,
) -> Result<(), JrpkError> {
    let mut id = 0;
    let throughput = metrics.throughput_owned(LBL_CLIENT, LBL_FETCH, LBL_TCP, LBL_WRITE, Some(key.clone()));
    while let Some(offset) = offset_rcv.recv().await {
        // TODO: support all codecs
        let key = key.clone();
        let codecs = JrpDataCodecs::new(JrpDataCodec::Str, JrpDataCodec::Json);
        let bytes = 1..max_batch_size;
        let jrp_req_fetch = JrpReq::fetch(id, key.topic, key.partition, a2j_offset(offset), codecs, bytes, max_wait_ms);
        let metered_item = JrpkMeteredConsReq::new(jrp_req_fetch, throughput.clone());
        times.insert(id, Instant::now()).await;
        tcp_sink.send(metered_item).await?;
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
#[instrument(ret, err, level="trace", skip(writer, records))]
async fn write_records<'a>(
    id: usize,
    records: Vec<JrpRecFetch<'a>>,
    until: Offset,
    writer: &mut BufWriter<File>,
) -> Result<(), JrpkError> {
    block_in_place(|| {
        for record in records {
            if less_record_offset(&record, until) {
                match record.value {
                    Some(data) => {
                        trace!("record, id: {}, timestamp: {}, offset: {}", id, 0, record.offset);
                        // TODO: differentiate between binary and text data
                        let buf = data.as_bytes()?;
                        writer.write_all(buf.as_ref())?;
                        writer.write_all(b"\n")?;
                    }
                    None => {
                        warn!("record, id: {}, offset: {} - EMPTY", id, record.offset);
                    }
                }
            }
        }
        Ok(())
    })
}

#[instrument(ret, err, skip(meters, offset_snd, tcp_stream))]
async fn consumer_rsp_reader(
    key: KfkKey,
    path: Ustr,
    from: Offset,
    until: Offset,
    meters: JrpkMeters,
    times: Arc<Cache<usize, Instant>>,
    offset_snd: Sender<Offset>,
    mut tcp_stream: SplitStream<Framed<TcpStream, JsonCodec>>,
) -> Result<(), JrpkError> {
    let throughput = meters.throughput_owned(LBL_CLIENT, LBL_FETCH, LBL_TCP, LBL_READ, Some(key));
    let file = File::create(path)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    offset_snd.send(from).await?;
    while let Some(result) = tcp_stream.next().await {
        let frame = result?;
        let length = frame.len() as u64;
        let jrp_rsp = serde_json::from_slice::<JrpRsp>(frame.as_ref())?;
        let id = jrp_rsp.id;
        // TODO: write latency metric
        match jrp_rsp.take_result() {
            Ok(jrp_rsp_data) => {
                match jrp_rsp_data {
                    JrpRspData::Fetch { high_watermark, mut records } => {
                        throughput.inc_by(length);
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
                        write_records(id, records, until, &mut writer).await?;
                        if done {
                            break;
                        }
                    }
                    JrpRspData::Send { .. } => {
                        error!("error, id: {}, response: send", id);
                        return Err(JrpkError::Unexpected("send"));
                    }
                    JrpRspData::Offset(_) => {
                        error!("error, id: {}, response: offset", id);
                        return Err(JrpkError::Unexpected("offset"));
                    }
                }
            }
            Err(err) => {
                error!("error, id: {}, message: {}", id, err.message);
                return Err(JrpkError::Internal(err.message))
            }
        }
    }
    writer.flush()?;
    Ok(())
}

#[instrument(ret, err)]
pub async fn consume(
    address: Ustr,
    key: KfkKey,
    path: Ustr,
    from: Offset,
    until: Offset,
    max_batch_size: i32,
    max_wait_ms: i32,
    max_frame_size: usize,
    metrics_uri: Uri,
    metrics_period: Duration,
) -> Result<(), JrpkError> {

    let mut registry = Registry::default();
    let metrics = JrpkMeters::new(&mut registry);
    let ph = spawn_push_prometheus(
        metrics_uri,
        metrics_period,
        registry
    );

    let stream = TcpStream::connect(address.as_str()).await?;
    let codec = JsonCodec::new(max_frame_size);
    let framed = Framed::new(stream, codec);
    let (tcp_sink, tcp_stream) = framed.split();
    let (offset_snd, offset_rcv) = mpsc::channel::<Offset>(2);
    let times = Arc::new(Cache::builder().time_to_live(Duration::from_mins(1)).build());

    let wh = spawn(
        consumer_req_writer(
            key.clone(),
            max_batch_size,
            max_wait_ms,
            metrics.clone(),
            times.clone(),
            offset_rcv,
            tcp_sink
        )
    );

    let rh = spawn(
        consumer_rsp_reader(
            key,
            path,
            from,
            until,
            metrics.clone(),
            times.clone(),
            offset_snd,
            tcp_stream
        )
    );

    let _ = try_join!(wh, rh);
    let _ = ph.cancel().await?;
    Ok(())
}

type JrpkMeteredProdReq<'a> = MeteredItem<JrpBytes<JrpReq<'a>>>;

#[instrument(ret, skip(meters, tcp_sink))]
pub async fn producer_req_writer(
    key: KfkKey,
    path: Ustr,
    max_frame_size: usize,
    max_batch_rec_count: usize,
    max_batch_size: usize,
    max_rec_size: usize,
    meters: JrpkMeters,
    times: Arc<Cache<usize, Instant>>,
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpkMeteredProdReq<'_>>,
) -> Result<(), JrpkError> {
    let throughput = meters.throughput_owned(LBL_CLIENT, LBL_SEND, LBL_TCP, LBL_WRITE, Some(key.clone()));
    let file = async_clean_return!(tokio::fs::File::open(path).await, tcp_sink.close().await);
    let reader = tokio::io::BufReader::with_capacity(1024 * 1024, file);
    let codec = JsonCodec::new(max_frame_size);
    let mut file_stream = FramedRead::with_capacity(reader, codec, max_frame_size);
    let mut id: usize = 0;
    let mut frames: Vec<Bytes> = Vec::with_capacity(max_batch_rec_count);
    let mut records: Vec<JrpRecSend> = Vec::with_capacity(max_batch_rec_count);
    let mut size: usize = 0;
    while let Some(result) = file_stream.next().await {
        let key = key.clone();
        let frame = result?;
        let json: &RawValue = unsafe { async_clean_return!( JrpBytes::from_bytes(&frame), tcp_sink.close().await) };
        let jrp_data: JrpData = JrpData::Json(Cow::Borrowed(json));
        let jrp_rec = JrpRecSend::new(None, Some(jrp_data));
        size += frame.len();
        frames.push(frame);
        records.push(jrp_rec);
        if size > max_batch_size - max_rec_size || records.len() >= max_batch_rec_count {
            let jrp_req = JrpReq::send(id, key.topic, key.partition, records);
            records = Vec::with_capacity(max_batch_rec_count);
            let jrp_bytes = JrpBytes::new(jrp_req, frames);
            frames = Vec::with_capacity(max_batch_rec_count);
            times.insert(id, Instant::now()).await;
            let metered_item = JrpkMeteredProdReq::new(jrp_bytes, throughput.clone());
            async_clean_return!(tcp_sink.send(metered_item).await, tcp_sink.close().await);
            id += 1;
            size = 0;
        }
    }

    if !records.is_empty() && !frames.is_empty() {
        let jrp_req = JrpReq::send(id, key.topic, key.partition, records);
        let jrp_bytes = JrpBytes::new(jrp_req, frames);
        times.insert(id, Instant::now()).await;
        let metered_item = JrpkMeteredProdReq::new(jrp_bytes, throughput);
        async_clean_return!(tcp_sink.send(metered_item).await, tcp_sink.close().await);
    }

    async_clean_return!(tcp_sink.flush().await, tcp_sink.close().await);
    tcp_sink.close().await?;
    Ok(())
}

#[instrument(ret, skip(meters, tcp_stream))]
pub async fn producer_rsp_reader(
    key: KfkKey,
    meters: JrpkMeters,
    times: Arc<Cache<usize, Instant>>,
    mut tcp_stream: SplitStream<Framed<TcpStream, JsonCodec>>
) -> Result<(), JrpkError> {
    let throughput = meters.throughput_owned(LBL_CLIENT, LBL_SEND, LBL_TCP, LBL_READ, Some(key.clone()));
    while let Some(result) = tcp_stream.next().await {
        let frame = result?;
        let length = frame.len() as u64;
        throughput.inc_by(length);
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

#[instrument(ret, err)]
pub async fn produce(
    address: Ustr,
    key: KfkKey,
    path: Ustr,
    max_frame_size: usize,
    max_batch_rec_count: usize,
    max_batch_size: usize,
    max_rec_byte_size: usize,
    metrics_uri: Uri,
    metrics_period: Duration,
) -> Result<(), JrpkError> {

    let mut registry = Registry::default();
    let metrics = JrpkMeters::new(&mut registry);
    let times = Arc::new(Cache::builder().time_to_live(Duration::from_mins(1)).build());
    let ph = spawn_push_prometheus(
        metrics_uri,
        metrics_period,
        registry
    );

    let stream = TcpStream::connect(address.as_str()).await?;
    let codec = JsonCodec::new(max_frame_size);
    let framed = Framed::with_capacity(stream, codec, max_frame_size);
    let (tcp_sink, tcp_stream) = framed.split();
    let wh = tokio::spawn(
        producer_req_writer(
            key.clone(),
            path,
            max_frame_size,
            max_batch_rec_count,
            max_batch_size,
            max_rec_byte_size,
            metrics.clone(),
            times.clone(),
            tcp_sink
        )
    );
    let rh = tokio::spawn(
        producer_rsp_reader(
            key,
            metrics.clone(),
            times.clone(),
            tcp_stream
        )
    );

    let _ = try_join!(wh, rh);
    let _ = ph.cancel().await?;
    Ok(())
}
