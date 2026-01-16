use crate::async_clean_return;
use crate::codec::LinesCodec;
use crate::error::JrpkError;
use crate::model::{JrpCodec, JrpData, JrpRecSend, JrpReq, JrpRsp};
use crate::metrics::{spawn_push_prometheus, JrpkMetrics, JrpkLabels, LblMethod, LblTier, LblTraffic, MeteredItem};
use crate::util::{url_append_tap, Tap};
use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::{Duration, Instant};
use faststr::FastStr;
use moka::future::Cache;
use reqwest::Url;
use serde::{Serialize, Serializer};
use tokio::net::TcpStream;
use tokio::try_join;
use tokio_util::codec::{Encoder, Framed, FramedRead};
use tracing::{debug, error, info, instrument};
use crate::args::Load;

type JrpRecParser = fn(&[u8]) -> Result<JrpRecSend, JrpkError>;

fn b2r_json(buf: &[u8]) -> Result<JrpRecSend, JrpkError> {
    let json: &RawValue = serde_json::from_slice(buf)?;
    let data = JrpData::Json(Cow::Borrowed(json));
    let rec = JrpRecSend::new(None, Some(data), Vec::new());
    Ok(rec)
}

fn b2r_text(buf: &[u8]) -> Result<JrpRecSend, JrpkError> {
    let text = from_utf8(buf)?;
    let data = JrpData::Str(Cow::Borrowed(text));
    let rec = JrpRecSend::new(None, Some(data), Vec::new());
    Ok(rec)
}

fn b2r_base64(buf: &[u8]) -> Result<JrpRecSend, JrpkError> {
    let base64 = from_utf8(buf)?;
    let data = JrpData::Base64(Cow::Borrowed(base64));
    let rec = JrpRecSend::new(None, Some(data), Vec::new());
    Ok(rec)
}

fn b2r_rec(buf: &[u8]) -> Result<JrpRecSend, JrpkError> {
    let rec: JrpRecSend = serde_json::from_slice(buf)?;
    Ok(rec)
}

#[inline]
fn b2r_rec_vec(bytes: &Vec<Bytes>, b2r: JrpRecParser) -> Result<Vec<JrpRecSend>, JrpkError> {
    bytes.iter().map(|bytes| b2r(bytes.as_ref())).collect()
}

struct JrpReqBuilder {
    id: usize,
    topic: FastStr,
    partition: i32,
    bytes: Vec<Bytes>,
    b2r: JrpRecParser,
}

impl JrpReqBuilder {
    fn new(id: usize, topic: FastStr, partition: i32, bytes: Vec<Bytes>, b2r: JrpRecParser) -> Self {
        JrpReqBuilder { id, topic, partition, bytes, b2r }
    }
    fn build(&self) -> Result<JrpReq, JrpkError> {
        let recs = b2r_rec_vec(&self.bytes, self.b2r)?;
        let req = JrpReq::send(self.id, self.topic.clone(), self.partition, recs);
        Ok(req)
    }
}

type JrpkMeteredProdReq = MeteredItem<JrpReqBuilder>;

impl Encoder<JrpkMeteredProdReq> for LinesCodec {
    type Error = JrpkError;
    fn encode(&mut self, metered_item: JrpkMeteredProdReq, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let MeteredItem { item, metrics, labels } = metered_item;
        let length = dst.len();
        dst.reserve(self.max_length);
        let jrp_req = item.build()?;
        serde_json::to_writer(dst.writer(), &jrp_req)?;
        dst.put_u8(b'\n');
        metrics.size_by_value(&labels, dst.len() - length);
        Ok(())
    }
}

#[instrument(ret, skip(metrics, times, tcp_sink))]
pub async fn producer_req_writer(
    tap: Tap,
    path: FastStr,
    load: Load,
    max_frame_size: usize,
    max_batch_rec_count: usize,
    max_batch_size: usize,
    max_rec_size: usize,
    metrics: Arc<JrpkMetrics>,
    times: Arc<Cache<usize, Instant>>,
    mut tcp_sink: SplitSink<Framed<TcpStream, LinesCodec>, JrpkMeteredProdReq>,
) -> Result<(), JrpkError> {

    let b2r = match load {
        Load::Value { codec: JrpCodec::Json } => b2r_json,
        Load::Value { codec: JrpCodec::Str } => b2r_text,
        Load::Value { codec: JrpCodec::Base64 } => b2r_base64,
        Load::Record => b2r_rec,
    };

    let labels = JrpkLabels::new(LblTier::Client)
        .method(LblMethod::Send)
        .traffic(LblTraffic::Out)
        .tap(tap.clone())
        .build();

    let file = async_clean_return!(tokio::fs::File::open(path.as_str()).await, tcp_sink.close().await);
    let reader = tokio::io::BufReader::with_capacity(max_frame_size, file);
    let codec = LinesCodec::new_with_max_length(max_frame_size);
    let mut file_stream = FramedRead::with_capacity(reader, codec, max_frame_size);
    let mut id: usize = 0;

    let mut frames: Vec<Bytes> = Vec::with_capacity(max_batch_rec_count);
    let mut batch_length: usize = 0;
    while let Some(result) = file_stream.next().await {
        let tap = tap.clone();
        let frame = result?;
        batch_length += frame.len();
        if batch_length > max_batch_size - max_rec_size || frames.len() >= max_batch_rec_count {
            info!("produce, batch-size: {}, max-rec-size: {}", batch_length, max_rec_size);
            let jrp_req_builder = JrpReqBuilder::new(id, tap.topic, tap.partition, frames, b2r);
            frames = Vec::with_capacity(max_batch_rec_count);
            times.insert(id, Instant::now()).await;
            let metered_item = JrpkMeteredProdReq::new(jrp_req_builder, metrics.clone(), labels.clone());
            async_clean_return!(tcp_sink.send(metered_item).await, tcp_sink.close().await);
            id += 1;
            batch_length = 0;
        }
    }

    if !frames.is_empty() {
        let jrp_req_builder = JrpReqBuilder::new(id, tap.topic, tap.partition, frames, b2r);
        times.insert(id, Instant::now()).await;
        let metered_item = JrpkMeteredProdReq::new(jrp_req_builder, metrics, labels);
        async_clean_return!(tcp_sink.send(metered_item).await, tcp_sink.close().await);
    }

    async_clean_return!(tcp_sink.flush().await, tcp_sink.close().await);
    tcp_sink.close().await?;
    Ok(())
}

#[instrument(ret, skip(metrics, times, tcp_stream))]
pub async fn producer_rsp_reader(
    tap: Tap,
    metrics: Arc<JrpkMetrics>,
    times: Arc<Cache<usize, Instant>>,
    mut tcp_stream: SplitStream<Framed<TcpStream, LinesCodec>>,
) -> Result<(), JrpkError> {
    let labels = JrpkLabels::new(LblTier::Client)
        .method(LblMethod::Send)
        .traffic(LblTraffic::In)
        .tap(tap)
        .build();
    while let Some(result) = tcp_stream.next().await {
        let frame = result?;
        metrics.size(&labels, &frame);
        let jrp_rsp = serde_json::from_slice::<JrpRsp>(frame.as_ref())?;
        let id = jrp_rsp.id;
        if let Some(ts) = times.remove(&id).await {
            metrics.time(&labels, ts);
        }
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

#[instrument(ret, err, skip(metrics, metrics_url))]
pub async fn produce(
    address: FastStr,
    tap: Tap,
    path: FastStr,
    load: Load,
    max_frame_size: usize,
    max_batch_rec_count: usize,
    max_batch_size: usize,
    max_rec_size: usize,
    metrics: Arc<JrpkMetrics>,
    mut metrics_url: Url,
    metrics_period: Duration,
) -> Result<(), JrpkError> {

    url_append_tap(&mut metrics_url, &tap)?;
    let times = Arc::new(Cache::builder().time_to_live(Duration::from_mins(1)).build());
    let ph = spawn_push_prometheus(
        metrics_url,
        metrics_period,
        metrics.clone()
    );

    let stream = TcpStream::connect(address.as_str()).await?;
    let codec = LinesCodec::new_with_max_length(max_frame_size);
    let framed = Framed::with_capacity(stream, codec, max_frame_size);
    let (tcp_sink, tcp_stream) = framed.split();
    let wh = tokio::spawn(
        producer_req_writer(
            tap.clone(),
            path,
            load,
            max_frame_size,
            max_batch_rec_count,
            max_batch_size,
            max_rec_size,
            metrics.clone(),
            times.clone(),
            tcp_sink
        )
    );
    let rh = tokio::spawn(
        producer_rsp_reader(
            tap,
            metrics.clone(),
            times.clone(),
            tcp_stream
        )
    );

    let _ = try_join!(wh, rh);
    let _ = ph.cancel().await?;
    Ok(())
}
