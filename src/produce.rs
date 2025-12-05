use crate::async_clean_return;
use crate::codec::{JsonCodec, MeteredItem};
use crate::error::JrpkError;
use crate::model::{JrpBytes, JrpData, JrpRecSend, JrpReq, JrpRsp};
use crate::metrics::{spawn_push_prometheus, JrpkMetrics, Labels, LblMethod, LblTier, LblTraffic};
use crate::util::{url_append_tap, Tap};
use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use prometheus_client::registry::Registry;
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use faststr::FastStr;
use moka::future::Cache;
use reqwest::Url;
use tokio::net::TcpStream;
use tokio::try_join;
use tokio_util::codec::{Framed, FramedRead};
use tracing::{debug, error, instrument};

type JrpkMeteredProdReq<'a> = MeteredItem<JrpBytes<JrpReq<'a>>>;

#[instrument(ret, skip(metrics, tcp_sink))]
pub async fn producer_req_writer(
    tap: Tap,
    path: FastStr,
    max_frame_size: usize,
    max_batch_rec_count: usize,
    max_batch_size: usize,
    max_rec_size: usize,
    metrics: JrpkMetrics,
    times: Arc<Cache<usize, Instant>>,
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpkMeteredProdReq<'_>>,
) -> Result<(), JrpkError> {
    let labels = Labels::new(LblTier::Client)
        .method(LblMethod::Send)
        .traffic(LblTraffic::Out)
        .tap(tap.clone())
        .build();
    let throughput = metrics.throughputs.get_or_create_owned(&labels);
    let file = async_clean_return!(tokio::fs::File::open(path.as_str()).await, tcp_sink.close().await);
    let reader = tokio::io::BufReader::with_capacity(1024 * 1024, file);
    let codec = JsonCodec::new(max_frame_size);
    let mut file_stream = FramedRead::with_capacity(reader, codec, max_frame_size);
    let mut id: usize = 0;
    let mut frames: Vec<Bytes> = Vec::with_capacity(max_batch_rec_count);
    let mut records: Vec<JrpRecSend> = Vec::with_capacity(max_batch_rec_count);
    let mut size: usize = 0;
    while let Some(result) = file_stream.next().await {
        let tap = tap.clone();
        let frame = result?;
        let json: &RawValue = unsafe { async_clean_return!( JrpBytes::from_bytes(&frame), tcp_sink.close().await) };
        let jrp_data: JrpData = JrpData::Json(Cow::Borrowed(json));
        let jrp_rec = JrpRecSend::new(None, Some(jrp_data));
        size += frame.len();
        frames.push(frame);
        records.push(jrp_rec);
        if size > max_batch_size - max_rec_size || records.len() >= max_batch_rec_count {
            let jrp_req = JrpReq::send(id, tap.topic, tap.partition, records);
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
        let jrp_req = JrpReq::send(id, tap.topic, tap.partition, records);
        let jrp_bytes = JrpBytes::new(jrp_req, frames);
        times.insert(id, Instant::now()).await;
        let metered_item = JrpkMeteredProdReq::new(jrp_bytes, throughput);
        async_clean_return!(tcp_sink.send(metered_item).await, tcp_sink.close().await);
    }

    async_clean_return!(tcp_sink.flush().await, tcp_sink.close().await);
    tcp_sink.close().await?;
    Ok(())
}

#[instrument(ret, skip(metrics, tcp_stream))]
pub async fn producer_rsp_reader(
    tap: Tap,
    metrics: JrpkMetrics,
    times: Arc<Cache<usize, Instant>>,
    mut tcp_stream: SplitStream<Framed<TcpStream, JsonCodec>>
) -> Result<(), JrpkError> {
    let labels = Labels::new(LblTier::Client)
        .method(LblMethod::Send)
        .traffic(LblTraffic::In)
        .tap(tap)
        .build();
    let throughput = metrics.throughputs.get_or_create_owned(&labels);
    let latency = metrics.latencies.get_or_create_owned(&labels);
    while let Some(result) = tcp_stream.next().await {
        let frame = result?;
        let length = frame.len() as u64;
        let jrp_rsp = serde_json::from_slice::<JrpRsp>(frame.as_ref())?;
        let id = jrp_rsp.id;
        throughput.inc_by(length);
        if let Some(ts) = times.remove(&id).await {
            latency.observe(ts.elapsed().as_secs_f64());
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

#[instrument(ret, err)]
pub async fn produce(
    address: FastStr,
    tap: Tap,
    path: FastStr,
    max_frame_size: usize,
    max_batch_rec_count: usize,
    max_batch_size: usize,
    max_rec_byte_size: usize,
    mut metrics_url: Url,
    metrics_period: Duration,
) -> Result<(), JrpkError> {

    url_append_tap(&mut metrics_url, &tap)?;
    let registry = Arc::new(Mutex::new(Registry::default()));
    let metrics = JrpkMetrics::new(registry.clone());
    let times = Arc::new(Cache::builder().time_to_live(Duration::from_mins(1)).build());
    let ph = spawn_push_prometheus(
        metrics_url,
        metrics_period,
        registry
    );

    let stream = TcpStream::connect(address.as_str()).await?;
    let codec = JsonCodec::new(max_frame_size);
    let framed = Framed::with_capacity(stream, codec, max_frame_size);
    let (tcp_sink, tcp_stream) = framed.split();
    let wh = tokio::spawn(
        producer_req_writer(
            tap.clone(),
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
