use crate::async_clean_return;
use crate::codec::LinesCodec;
use crate::error::JrpkError;
use crate::model::{b2j, JrpCodec, JrpReq, JrpReqBuilder, JrpRsp, JrpkMeteredProdReq};
use crate::metrics::{spawn_push_prometheus, JrpkMetrics, JrpkLabels, LblMethod, LblTier, LblTraffic};
use crate::util::{join_with_quit, url_append_tap, Tap};
use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::{Duration, Instant};
use faststr::FastStr;
use futures_util::future::join_all;
use moka::future::Cache;
use reqwest::Url;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, FramedRead};
use tracing::{debug, error, instrument};
use crate::args::FileFormat;

#[instrument(ret, skip(metrics, times, tcp_sink))]
pub async fn producer_req_writer(
    jrp_frame_max_size: usize,
    jrp_send_max_size: usize,
    jrp_send_max_rec_count: usize,
    jrp_send_max_rec_size: usize,
    jrp_value_codec: JrpCodec,
    kfk_tap: Tap,
    file_path: FastStr,
    file_format: FileFormat,
    file_load_max_rec_count: usize,
    file_load_max_size: usize,
    metrics: Arc<JrpkMetrics>,
    times: Arc<Cache<usize, Instant>>,
    mut tcp_sink: SplitSink<Framed<TcpStream, LinesCodec>, JrpkMeteredProdReq>,
) -> Result<(), JrpkError> {

    let mut file_load_rec_count_budget = file_load_max_rec_count;
    let mut file_load_size_budget = file_load_max_size;

    let b2j = b2j(file_format, jrp_value_codec);

    let labels = JrpkLabels::new(LblTier::Client)
        .method(LblMethod::Send)
        .traffic(LblTraffic::Out)
        .tap(kfk_tap.clone())
        .build();

    let file = async_clean_return!(tokio::fs::File::open(file_path.as_str()).await, tcp_sink.close().await);
    let reader = tokio::io::BufReader::with_capacity(jrp_frame_max_size, file);
    let codec = LinesCodec::new_with_max_length(jrp_frame_max_size);
    let mut framed = FramedRead::with_capacity(reader, codec, jrp_frame_max_size);
    let mut id: usize = 0;

    let mut frames: Vec<Bytes> = Vec::with_capacity(jrp_send_max_rec_count);
    let mut batch_length: usize = 0;
    while let Some(result) = framed.next().await {

        let frame = result?;
        if file_load_rec_count_budget == 0 && file_load_size_budget < frame.len() {
            break;
        } else {
            file_load_rec_count_budget -= 1;
            file_load_size_budget -= frame.len();
        }

        let kfk_tap = kfk_tap.clone();
        batch_length += frame.len();
        frames.push(frame);
        if batch_length > jrp_send_max_size - jrp_send_max_rec_size || frames.len() >= jrp_send_max_rec_count {
            debug!("produce, batch-size: {}, max-rec-size: {}", batch_length, jrp_send_max_rec_size);
            let jrp_req_builder = JrpReqBuilder::new(id, kfk_tap.topic, kfk_tap.partition, frames, b2j);
            frames = Vec::with_capacity(jrp_send_max_rec_count);
            times.insert(id, Instant::now()).await;
            let metered_item = JrpkMeteredProdReq::new(jrp_req_builder, metrics.clone(), labels.clone());
            async_clean_return!(tcp_sink.send(metered_item).await, tcp_sink.close().await);
            id += 1;
            batch_length = 0;
        }
    }

    if !frames.is_empty() {
        let jrp_req_builder = JrpReqBuilder::new(id, kfk_tap.topic, kfk_tap.partition, frames, b2j);
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
    kfk_tap: Tap,
    metrics: Arc<JrpkMetrics>,
    times: Arc<Cache<usize, Instant>>,
    mut tcp_stream: SplitStream<Framed<TcpStream, LinesCodec>>,
) -> Result<(), JrpkError> {
    let labels = JrpkLabels::new(LblTier::Client)
        .method(LblMethod::Send)
        .traffic(LblTraffic::In)
        .tap(kfk_tap)
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

#[instrument(ret, err, skip(prom_push_url))]
pub async fn produce(
    jrp_address: FastStr,
    jrp_frame_max_size: usize,
    jrp_send_max_size: usize,
    jrp_send_max_rec_count: usize,
    jrp_send_max_rec_size: usize,
    jrp_value_codec: JrpCodec,
    kfk_topic: FastStr,
    kfk_partition: i32,
    file_path: FastStr,
    file_format: FileFormat,
    file_load_max_rec_count: usize,
    file_load_max_size: usize,
    mut prom_push_url: Url,
    prom_push_period: Duration,
) -> Result<(), JrpkError> {

    let kfk_tap = Tap::new(kfk_topic, kfk_partition);
    let metrics = Arc::new(JrpkMetrics::new());

    url_append_tap(&mut prom_push_url, &kfk_tap)?;
    let times = Arc::new(Cache::builder().time_to_live(Duration::from_mins(1)).build());
    let ph = spawn_push_prometheus(
        prom_push_url,
        prom_push_period,
        metrics.clone()
    );

    let stream = TcpStream::connect(jrp_address.as_str()).await?;
    let codec = LinesCodec::new_with_max_length(jrp_frame_max_size);
    let framed = Framed::with_capacity(stream, codec, jrp_frame_max_size);
    let (tcp_sink, tcp_stream) = framed.split();
    let wh = tokio::spawn(
        producer_req_writer(
            jrp_frame_max_size,
            jrp_send_max_size,
            jrp_send_max_rec_count,
            jrp_send_max_rec_size,
            jrp_value_codec,
            kfk_tap.clone(),
            file_path,
            file_format,
            file_load_max_rec_count,
            file_load_max_size,
            metrics.clone(),
            times.clone(),
            tcp_sink
        )
    );
    let rh = tokio::spawn(
        producer_rsp_reader(
            kfk_tap,
            metrics.clone(),
            times.clone(),
            tcp_stream
        )
    );

    join_with_quit(join_all(vec!(wh, rh))).await;
    let _ = ph.cancel().await?;

    Ok(())
}
