use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use crate::error::JrpkError;
use crate::jsonrpc::{JrpCtx, JrpCtxTypes};
use crate::kafka::{KfkClientCache, KfkOffset, KfkReq, KfkRsp};
use crate::metrics::{encode_registry, JrpkLabels, JrpkMetrics, LblMethod, LblTier, LblTraffic};
use crate::model::{JrpCodecs, JrpOffset};
use crate::size::Size;
use crate::util::{Ctx, Req, Tap};
use axum::extract::{Path, Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, Response};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use bytes::{BufMut, Bytes, BytesMut};
use bytesize::ByteSize;
use faststr::FastStr;
use futures_util::stream::try_unfold;
use http_body_util::StreamBody;
use hyper::body::Frame;
use prometheus_client::registry::Registry;
use serde::Deserialize;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use axum::body::{Body, BodyDataStream};
use chrono::Utc;
use futures_util::TryStreamExt;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::histogram::Histogram;
use rskafka::record::Record;
use tokio::io::AsyncBufReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::{spawn, try_join};
use tokio_stream::wrappers::LinesStream;
use tokio_util::io::StreamReader;
use tracing::{debug, info, instrument};

#[instrument(level="trace", ret, err, skip(registry))]
async fn get_prometheus_metrics(State(registry): State<Arc<Mutex<Registry>>>) -> Result<String, JrpkError> {
    let text = encode_registry(registry)?;
    Ok(text)
}

#[derive(Clone)]
struct HttpState {
    clients: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
    metrics: JrpkMetrics,
}

impl HttpState {
    fn new(clients: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>, metrics: JrpkMetrics) -> Self {
        HttpState { clients, metrics }
    }
}

#[derive(Deserialize)]
struct HttpOffsetQuery {
    at: JrpOffset,
}

#[instrument(ret, err, skip(state))]
async fn get_kafka_offset(
    State(state): State<HttpState>,
    Path((topic, partition)): Path<(FastStr, i32)>,
    Query(HttpOffsetQuery { at }): Query<HttpOffsetQuery>,
) -> Result<String, JrpkError> {
    let HttpState { clients, metrics } = state;
    let ts = Instant::now();
    let tap = Tap::new(topic, partition);
    let ctx = JrpCtxTypes::offset(0, Instant::now(), tap.clone());
    let req_snd = clients.lookup_sender(tap.clone()).await?;
    let at = at.into();
    let (rsp_snd, mut rsp_rcv) = tokio::sync::mpsc::channel(1);
    let kfk_req = KfkReq::Offset(Req(Ctx(ctx, at), rsp_snd));
    req_snd.send(kfk_req).await?;
    let kfk_rsp = rsp_rcv.recv().await.ok_or(JrpkError::Unexpected("kafka client does not respond"))?;
    let Ctx(_, kfk_offset_res) = kfk_rsp.offset_or(JrpkError::Unexpected("kafka client wrong response"))?;
    let offset = kfk_offset_res?;
    let body = offset.to_string();
    let labels = JrpkLabels::new(LblTier::Http).method(LblMethod::Offset).traffic(LblTraffic::Out).tap(tap).build();
    metrics.throughput(&labels, &body);
    metrics.latency(&labels, ts);
    Ok(offset.to_string())
}

#[derive(Deserialize)]
struct HttpFetchPath {
    topic: FastStr,
    partition: i32,
}

const fn default_from() -> JrpOffset {
    JrpOffset::Earliest
}

const fn default_until() -> JrpOffset {
    JrpOffset::Latest
}

const fn default_min_batch_size() -> ByteSize {
    ByteSize::kib(1)
}

const fn default_max_batch_size() -> ByteSize {
    ByteSize::mib(1)
}

const fn default_max_wait() -> Duration {
    Duration::from_millis(100)
}

const fn default_max_rec_count() -> usize {
    ByteSize::mib(1).as_u64() as usize
}

const fn default_max_bytes_size() -> ByteSize {
    ByteSize::gib(1)
}

#[derive(Deserialize)]
struct HttpFetchQuery {
    #[serde(default = "default_from")]
    from: JrpOffset,
    #[serde(default = "default_until")]
    until: JrpOffset,
    #[serde(default = "default_min_batch_size")]
    min_batch_size: ByteSize,
    #[serde(default = "default_max_batch_size")]
    max_batch_size: ByteSize,
    #[serde(default = "default_max_wait", with = "humantime_serde")]
    max_wait_duration: Duration,
    #[serde(default = "default_max_rec_count")]
    max_rec_count: usize,
    #[serde(default = "default_max_bytes_size")]
    max_bytes_size: ByteSize,
}

enum KfkFetchState {
    Next {
        until: KfkOffset,
        min_max_bytes: Range<i32>,
        max_wait_ms: i32,
        rec_count_budget: usize,
        byte_size_budget: usize,
        req_snd: Sender<KfkReq<JrpCtxTypes>>,
        rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
        rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
        throughput: Counter,
        latency: Histogram,
    },
    Done {
        ts: Instant,
        latency: Histogram,
    },
}

impl KfkFetchState {
    fn next(
        until: KfkOffset,
        min_max_bytes: Range<i32>,
        max_wait_ms: i32,
        rec_count_budget: usize,
        byte_size_budget: usize,
        req_snd: Sender<KfkReq<JrpCtxTypes>>,
        rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
        rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
        throughput: Counter,
        latency: Histogram,
    ) -> Self {
        KfkFetchState::Next { until, min_max_bytes, max_wait_ms, rec_count_budget, byte_size_budget, req_snd, rsp_snd, rsp_rcv, throughput, latency }
    }
    fn done(ts: Instant, latency: Histogram) -> KfkFetchState {
        KfkFetchState::Done { ts, latency }
    }
}

impl Debug for KfkFetchState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KfkFetchState::Next { until, min_max_bytes, max_wait_ms, .. } => {
                f.debug_struct("KfkFetchState::Next")
                    .field("until", until)
                    .field("min_max_bytes", min_max_bytes)
                    .field("max_wait_ms", max_wait_ms)
                    .finish()
            }
            KfkFetchState::Done { ts, .. } => {
                f.debug_struct("KfkFetchState::Done")
                    .field("ts", ts)
                    .finish()
            }
        }
    }
}

#[instrument(level = "debug", err)]
async fn get_kafka_fetch_chunk(state: KfkFetchState) -> Result<Option<(Frame<Bytes>, KfkFetchState)>, JrpkError> {
    match state {
        KfkFetchState::Next { until, min_max_bytes, max_wait_ms, mut rec_count_budget, mut byte_size_budget, req_snd, rsp_snd, mut rsp_rcv, throughput, latency } => {
            let kfk_rsp = rsp_rcv.recv().await.ok_or(JrpkError::Unexpected("kafka client does not respond"))?;
            let Ctx(ctx, kfk_res) = kfk_rsp.fetch_or(JrpkError::Unexpected("kafka response wrong type"))?;
            let JrpCtx { id, ts, tap, extra } = ctx;
            let (records_and_offsets, high_watermark) = kfk_res?;
            let mut done = false;
            let mut buf = BytesMut::with_capacity(records_and_offsets.size() + records_and_offsets.len());
            let mut pos = 0;
            for mut ro in records_and_offsets {
                pos = ro.offset;
                if let Some(value) = ro.record.value.take() {
                    let size = value.len() + 1;
                    if until > ro && ro.offset < high_watermark && rec_count_budget > 0 && byte_size_budget > size {
                        rec_count_budget -= 1;
                        byte_size_budget -= size;
                        buf.put_slice(&value);
                        buf.put_slice(b"\n");
                    } else {
                        done = true;
                        break;
                    }
                }
            }
            let bytes = buf.freeze();
            throughput.inc_by(bytes.len() as u64);
            let frame = Frame::data(bytes);
            let pos = pos + 1;
            if !done && pos < high_watermark {
                let from = KfkOffset::Pos(pos);
                let ctx = JrpCtx::new(id + 1, ts, tap, extra);
                let req = KfkReq::Fetch(Req(Ctx(ctx, (from, min_max_bytes.clone(), max_wait_ms)), rsp_snd.clone()));
                req_snd.send(req).await?;
                let next = KfkFetchState::next(until, min_max_bytes, max_wait_ms, rec_count_budget, byte_size_budget, req_snd, rsp_snd, rsp_rcv, throughput, latency);
                Ok(Some((frame, next)))
            } else {
                Ok(Some((frame, KfkFetchState::done(ts, latency))))
            }
        },
        KfkFetchState::Done { ts, latency } => {
            latency.observe(ts.elapsed().as_secs_f64());
            Ok(None)
        }
    }
}

#[instrument(err, skip(state))]
async fn get_kafka_fetch(
    State(state): State<HttpState>,
    Path(HttpFetchPath { topic, partition }): Path<HttpFetchPath>,
    Query(HttpFetchQuery { from, until, min_batch_size, max_batch_size, max_wait_duration, max_rec_count, max_bytes_size }): Query<HttpFetchQuery>,
) -> Result<impl IntoResponse, JrpkError> {
    let HttpState { clients, metrics } = state;
    let tap = Tap::new(topic, partition);
    let labels = JrpkLabels::new(LblTier::Http).method(LblMethod::Fetch).traffic(LblTraffic::Out).tap(tap.clone()).build();
    let throughput = metrics.throughputs.get_or_create_owned(&labels);
    let latency = metrics.latencies.get_or_create_owned(&labels);
    let req_snd = clients.lookup_sender(tap.clone()).await?;
    let from = from.into();
    let until = until.into();
    let min_max_bytes = min_batch_size.as_u64() as i32 .. max_batch_size.as_u64() as i32;
    let max_wait_ms = max_wait_duration.as_millis() as i32;
    let rec_count_budget = max_rec_count;
    let byte_size_budget = max_bytes_size.as_u64() as usize;
    let (rsp_snd, rsp_rcv) = tokio::sync::mpsc::channel(1);

    let ctx = JrpCtxTypes::fetch(0, Instant::now(), tap, JrpCodecs::default());
    let req = KfkReq::Fetch(Req(Ctx(ctx, (from, min_max_bytes.clone(), max_wait_ms)), rsp_snd.clone()));
    req_snd.send(req).await?;

    let state = KfkFetchState::next(until, min_max_bytes, max_wait_ms, rec_count_budget, byte_size_budget, req_snd, rsp_snd, rsp_rcv, throughput, latency);
    let stream = try_unfold(state, get_kafka_fetch_chunk);
    let body = StreamBody::new(stream);
    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "application/json-lines")
        .body(body)?;
    Ok(response)
}

#[derive(Deserialize)]
struct HttpSendQuery {
    max_batch_size: ByteSize,
}

#[instrument(err, skip(stream))]
async fn post_kafka_send_send_requests(
    stream: BodyDataStream,
    req_snd: Sender<KfkReq<JrpCtxTypes>>,
    rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
    tap: Tap,
    max_batch_size: usize,
    throughput: Counter,
) -> Result<(), JrpkError> {
    let mut max_rec_size: usize = 0;
    let mut batch_size: usize = 0;
    let mut records: Vec<Record> = Vec::with_capacity(1024);
    let stream = stream.map_err(std::io::Error::other);
    let reader = StreamReader::new(stream);
    let mut lines = LinesStream::new(reader.lines());
    let mut id: usize = 0;
    while let Some(line) = lines.try_next().await? {
        batch_size += line.len();
        max_rec_size = max_rec_size.max(line.len());
        let record = Record { key: None, value: Some(line.into_bytes()), headers: BTreeMap::default(), timestamp: Utc::now() };
        records.push(record);
        if batch_size > max_batch_size - max_rec_size {
            throughput.inc_by(batch_size as u64);
            let recs_count = records.len();
            let ctx = JrpCtxTypes::send(id, Instant::now(), tap.clone());
            let req = KfkReq::Send(Req(Ctx(ctx, records), rsp_snd.clone()));
            req_snd.send(req).await?;
            id = id + 1;
            batch_size = 0;
            records = Vec::with_capacity(recs_count);
        }
    }

    if !records.is_empty() {
        throughput.inc_by(batch_size as u64);
        let ctx = JrpCtxTypes::send(id, Instant::now(), tap.clone());
        let req = KfkReq::Send(Req(Ctx(ctx, records), rsp_snd.clone()));
        req_snd.send(req).await?;
    }

    Ok(())
}

#[instrument(err, skip(rsp_rcv))]
async fn post_kafka_send_proc_responses(
    mut rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
    throughput: Counter,
    latency: Histogram,
) -> Result<(), JrpkError> {
    let ts = Instant::now();
    while let Some(rsp) = rsp_rcv.recv().await {
        let Ctx(ctx, res) = rsp.send_or(JrpkError::Unexpected("kafka response wrong type"))?;
        let JrpCtx { id, ts, tap, .. } = ctx;
        debug!("response, id: {}, tap: {}, duration: {} ms", id, tap, ts.elapsed().as_millis());
        let offsets = res?;
        throughput.inc_by(offsets.size() as u64);
    }
    latency.observe(ts.elapsed().as_secs_f64());
    Ok(())
}

#[instrument(ret, err, skip(state))]
async fn post_kafka_send(
    State(state): State<HttpState>,
    Path(HttpFetchPath { topic, partition }): Path<HttpFetchPath>,
    Query(HttpSendQuery { max_batch_size }): Query<HttpSendQuery>,
    request: Request<Body>,
) -> Result<(), JrpkError> {
    let HttpState { clients, metrics } = state;
    let tap = Tap::new(topic, partition);
    let mut labels = JrpkLabels::new(LblTier::Http).method(LblMethod::Send).traffic(LblTraffic::In).tap(tap.clone()).build();
    let latency = metrics.latencies.get_or_create_owned(&labels);
    let throughput_in = metrics.throughputs.get_or_create_owned(&labels);
    let labels = labels.traffic(LblTraffic::Out);
    let throughput_out = metrics.throughputs.get_or_create_owned(&labels);
    let max_batch_size = max_batch_size.as_u64() as usize;
    let req_snd = clients.lookup_sender(tap.clone()).await?;
    let (rsp_snd, rsp_rcv) = tokio::sync::mpsc::channel::<KfkRsp<JrpCtxTypes>>(1024);
    let body = request.into_body();
    let stream = body.into_data_stream();
    let rsp_h = spawn(post_kafka_send_proc_responses(rsp_rcv, throughput_out, latency));
    let req_h = spawn(post_kafka_send_send_requests(stream, req_snd, rsp_snd, tap, max_batch_size, throughput_in));
    let _ = try_join!(req_h, rsp_h)?;
    Ok(())
}

#[instrument(ret, err, skip(kafka_clients, prometheus_registry))]
pub async fn listen_http(
    bind: SocketAddr,
    kafka_clients: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
    prometheus_registry: Arc<Mutex<Registry>>
) -> Result<(), JrpkError> {
    let jrpk_metrics = JrpkMetrics::new(prometheus_registry.clone());
    let state = HttpState::new(kafka_clients, jrpk_metrics);
    let listener = TcpListener::bind(bind).await?;
    let metrics = Router::new()
        .route("/", get(get_prometheus_metrics))
        .with_state(prometheus_registry);
    let kafka = Router::new()
        .route("/offset/{topic}/{partition}", get(get_kafka_offset))
        .route("/fetch/{topic}/{partition}", get(get_kafka_fetch))
        .route("/send/{topic}/{partition}", post(post_kafka_send))
        .with_state(state);
    let root = Router::new()
        .nest("/kafka", kafka)
        .nest("/metrics", metrics);
    axum::serve(listener, root).await?;
    Ok(())
}
