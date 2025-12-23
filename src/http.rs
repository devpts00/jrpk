use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use crate::error::JrpkError;
use crate::jsonrpc::JrpCtxTypes;
use crate::kafka::{KfkClientCache, KfkIn, KfkOffset, KfkReq, KfkRsp};
use crate::metrics::{encode_registry, JrpkLabels, JrpkMetrics, LblMethod, LblTier, LblTraffic};
use crate::model::{JrpCodecs, JrpOffset};
use crate::size::Size;
use crate::util::{Ctx, Req, Tap};
use axum::extract::{Path, Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, Response};
use axum::response::IntoResponse;
use axum::routing::get;
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
use axum::body::Body;
use chrono::Utc;
use futures_util::TryStreamExt;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::histogram::Histogram;
use rskafka::record::Record;
use tokio::io::AsyncBufReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::LinesStream;
use tokio_util::io::StreamReader;
use tracing::instrument;

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
    let ctx = JrpCtxTypes::offset(0, tap.clone());
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

#[derive(Deserialize)]
struct HttpFetchQuery {
    from: JrpOffset,
    until: JrpOffset,
    min_size: ByteSize,
    max_size: ByteSize,
    #[serde(with = "humantime_serde")]
    max_wait: Duration,
}

enum KfkFetchState {
    Next {
        id: usize,
        ts: Instant,
        tap: Tap,
        codecs: JrpCodecs,
        from: KfkOffset,
        until: KfkOffset,
        min_max_bytes: Range<i32>,
        max_wait_ms: i32,
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
        id: usize,
        ts: Instant,
        tap: Tap,
        codecs: JrpCodecs,
        from: KfkOffset,
        until: KfkOffset,
        min_max_bytes: Range<i32>,
        max_wait_ms: i32,
        req_snd: Sender<KfkReq<JrpCtxTypes>>,
        rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
        rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
        throughput: Counter,
        latency: Histogram,
    ) -> Self {
        KfkFetchState::Next { id, ts, tap, codecs, from, until, min_max_bytes, max_wait_ms, req_snd, rsp_snd, rsp_rcv, throughput, latency }
    }
    fn done(ts: Instant, latency: Histogram) -> KfkFetchState {
        KfkFetchState::Done { ts, latency }
    }
}

impl Debug for KfkFetchState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KfkFetchState::Next { id, ts, tap, codecs, from, until, min_max_bytes, max_wait_ms, .. } => {
                f.debug_struct("KfkFetchState::Next")
                    .field("id", id)
                    .field("ts", ts)
                    .field("tap", tap)
                    .field("codecs", codecs)
                    .field("from", from)
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
        KfkFetchState::Next { id, ts, tap, codecs, from, until, min_max_bytes, max_wait_ms, req_snd, rsp_snd, mut rsp_rcv, throughput, latency } => {
            let ctx = JrpCtxTypes::fetch(id, tap.clone(), codecs);
            let kfk_req = KfkReq::Fetch(Req(Ctx(ctx, (from, min_max_bytes.clone(), max_wait_ms)), rsp_snd.clone()));
            req_snd.send(kfk_req).await?;
            let kfk_rsp = rsp_rcv.recv().await.ok_or(JrpkError::Unexpected("kafka client does not respond"))?;
            let Ctx(_, kfk_res) = kfk_rsp.fetch_or(JrpkError::Unexpected("kafka response wrong type"))?;
            let (records_and_offsets, high_watermark) = kfk_res?;
            let mut done = false;
            let mut buf = BytesMut::with_capacity(records_and_offsets.size() + records_and_offsets.len());
            let mut pos = 0;
            for ro in records_and_offsets {
                if until > ro && ro.offset < high_watermark {
                    pos = ro.offset;
                    if let Some(value) = ro.record.value {
                        buf.put_slice(&value);
                        buf.put_slice(b"\n");
                    }
                } else {
                    done = true;
                    break;
                }
            }
            let bytes = buf.freeze();
            throughput.inc_by(bytes.len() as u64);
            let frame = Frame::data(bytes);
            let pos = pos + 1;
            if !done && pos < high_watermark {
                let offset = KfkOffset::Pos(pos);
                let next = KfkFetchState::next(id + 1, ts, tap, codecs, offset, until, min_max_bytes, max_wait_ms, req_snd, rsp_snd, rsp_rcv, throughput, latency);
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
    Query(HttpFetchQuery { from, until, min_size, max_size, max_wait }): Query<HttpFetchQuery>,
) -> Result<impl IntoResponse, JrpkError> {
    let HttpState { clients, metrics } = state;
    let tap = Tap::new(topic, partition);
    let labels = JrpkLabels::new(LblTier::Http).method(LblMethod::Fetch).traffic(LblTraffic::Out).tap(tap.clone()).build();
    let throughput = metrics.throughputs.get_or_create_owned(&labels);
    let latency = metrics.latencies.get_or_create_owned(&labels);
    let req_snd = clients.lookup_sender(tap.clone()).await?;
    let from = from.into();
    let until = until.into();
    let bytes = min_size.as_u64() as i32 .. max_size.as_u64() as i32;
    let max_wait_ms = max_wait.as_millis() as i32;
    let (rsp_snd, rsp_rcv) = tokio::sync::mpsc::channel(1);
    let state = KfkFetchState::next(0, Instant::now(), tap, JrpCodecs::default(), from, until, bytes, max_wait_ms, req_snd, rsp_snd, rsp_rcv, throughput, latency);
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
    max_batch_size: usize,
}

/*
#[instrument(level = "debug", err, skip(state))]
async fn post_kafka_send(
    State(state): State<HttpState>,
    Path(HttpFetchPath { topic, partition }): Path<HttpFetchPath>,
    Query(HttpSendQuery { max_batch_size }): Query<HttpSendQuery>,
    request: Request<Body>,
) -> Result<(), JrpkError> {
    let HttpState { clients, metrics } = state;
    let tap = Tap::new(topic, partition);
    let req_snd = clients.lookup_sender(tap).await?;
    let mut max_rec_size: usize = 0;
    let mut batch_size: usize = 0;
    let mut records: Vec<Record> = Vec::with_capacity(1024);

    let body = request.into_body();
    let stream = body.into_data_stream();
    let stream = stream.map_err(std::io::Error::other);
    let reader = StreamReader::new(stream);
    let mut lines = LinesStream::new(reader.lines());

    while let Some(line) = lines.try_next().await? {
        batch_size += line.len();
        max_rec_size = max_rec_size.max(line.len());
        let record = Record { key: None, value: Some(line.into_bytes()), headers: BTreeMap::default(), timestamp: Utc::now() };
        records.push(record);
        if batch_size > max_batch_size - max_rec_size {



            


            batch_size = 0;
            records = Vec::with_capacity(max_rec_size);
        }




        info!("line: {}", line);
    }
    Ok(())
}
 */

#[instrument(ret, err, skip(kafka_clients, prometheus_registry))]
pub async fn listen_http(
    addr: SocketAddr,
    kafka_clients: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
    prometheus_registry: Arc<Mutex<Registry>>
) -> Result<(), JrpkError> {
    let jrpk_metrics = JrpkMetrics::new(prometheus_registry.clone());
    let state = HttpState::new(kafka_clients, jrpk_metrics);
    let listener = TcpListener::bind(addr).await?;
    let metrics = Router::new()
        .route("/", get(get_prometheus_metrics))
        .with_state(prometheus_registry);
    let kafka = Router::new()
        .route("/offset/{topic}/{partition}", get(get_kafka_offset))
        .route("/fetch/{topic}/{partition}", get(get_kafka_fetch))
        //.route("/send/{topic}/{partition}", post(post_kafka_send))
        .with_state(state);
    let root = Router::new()
        .nest("/kafka", kafka)
        .nest("/metrics", metrics);
    axum::serve(listener, root).await?;
    Ok(())
}
