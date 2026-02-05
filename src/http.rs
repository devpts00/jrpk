use std::fmt::{Debug, Formatter};
use crate::error::JrpkError;
use crate::jsonrpc::{k2j_rec_fetch, JrpCtx, JrpCtxTypes};
use crate::kafka::{KfkClientCache, KfkOffset, KfkReq, KfkRsp, KfkTap};
use crate::metrics::{JrpkLabels, JrpkMetrics, LblMethod, LblTier, LblTraffic};
use crate::model::{b2j, b2j_rec_vec, write_format, JrpCodec, JrpOffset, JrpSelector, Progress};
use crate::util::{Budget, Ctx, Req};
use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, Response};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use bytesize::ByteSize;
use faststr::FastStr;
use futures_util::stream::try_unfold;
use http_body_util::StreamBody;
use hyper::body::Frame;
use serde::{Deserialize, Deserializer};
use std::net::SocketAddr;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use axum::body::{Body, BodyDataStream};
use axum_extra::extract::Query;
use futures_util::{StreamExt, TryStreamExt};
use reqwest::Url;
use rskafka::record::Record;
use serde::de::{Error, Visitor};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::{spawn, try_join};
use tokio_util::codec::FramedRead;
use tokio_util::io::StreamReader;
use tracing::{debug, instrument, trace};
use crate::args::{FileFormat, NamedCodec};
use crate::codec::LinesCodec;

#[inline]
pub fn url_append_tap(url: &mut Url, tap: &KfkTap) -> Result<(), JrpkError> {
    match url.path_segments_mut() {
        Ok(mut segments) => {
            segments.push("topic");
            segments.push(tap.topic.as_str());
            segments.push("partition");
            segments.push(tap.partition.to_string().as_str());
            Ok(())
        },
        Err(_) => {
            Err(JrpkError::Url)
        }
    }
}

#[instrument(level="trace", ret, err, skip(metrics))]
async fn get_prometheus_metrics(State(metrics): State<Arc<JrpkMetrics>>) -> Result<String, JrpkError> {
    let buf = metrics.encode()?;
    Ok(buf)
}

impl <'de> Deserialize<'de> for NamedCodec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct NamedCodecVisitor;
        impl <'de> Visitor<'de> for NamedCodecVisitor {
            type Value = NamedCodec;
            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, "header1:json,header2:str,header3:base64")
            }
            fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
                NamedCodec::from_str(v).map_err(Error::custom)
            }
        }
        d.deserialize_str(NamedCodecVisitor)
    }
}

impl <'de> Deserialize<'de> for FileFormat {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct FormatVisitor;
        impl <'de> Visitor<'de> for FormatVisitor {
            type Value = FileFormat;
            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, "record or value")
            }
            fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
                FileFormat::from_str(v).map_err(Error::custom)
            }
        }
        d.deserialize_str(FormatVisitor)
    }
}

#[derive(Clone)]
struct HttpState {
    kfk_clients: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
    metrics: Arc<JrpkMetrics>,
}

impl HttpState {
    fn new(kfk_clients: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>, metrics: Arc<JrpkMetrics>) -> Self {
        HttpState { kfk_clients, metrics }
    }
}

#[derive(Deserialize)]
struct HttpOffsetQuery {
    kfk_offset: JrpOffset,
}

#[instrument(level="debug", ret, err, skip(kfk_clients, metrics))]
async fn get_kafka_offset(
    State(HttpState { kfk_clients, metrics, }): State<HttpState>,
    Path(HttpKfkPath { kfk_topic, kfk_partition }): Path<HttpKfkPath>,
    Query(HttpOffsetQuery { kfk_offset }): Query<HttpOffsetQuery>,
) -> Result<String, JrpkError> {
    let ts = Instant::now();
    let kfk_tap = KfkTap::new(kfk_topic, kfk_partition);
    let ctx = JrpCtxTypes::offset(0, Instant::now(), kfk_tap.clone());
    let kfk_req_snd = kfk_clients.lookup_sender(kfk_tap.clone()).await?;
    let kfk_offset = kfk_offset.into();
    let (rsp_snd, mut rsp_rcv) = tokio::sync::mpsc::channel(1);
    let kfk_req = KfkReq::Offset(Req(Ctx(ctx, kfk_offset), rsp_snd));
    kfk_req_snd.send(kfk_req).await?;
    let kfk_rsp = rsp_rcv.recv().await.ok_or(JrpkError::Unexpected("kafka client does not respond"))?;
    let Ctx(_, kfk_offset_res) = kfk_rsp.offset_or(JrpkError::Unexpected("kafka client wrong response"))?;
    let offset = kfk_offset_res?;
    let body = offset.to_string();
    let labels = JrpkLabels::new(LblTier::Http).method(LblMethod::Offset).traffic(LblTraffic::Out).tap(kfk_tap).build();
    metrics.size(&labels, &body);
    metrics.time(&labels, ts);
    Ok(offset.to_string())
}

#[derive(Deserialize)]
pub struct HttpKfkPath {
    pub kfk_topic: FastStr,
    pub kfk_partition: i32,
}

const fn default_jrp_codec_value() -> JrpCodec {
    JrpCodec::Json
}

const fn default_jrp_header_codecs() -> Vec<NamedCodec> { Vec::new() }

const fn default_kfk_offset_from() -> JrpOffset {
    JrpOffset::Earliest
}

const fn default_kfk_offset_until() -> JrpOffset {
    JrpOffset::Latest
}

const fn default_kfk_fetch_min_size() -> ByteSize {
    ByteSize::kib(1)
}

const fn default_kfk_fetch_max_size() -> ByteSize {
    ByteSize::mib(1)
}

const fn default_kfk_fetch_max_wait_time() -> Duration {
    Duration::from_millis(100)
}

const fn default_file_format() -> FileFormat { FileFormat::Value }

const fn default_file_save_max_rec_count() -> usize { usize::MAX }

const fn default_file_save_max_size() -> ByteSize { ByteSize::b(u64::MAX) }

#[derive(Deserialize)]
struct HttpFetchQuery {
    jrp_key_codec: Option<JrpCodec>,
    #[serde(default = "default_jrp_codec_value")]
    jrp_value_codec: JrpCodec,
    #[serde(default = "default_jrp_header_codecs")]
    jrp_header_codecs: Vec<NamedCodec>,
    jrp_header_codec_default: Option<JrpCodec>,

    #[serde(default = "default_kfk_offset_from")]
    kfk_offset_from: JrpOffset,
    #[serde(default = "default_kfk_offset_until")]
    kfk_offset_until: JrpOffset,
    #[serde(default = "default_kfk_fetch_min_size")]
    kfk_fetch_min_size: ByteSize,
    #[serde(default = "default_kfk_fetch_max_size")]
    kfk_fetch_max_size: ByteSize,
    #[serde(default = "default_kfk_fetch_max_wait_time", with = "humantime_serde")]
    kfk_fetch_max_wait_time: Duration,

    #[serde(default = "default_file_format")]
    file_format: FileFormat,
    #[serde(default = "default_file_save_max_rec_count")]
    file_save_max_rec_count: usize,
    #[serde(default = "default_file_save_max_size")]
    file_save_max_size: ByteSize,
}

enum KfkFetchState {
    Next {
        kfk_offset_until: KfkOffset,
        kfk_fetch_min_max_bytes: Range<i32>,
        kfk_fetch_max_wait_ms: i32,
        file_budget: Budget,
        file_format: FileFormat,
        req_snd: Sender<KfkReq<JrpCtxTypes>>,
        rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
        rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
        metrics: Arc<JrpkMetrics>,
        labels: JrpkLabels,
    },
    Done {
        ts: Instant,
        metrics: Arc<JrpkMetrics>,
        labels: JrpkLabels,
    },
}

impl KfkFetchState {
    fn next(
        kfk_offset_until: KfkOffset,
        kfk_fetch_min_max_bytes: Range<i32>,
        kfk_fetch_max_wait_ms: i32,
        file_budget: Budget,
        file_format: FileFormat,
        req_snd: Sender<KfkReq<JrpCtxTypes>>,
        rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
        rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
        metrics: Arc<JrpkMetrics>,
        labels: JrpkLabels,
    ) -> Self {
        KfkFetchState::Next { kfk_offset_until, kfk_fetch_min_max_bytes, kfk_fetch_max_wait_ms, file_budget, file_format, req_snd, rsp_snd, rsp_rcv, metrics, labels }
    }
    fn done(
        ts: Instant,
        metrics: Arc<JrpkMetrics>,
        labels: JrpkLabels,
    ) -> KfkFetchState {
        KfkFetchState::Done { ts, metrics, labels }
    }
}

impl Debug for KfkFetchState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KfkFetchState::Next {
                kfk_offset_until,
                kfk_fetch_min_max_bytes,
                kfk_fetch_max_wait_ms,
                file_budget,
                file_format,
                ..
            } => {
                f.debug_struct("KfkFetchState::Next")
                    .field("kfk_offset_until", kfk_offset_until)
                    .field("kfk_fetch_min_max_bytes", kfk_fetch_min_max_bytes)
                    .field("kfk_fetch_max_wait_ms", kfk_fetch_max_wait_ms)
                    .field("file_budget", file_budget)
                    .field("file_format", file_format)
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
        KfkFetchState::Next {
            kfk_offset_until,
            kfk_fetch_min_max_bytes,
            kfk_fetch_max_wait_ms,
            mut file_budget,
            file_format,
            req_snd,
            rsp_snd,
            mut rsp_rcv,
            metrics,
            labels
        } => {
            let flush_size = 64 * 1024;
            let kfk_rsp = rsp_rcv.recv().await
                .ok_or(JrpkError::Unexpected("kafka client does not respond"))?;
            let Ctx(ctx, kfk_res) = kfk_rsp
                .fetch_or(JrpkError::Unexpected("kafka response wrong type"))?;
            let JrpCtx { id, ts, tap: kfk_tap, extra: jrp_selector } = ctx;
            let (ros, high_watermark) = kfk_res?;

            let mut buf = Vec::with_capacity(64 * 1024);

            let records = ros.into_iter()
                .map(|ro| k2j_rec_fetch(ro, &jrp_selector));

            let progress = write_format(file_format, records, kfk_offset_until.into(), flush_size, &mut file_budget, &mut buf)?;

            let bytes = buf.into();
            metrics.size(&labels, &bytes);
            let frame = Frame::data(bytes);

            match progress {
                Progress::Continue(pos) => {
                    trace!("continue");
                    if pos < high_watermark {
                        let from = KfkOffset::Pos(pos);
                        let ctx = JrpCtx::new(id + 1, ts, kfk_tap, jrp_selector);
                        let req = KfkReq::Fetch(Req(Ctx(ctx, (from, kfk_fetch_min_max_bytes.clone(), kfk_fetch_max_wait_ms)), rsp_snd.clone()));
                        req_snd.send(req).await?;
                        let next = KfkFetchState::next(
                            kfk_offset_until,
                            kfk_fetch_min_max_bytes,
                            kfk_fetch_max_wait_ms,
                            file_budget,
                            file_format,
                            req_snd,
                            rsp_snd,
                            rsp_rcv,
                            metrics,
                            labels
                        );
                        Ok(Some((frame, next)))
                    } else {
                        Ok(Some((frame, KfkFetchState::done(ts, metrics, labels))))
                    }
                }
                Progress::Done => {
                    debug!("break: until reached");
                    Ok(Some((frame, KfkFetchState::done(ts, metrics, labels))))
                }
                Progress::Overdraft => {
                    debug!("break: budget overdraft");
                    Ok(Some((frame, KfkFetchState::done(ts, metrics, labels))))
                }
            }
        },
        KfkFetchState::Done { ts, metrics, labels } => {
            metrics.time(&labels, ts);
            Ok(None)
        }
    }
}

#[instrument(level="info", err, skip(kfk_clients, metrics))]
async fn get_kafka_fetch(
    State(
        HttpState {
            kfk_clients,
            metrics
        }
    ): State<HttpState>,
    Path(
        HttpKfkPath {
            kfk_topic,
            kfk_partition
        }
    ): Path<HttpKfkPath>,
    Query(
        HttpFetchQuery {
            jrp_key_codec,
            jrp_value_codec,
            jrp_header_codecs,
            jrp_header_codec_default,
            kfk_offset_from,
            kfk_offset_until,
            kfk_fetch_min_size,
            kfk_fetch_max_size,
            kfk_fetch_max_wait_time,
            file_format,
            file_save_max_rec_count,
            file_save_max_size
        }
    ): Query<HttpFetchQuery>,
) -> Result<impl IntoResponse, JrpkError> {
    let kfk_tap = KfkTap::new(kfk_topic, kfk_partition);
    let labels = JrpkLabels::new(LblTier::Http).method(LblMethod::Fetch).traffic(LblTraffic::Out).tap(kfk_tap.clone()).build();
    let kfk_req_snd = kfk_clients.lookup_sender(kfk_tap.clone()).await?;
    let kfk_offset_from = kfk_offset_from.into();
    let kfk_offset_until = kfk_offset_until.into();
    let kfk_fetch_min_max_bytes = kfk_fetch_min_size.as_u64() as i32 .. kfk_fetch_max_size.as_u64() as i32;
    let kfk_fetch_max_wait_ms = kfk_fetch_max_wait_time.as_millis() as i32;
    let file_budget = Budget::new(file_save_max_size.as_u64() as usize, file_save_max_rec_count);
    let (kfk_rsp_snd, kfk_rsp_rcv) = tokio::sync::mpsc::channel(1);
    let jrp_header_codecs = jrp_header_codecs.into_iter().map(|nc|nc.into()).collect();
    let jrp_selector = JrpSelector::new(jrp_key_codec, jrp_value_codec, jrp_header_codecs, jrp_header_codec_default);
    let ctx = JrpCtxTypes::fetch(0, Instant::now(), kfk_tap, jrp_selector);
    let kfk_req = KfkReq::Fetch(Req(Ctx(ctx, (kfk_offset_from, kfk_fetch_min_max_bytes.clone(), kfk_fetch_max_wait_ms)), kfk_rsp_snd.clone()));
    kfk_req_snd.send(kfk_req).await?;

    let state = KfkFetchState::next(
        kfk_offset_until,
        kfk_fetch_min_max_bytes,
        kfk_fetch_max_wait_ms,
        file_budget,
        file_format,
        kfk_req_snd,
        kfk_rsp_snd,
        kfk_rsp_rcv,
        metrics,
        labels
    );
    let stream = try_unfold(state, get_kafka_fetch_chunk);
    let body = StreamBody::new(stream);
    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "application/json-lines")
        .body(body)?;
    Ok(response)
}

const fn default_jrp_send_max_size() -> ByteSize {
    ByteSize::kib(256)
}

const fn default_jrp_send_max_rec_count() -> usize {
    1000
}

const fn default_jrp_send_max_rec_size() -> ByteSize {
    ByteSize::kib(1)
}



#[derive(Deserialize)]
struct HttpSendQuery {
    #[serde(default = "default_jrp_codec_value")]
    jrp_value_codec: JrpCodec,
    #[serde(default = "default_jrp_send_max_size")]
    jrp_send_max_size: ByteSize,
    #[serde(default = "default_jrp_send_max_rec_count")]
    jrp_send_max_rec_count: usize,
    #[serde(default = "default_jrp_send_max_rec_size")]
    jrp_send_max_rec_size: ByteSize,
    #[serde(default = "default_file_format")]
    file_format: FileFormat,
}

#[instrument(level="info", err, skip(http_stream, kfk_req_snd, kfk_rsp_snd, metrics, labels))]
async fn post_kafka_send_proc_requests(
    http_stream: BodyDataStream,
    jrp_value_codec: JrpCodec,
    jrp_send_max_size: usize,
    jrp_send_max_rec_count: usize,
    mut jrp_send_max_rec_size: usize,
    kfk_tap: KfkTap,
    kfk_req_snd: Sender<KfkReq<JrpCtxTypes>>,
    kfk_rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
    file_format: FileFormat,
    metrics: Arc<JrpkMetrics>,
    labels: JrpkLabels,
) -> Result<(), JrpkError> {
    let http_stream = http_stream.map_err(std::io::Error::other);
    let http_reader = StreamReader::new(http_stream);
    let http_codec = LinesCodec::new_with_max_length(jrp_send_max_size);
    let mut http_framed = FramedRead::with_capacity(http_reader, http_codec, jrp_send_max_size);
    let mut jrp_send_size: usize = 0;
    let mut id: usize = 0;
    let mut frames: Vec<Bytes> = Vec::with_capacity(jrp_send_max_rec_count);
    let b2j = b2j(file_format, jrp_value_codec);
    while let Some(result) = http_framed.next().await {
        let frame = result?;
        jrp_send_size += frame.len();
        jrp_send_max_rec_size = jrp_send_max_rec_size.max(frame.len());
        frames.push(frame);
        if jrp_send_size > jrp_send_max_size - jrp_send_max_rec_size || frames.len() >= jrp_send_max_rec_count {
            metrics.size_by_value(&labels, jrp_send_size);
            let jrp_records = b2j_rec_vec(&frames, b2j)?;
            let kfk_records = jrp_records.into_iter()
                .map(|jrs| Record::try_from(jrs))
                .collect::<Result<Vec<Record>, JrpkError>>()?;
            let ctx = JrpCtxTypes::send(id, Instant::now(), kfk_tap.clone());
            let kfk_req = KfkReq::Send(Req(Ctx(ctx, kfk_records), kfk_rsp_snd.clone()));
            kfk_req_snd.send(kfk_req).await?;
            id = id + 1;
            jrp_send_size = 0;
            frames.clear();
        }
    }

    if !frames.is_empty() {
        metrics.size_by_value(&labels, jrp_send_size);
        let jrp_records = b2j_rec_vec(&frames, b2j)?;
        let kfk_records = jrp_records.into_iter()
            .map(|jrs| Record::try_from(jrs))
            .collect::<Result<Vec<Record>, JrpkError>>()?;
        let ctx = JrpCtxTypes::send(id, Instant::now(), kfk_tap.clone());
        let kfk_req = KfkReq::Send(Req(Ctx(ctx, kfk_records), kfk_rsp_snd));
        kfk_req_snd.send(kfk_req).await?;
    }

    Ok(())
}

#[instrument(level="info", err, skip(kfk_rsp_rcv, metrics, labels))]
async fn post_kafka_send_proc_responses(
    mut kfk_rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
    metrics: Arc<JrpkMetrics>,
    labels: JrpkLabels,
) -> Result<(), JrpkError> {
    let ts = Instant::now();
    while let Some(kfk_rsp) = kfk_rsp_rcv.recv().await {
        let Ctx(ctx, res) = kfk_rsp.send_or(JrpkError::Unexpected("kafka response wrong type"))?;
        let JrpCtx { id: _, ts: _, tap: _, .. } = ctx;
        let offsets = res?;
        metrics.size(&labels, &offsets);
    }
    metrics.time(&labels, ts);
    Ok(())
}

#[instrument(level="info", err, skip(kfk_clients, metrics))]
async fn post_kafka_send(
    State(
        HttpState {
            kfk_clients,
            metrics
        }
    ): State<HttpState>,
    Path(
        HttpKfkPath {
            kfk_topic,
            kfk_partition
        }
    ): Path<HttpKfkPath>,
    Query(
        HttpSendQuery {
            jrp_value_codec,
            jrp_send_max_size,
            jrp_send_max_rec_count,
            jrp_send_max_rec_size,
            file_format
        }
    ): Query<HttpSendQuery>,
    request: Request<Body>,
) -> Result<(), JrpkError> {
    let jrp_send_max_size = jrp_send_max_size.as_u64() as usize;
    let jrp_send_max_rec_size = jrp_send_max_rec_size.as_u64() as usize;
    let kfk_tap = KfkTap::new(kfk_topic, kfk_partition);
    let kfk_req_snd = kfk_clients.lookup_sender(kfk_tap.clone()).await?;
    let (kfk_rsp_snd, kfk_rsp_rcv) = tokio::sync::mpsc::channel::<KfkRsp<JrpCtxTypes>>(kfk_req_snd.max_capacity());
    let mut labels = JrpkLabels::new(LblTier::Http).method(LblMethod::Send).traffic(LblTraffic::In).tap(kfk_tap.clone()).build();
    let http_body = request.into_body();
    let http_stream = http_body.into_data_stream();
    let rsp_h = spawn(
        post_kafka_send_proc_responses(
            kfk_rsp_rcv,
            metrics.clone(),
            labels.clone()
        )
    );
    let labels = labels.traffic(LblTraffic::Out).build();
    let req_h = spawn(
        post_kafka_send_proc_requests(
            http_stream,
            jrp_value_codec,
            jrp_send_max_size,
            jrp_send_max_rec_count,
            jrp_send_max_rec_size,
            kfk_tap,
            kfk_req_snd,
            kfk_rsp_snd,
            file_format,
            metrics,
            labels
        )
    );
    let _ = try_join!(req_h, rsp_h)?;
    Ok(())
}

#[instrument(level="info", err, skip(kfk_clients, metrics))]
pub async fn listen_http(
    http_bind: SocketAddr,
    kfk_clients: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
    metrics: Arc<JrpkMetrics>,
) -> Result<(), JrpkError> {
    let state = HttpState::new(kfk_clients, metrics.clone());
    let listener = TcpListener::bind(http_bind).await?;
    let metrics = Router::new()
        .route("/", get(get_prometheus_metrics))
        .with_state(metrics);
    let kafka = Router::new()
        .route("/offset/{kfk_topic}/{kfk_partition}", get(get_kafka_offset))
        .route("/fetch/{kfk_topic}/{kfk_partition}", get(get_kafka_fetch))
        .route("/send/{kfk_topic}/{kfk_partition}", post(post_kafka_send))
        .with_state(state);
    let root = Router::new()
        .nest("/kafka", kafka)
        .nest("/metrics", metrics);
    axum::serve(listener, root).await?;
    Ok(())
}
