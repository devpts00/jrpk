use crate::error::JrpkError;
use crate::jsonrpc::JrpCtxTypes;
use crate::kafka::{KfkClientCache, KfkOffset, KfkReq, KfkRsp};
use crate::metrics::encode_registry;
use crate::model::{JrpCodecs, JrpOffset};
use crate::size::Size;
use crate::util::{Ctx, Request, Tap};
use axum::extract::{Path, Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::Response;
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
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::instrument;

#[instrument(level="trace", ret, err, skip(registry))]
async fn get_prometheus_metrics(State(registry): State<Arc<Mutex<Registry>>>) -> Result<String, JrpkError> {
    let text = encode_registry(registry)?;
    Ok(text)
}

async fn get_kafka_offset(
    State(cache): State<Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>>,
    Path((topic, partition, offset)): Path<(FastStr, i32, JrpOffset)>,
) -> Result<String, JrpkError> {
    let tap = Tap::new(topic, partition);
    let ctx = JrpCtxTypes::offset(0, tap.clone());
    let kfk_snd = cache.lookup_sender(tap).await?;
    let kfk_offset = offset.into();
    let (kfk_rsp_snd, mut kfk_rsp_rcv) = tokio::sync::mpsc::channel(1);
    let kfk_req = KfkReq::Offset(Request(Ctx(ctx, kfk_offset), kfk_rsp_snd));
    kfk_snd.send(kfk_req).await?;
    let kfk_rsp = kfk_rsp_rcv.recv().await.ok_or(JrpkError::Unexpected("kafka client does not respond"))?;
    let Ctx(_, kfk_offset_res) = kfk_rsp.offset_or(JrpkError::Unexpected("kafka client wrong response"))?;
    let offset = kfk_offset_res?;
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
        tap: Tap,
        codecs: JrpCodecs,
        from: KfkOffset,
        until: KfkOffset,
        min_max_bytes: Range<i32>,
        max_wait_ms: i32,
        req_snd: Sender<KfkReq<JrpCtxTypes>>,
        rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
        rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
    },
    Done,
}

impl KfkFetchState {

    fn next(
        id: usize,
        tap: Tap,
        codecs: JrpCodecs,
        from: KfkOffset,
        until: KfkOffset,
        min_max_bytes: Range<i32>,
        max_wait_ms: i32,
        req_snd: Sender<KfkReq<JrpCtxTypes>>,
        rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
        rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
    ) -> Self {
        KfkFetchState::Next { id, tap, codecs, from, until, min_max_bytes, max_wait_ms, req_snd, rsp_snd, rsp_rcv }
    }

}

async fn get_kafka_fetch_chunk(state: KfkFetchState) -> Result<Option<(Frame<Bytes>, KfkFetchState)>, JrpkError> {
    match state {
        KfkFetchState::Next { id, tap, codecs, from, until, min_max_bytes, max_wait_ms, req_snd, rsp_snd, mut rsp_rcv } => {
            let ctx = JrpCtxTypes::fetch(id, tap.clone(), codecs);
            let kfk_req = KfkReq::Fetch(Request(Ctx(ctx, (from, min_max_bytes.clone(), max_wait_ms)), rsp_snd.clone()));
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
            let frame = Frame::data(bytes);
            let offset = KfkOffset::Pos(pos + 1);
            if !done {
                let next = KfkFetchState::next(id + 1, tap, codecs, offset, until, min_max_bytes, max_wait_ms, req_snd, rsp_snd, rsp_rcv);
                Ok(Some((frame, next)))
            } else {
                Ok(Some((frame, KfkFetchState::Done)))
            }
        },
        KfkFetchState::Done => {
            Ok(None)
        }
    }
}

#[instrument(level="debug", err, skip(cache))]
async fn get_kafka_fetch(
    State(cache): State<Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>>,
    Path(HttpFetchPath { topic, partition }): Path<HttpFetchPath>,
    Query(HttpFetchQuery { from, until, min_size, max_size, max_wait }): Query<HttpFetchQuery>,
) -> Result<impl IntoResponse, JrpkError> {
    let tap = Tap::new(topic, partition);
    let req_snd = cache.lookup_sender(tap.clone()).await?;
    let from = from.into();
    let until = until.into();
    let bytes = min_size.as_u64() as i32 .. max_size.as_u64() as i32;
    let max_wait_ms = max_wait.as_millis() as i32;
    let (rsp_snd, rsp_rcv) = tokio::sync::mpsc::channel(1);
    let state = KfkFetchState::next(0, tap, JrpCodecs::default(), from, until, bytes, max_wait_ms, req_snd, rsp_snd, rsp_rcv);
    let stream = try_unfold(state, get_kafka_fetch_chunk);
    let body = StreamBody::new(stream);
    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "application/json-lines")
        .body(body)?;
    Ok(response)
}

/*
#[instrument(level = "debug", err, skip(cache))]
async fn post_kafka_send(
    State(cache): State<Arc<KfkClientCache>>,
    Path(HttpFetchPath { topic, partition, offset }): Path<HttpFetchPath>,
    request: Request,
) -> Result<(), JrpkError> {

    let tap = Tap::new(topic, partition);
    let http_snd = cache.lookup_http_sender(tap).await?;
    let mut rec_size_max: usize = 0;
    let mut batch_size: usize = 0;
    let batch_size_max: usize = ByteSize::mib(1).as_u64() as usize;
    let mut records: Vec<Record> = Vec::with_capacity(1024);

    let body = request.into_body();
    let stream = body.into_data_stream();
    let stream = stream.map_err(std::io::Error::other);
    let reader = StreamReader::new(stream);
    let mut lines = LinesStream::new(reader.lines());

    while let Some(line) = lines.try_next().await? {
        if batch_size > batch_size_max - rec_size_max {
            let req = KfkIn::send(records);
            


            records = Vec::with_capacity(rec_size_max);
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
    let listener = TcpListener::bind(addr).await?;
    let metrics = Router::new()
        .route("/", get(get_prometheus_metrics))
        .with_state(prometheus_registry);
    let kafka = Router::new()
        .route("/offset/{topic}/{partition}/{offset}", get(get_kafka_offset))
        .route("/fetch/{topic}/{partition}/{offset}", get(get_kafka_fetch))
        //.route("/send/{topic}/{partition}", post(post_kafka_send))
        .with_state(kafka_clients);
    let root = Router::new()
        .nest("/kafka", kafka)
        .nest("/metrics", metrics);
    axum::serve(listener, root).await?;
    Ok(())
}
