use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use axum::body::{Body, BodyDataStream};
use axum::extract::{Path, Query, Request, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::Response;
use axum::response::IntoResponse;
use axum::Router;
use axum::routing::{get, post};
use axum_extra::response::JsonLines;
use bytes::Bytes;
use bytesize::ByteSize;
use faststr::FastStr;
use prometheus_client::registry::Registry;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_util::io::StreamReader;
use tokio_stream::wrappers::LinesStream;
use futures_util::stream::{BoxStream, Stream, TryStream, TryStreamExt};
use rskafka::record::Record;
use tracing::{info, instrument};
use tokio::io::AsyncBufReadExt;
use crate::error::JrpkError;
use crate::jsonrpc::j2k_offset;
use crate::kafka::{KfkClientCache, KfkIn};
use crate::metrics::encode_registry;
use crate::model::JrpOffset;
use crate::util::{Req, Tap};

#[instrument(level="trace", ret, err, skip(registry))]
async fn get_prometheus_metrics(State(registry): State<Arc<Mutex<Registry>>>) -> Result<String, JrpkError> {
    let text = encode_registry(registry)?;
    Ok(text)
}

async fn get_kafka_offset(
    State(cache): State<Arc<KfkClientCache>>,
    Path((topic, partition, offset)): Path<(FastStr, i32, JrpOffset)>,
) -> Result<String, JrpkError> {
    let tap = Tap::new(topic, partition);
    let http_snd = cache.lookup_http_sender(tap).await?;
    let kfk_req = KfkIn::offset(j2k_offset(offset));
    let (res_rsp_snd, res_rsp_rcv) = tokio::sync::oneshot::channel();
    let req = Req::new(kfk_req, res_rsp_snd);
    http_snd.send(req).await?;
    let kfk_rsp = res_rsp_rcv.await??;
    let pos = kfk_rsp.to_offset()?;
    Ok(pos.to_string())
}

#[inline]
fn add_new_line(b: Bytes) -> impl Iterator<Item = Bytes> {
    [b, Bytes::from_static(b"\n")].into_iter()
}

#[derive(Deserialize)]
struct HttpFetchPath {
    topic: FastStr,
    partition: i32,
    offset: JrpOffset,
}

#[derive(Deserialize)]
struct HttpFetchQuery {
    min_size: ByteSize,
    max_size: ByteSize,
    #[serde(with = "humantime_serde")]
    max_wait: Duration,
}

#[instrument(level="debug", err, skip(cache))]
async fn get_kafka_fetch(
    State(cache): State<Arc<KfkClientCache>>,
    Path(HttpFetchPath { topic, partition, offset }): Path<HttpFetchPath>,
    Query(HttpFetchQuery { min_size, max_size, max_wait }): Query<HttpFetchQuery>,
) -> Result<impl IntoResponse, JrpkError> {
    let tap = Tap::new(topic, partition);
    let http_snd = cache.lookup_http_sender(tap).await?;
    let kfk_req = KfkIn::fetch(
        j2k_offset(offset),
        min_size.as_u64() as i32..max_size.as_u64() as i32,
        max_wait.as_millis() as i32,
    );
    let (res_rsp_snd, res_rsp_rcv) = tokio::sync::oneshot::channel();
    let req = Req::new(kfk_req, res_rsp_snd);
    http_snd.send(req).await?;
    let kfk_rsp = res_rsp_rcv.await??;
    let (recs_and_offsets, high_watermark) = kfk_rsp.to_fetch()?;
    // TODO: take codecs into account
    let values = recs_and_offsets.into_iter()
        .filter_map(|ro| ro.record.value)
        .map(|v| Bytes::from(v))
        .flat_map(|b|add_new_line(b))
        .map(|v| Result::<Bytes, Infallible>::Ok(Bytes::from(v)));
    let stream = futures::stream::iter(values);
    let body = Body::from_stream(stream);
    let rsp = Response::builder()
        .header(CONTENT_TYPE, "application/jsonl")
        .header("high_watermark", high_watermark)
        .body(body)?;
    Ok(rsp)
}

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

#[instrument(ret, err, skip(kafka_clients, prometheus_registry))]
pub async fn listen_http(
    addr: SocketAddr,
    kafka_clients: Arc<KfkClientCache>,
    prometheus_registry: Arc<Mutex<Registry>>
) -> Result<(), JrpkError> {
    let listener = TcpListener::bind(addr).await?;
    let metrics = Router::new()
        .route("/", get(get_prometheus_metrics))
        .with_state(prometheus_registry);
    let kafka = Router::new()
        .route("/offset/{topic}/{partition}/{offset}", get(get_kafka_offset))
        .route("/fetch/{topic}/{partition}/{offset}", get(get_kafka_fetch))
        .route("/send/{topic}/{partition}", post(post_kafka_send))
        .with_state(kafka_clients);
    let root = Router::new()
        .nest("/kafka", kafka)
        .nest("/metrics", metrics);
    axum::serve(listener, root).await?;
    Ok(())
}
