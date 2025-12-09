use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::Response;
use axum::response::IntoResponse;
use axum::Router;
use axum::routing::get;
use bytes::Bytes;
use faststr::FastStr;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tracing::instrument;
use crate::error::JrpkError;
use crate::jsonrpc::j2k_offset;
use crate::kafka::{KfkClientCache, KfkReq};
use crate::metrics::encode_registry;
use crate::model::JrpOffset;
use crate::util::{Req, Tap};

#[instrument(level="debug", ret, err, skip(registry))]
async fn get_prometheus_metrics(State(registry): State<Arc<Mutex<Registry>>>) -> Result<String, JrpkError> {
    let text = encode_registry(registry)?;
    Ok(text)
}

async fn get_kafka_offset(
    State(cache): State<Arc<KfkClientCache>>,
    Path((topic, partition, offset)): Path<(FastStr, i32, JrpOffset)>,
) -> Result<String, JrpkError> {
    let tap = Tap::new(topic, partition);
    let (_, http_snd) = cache.lookup_kafka_senders(tap).await?;
    let kfk_req = KfkReq::offset(j2k_offset(offset));
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

async fn get_kafka_fetch(
    State(cache): State<Arc<KfkClientCache>>,
    Path((topic, partition, offset)): Path<(FastStr, i32, JrpOffset)>,
) -> Result<impl IntoResponse, JrpkError> {
    let tap = Tap::new(topic, partition);
    let (_, http_snd) = cache.lookup_kafka_senders(tap).await?;
    let kfk_req = KfkReq::fetch(j2k_offset(offset), 0..1024*1024, 100);
    let (res_rsp_snd, res_rsp_rcv) = tokio::sync::oneshot::channel();
    let req = Req::new(kfk_req, res_rsp_snd);
    http_snd.send(req).await?;
    let kfk_rsp = res_rsp_rcv.await??;
    let (recs_and_offsets, high_watermark) = kfk_rsp.to_fetch()?;
    let values = recs_and_offsets.into_iter()
        .filter_map(|ro| ro.record.value)
        .map(|v| Bytes::from(v))
        .flat_map(|b|add_new_line(b))
        .map(|v| Result::<Bytes, Infallible>::Ok(Bytes::from(v)));
    let stream = futures::stream::iter(values);
    let body = Body::from_stream(stream);
    let rsp = Response::builder()
        .header(CONTENT_TYPE, "text/plain")
        .header("high_watermark", high_watermark)
        .body(body)?;
    Ok(rsp)
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
        .with_state(kafka_clients);
    let root = Router::new()
        .nest("/kafka", kafka)
        .nest("/metrics", metrics);
    axum::serve(listener, root).await?;
    Ok(())
}
