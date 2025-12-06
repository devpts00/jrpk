use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use axum::extract::State;
use axum::Router;
use axum::routing::get;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tracing::instrument;
use crate::error::JrpkError;
use crate::jsonrpc::SrvCtx;
use crate::kafka::KfkClientCache;
use crate::metrics::encode_registry;
use crate::model::JrpCodecs;

type JrpKfkClientCache = KfkClientCache<JrpCodecs, SrvCtx>;

#[instrument(level="debug", ret, err, skip(registry))]
async fn get_prometheus_metrics(State(registry): State<Arc<Mutex<Registry>>>) -> Result<String, JrpkError> {
    let text = encode_registry(registry)?;
    Ok(text)
}

async fn get_kafka_offset(State(cache): State<Arc<KfkClientCache<JrpCodecs, SrvCtx>>>) -> Result<String, JrpkError> {
    Ok("123".to_string())
}

#[instrument(ret, err, skip(kafka_clients, prometheus_registry))]
pub async fn listen_http(
    addr: SocketAddr,
    kafka_clients: Arc<JrpKfkClientCache>,
    prometheus_registry: Arc<Mutex<Registry>>
) -> Result<(), JrpkError> {
    let listener = TcpListener::bind(addr).await?;
    let metrics = Router::new()
        .route("/", get(get_prometheus_metrics))
        .with_state(prometheus_registry);
    let kafka = Router::new()
        .route("/offset", get(get_kafka_offset))
        .with_state(kafka_clients);
    let root = Router::new()
        .nest("/metrics", metrics)
        .nest("/kafka", kafka);
    axum::serve(listener, root).await?;
    Ok(())
}
