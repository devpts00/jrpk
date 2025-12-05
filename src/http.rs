use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use axum::extract::State;
use axum::Router;
use axum::routing::get;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tracing::instrument;
use crate::error::JrpkError;
use crate::metrics::encode_registry;

#[instrument(level="debug", ret, err, skip(registry))]
async fn get_prometheus_metrics(State(registry): State<Arc<Mutex<Registry>>>) -> Result<String, JrpkError> {
    let text = encode_registry(registry)?;
    Ok(text)
}

#[instrument(ret, err, skip(registry))]
pub async fn listen_http(addr: SocketAddr, registry: Arc<Mutex<Registry>>) -> Result<(), JrpkError> {
    let listener = TcpListener::bind(addr).await?;
    let router = Router::new()
        .route("/metrics", get(get_prometheus_metrics))
        .with_state(registry);
    axum::serve(listener, router).await?;
    Ok(())
}
