use std::net::SocketAddr;
use std::sync::Arc;
use futures_util::future::join_all;
use rskafka::client::ClientBuilder;
use tokio::spawn;
use tracing::{info, instrument};
use crate::error::JrpkError;
use crate::http::listen_http;
use crate::jsonrpc::listen_jsonrpc;
use crate::kafka::KfkClientCache;
use crate::metrics::JrpkMetrics;
use crate::util::join_with_quit;

#[instrument]
pub async fn serve(
    jrp_bind: SocketAddr,
    jrp_max_frame_size: usize,
    jrp_queue_len: usize,
    http_bind: SocketAddr,
    kfk_brokers: Vec<String>,
    tcp_send_buffer_size: usize,
    tcp_recv_buffer_size: usize,
) -> Result<(), JrpkError> {

    let metrics = Arc::new(JrpkMetrics::new());

    info!("connect: {}", kfk_brokers.join(","));
    let kfk_client = ClientBuilder::new(kfk_brokers).build().await?;
    let kfk_clients = Arc::new(KfkClientCache::new(kfk_client, 1024, jrp_queue_len, metrics.clone()));

    let jh = spawn(
        listen_jsonrpc(
            jrp_bind,
            jrp_max_frame_size,
            jrp_queue_len,
            kfk_clients.clone(),
            tcp_send_buffer_size,
            tcp_recv_buffer_size,
            metrics.clone(),
        )
    );

    let hh = spawn(
        listen_http(
            http_bind,
            kfk_clients,
            metrics
        )
    );

    join_with_quit(join_all(vec!(jh, hh))).await;
    Ok(())
}
