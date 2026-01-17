use std::net::SocketAddr;
use std::sync::Arc;
use bytesize::ByteSize;
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
    brokers: Vec<String>,
    jsonrpc_bind: SocketAddr,
    http_bind: SocketAddr,
    max_frame_byte_size: ByteSize,
    send_buffer_byte_size: ByteSize,
    recv_buffer_byte_size: ByteSize,
    queue_len: usize,
) -> Result<(), JrpkError> {

    info!("connect: {}", brokers.join(","));
    let kafka_client = ClientBuilder::new(brokers).build().await?;
    let metrics = Arc::new(JrpkMetrics::new());
    let kafka_clients = Arc::new(KfkClientCache::new(kafka_client, 1024, queue_len, metrics.clone()));

    let jh = spawn(
        listen_jsonrpc(
            jsonrpc_bind,
            max_frame_byte_size.as_u64() as usize,
            send_buffer_byte_size.as_u64() as usize,
            recv_buffer_byte_size.as_u64() as usize,
            queue_len,
            kafka_clients.clone(),
            metrics.clone(),
        )
    );

    let hh = spawn(
        listen_http(
            http_bind,
            kafka_clients,
            metrics
        )
    );

    join_with_quit(join_all(vec!(jh, hh))).await;
    Ok(())
}
