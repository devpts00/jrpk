mod args;
mod jsonrpc;
mod util;
mod model;
mod kafka;
mod codec;
mod produce;
mod metrics;
mod error;
mod consume;
mod http;
mod size;

use std::net::SocketAddr;
use crate::args::{Command, Mode};
use crate::consume::consume;
use crate::http::listen_http;
use crate::jsonrpc::{listen_jsonrpc, JrpCtxTypes};
use crate::kafka::{KfkClientCache, KfkReq};
use crate::metrics::JrpkMetrics;
use crate::produce::produce;
use crate::util::{init_tracing, join_with_signal, Tap};
use clap::Parser;
use futures::future::join_all;
use rskafka::client::ClientBuilder;
use std::sync::Arc;
use std::time::Duration;
use bytesize::ByteSize;
use duration_human::DurationHuman;
use faststr::FastStr;
use reqwest::Url;
use tokio;
use tokio::spawn;
use tracing::info;

async fn server(
    brokers: Vec<String>,
    jsonrpc_bind: SocketAddr,
    http_bind: SocketAddr,
    max_frame_byte_size: ByteSize,
    send_buffer_byte_size: ByteSize,
    recv_buffer_byte_size: ByteSize,
    queue_len: usize,
) {
    info!("connect: {}", brokers.join(","));
    // TODO: handle error
    let kafka_client = ClientBuilder::new(brokers).build().await.unwrap();
    let metrics = JrpkMetrics::new();
    let kafka_clients: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>> = Arc::new(KfkClientCache::new(kafka_client, 1024, queue_len, metrics.clone()));

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

    join_with_signal(
        join_all(vec!(jh, hh))
    ).await
}

async fn client(
    path: FastStr,
    address: FastStr,
    topic: FastStr,
    partition: i32,
    max_frame_byte_size: ByteSize,
    metrics_uri: Url,
    metrics_period: DurationHuman,
    command: Command,

) {
    let metrics = JrpkMetrics::new();
    let tap = Tap::new(topic, partition);

    match command {
        Command::Produce { max_batch_rec_count, max_batch_byte_size, max_rec_byte_size} => {
            join_with_signal(
                spawn(
                    produce(
                        address,
                        tap,
                        path,
                        max_frame_byte_size.as_u64() as usize,
                        max_batch_rec_count as usize,
                        max_batch_byte_size.as_u64() as usize,
                        max_rec_byte_size.as_u64() as usize,
                        metrics,
                        metrics_uri,
                        Duration::from(&metrics_period),

                    )
                )
            ).await
        }
        Command::Consume { from, until, max_batch_byte_size, max_wait_ms} => {
            join_with_signal(
                spawn(
                    consume(
                        address,
                        tap,
                        path,
                        from,
                        until,
                        max_batch_byte_size.as_u64() as i32,
                        max_wait_ms,
                        max_frame_byte_size.as_u64() as usize,
                        metrics,
                        metrics_uri,
                        Duration::from(&metrics_period),
                    )
                )
            ).await
        }
    }
}


fn main() {

    let _guard = init_tracing();
    let args = args::Args::parse();
    info!("args: {:?}", args);

    match args.mode {
        Mode::Server {
            brokers,
            jsonrpc_bind,
            http_bind,
            max_frame_byte_size,
            send_buffer_byte_size,
            recv_buffer_byte_size,
            queue_len
        } => {

            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_time()
                .enable_io()
                .build()
                .unwrap();

            rt.block_on(
                server(
                    brokers,
                    jsonrpc_bind,
                    http_bind,
                    max_frame_byte_size,
                    send_buffer_byte_size,
                    recv_buffer_byte_size,
                    queue_len,
                )
            );

            rt.shutdown_timeout(Duration::from_secs(1));

        }
        Mode::Client {
            address,
            topic,
            partition,
            path,
            max_frame_byte_size,
            metrics_uri,
            metrics_period,
            command
        } => {
            info!("client, address: {}, topic: {}, partition: {:?}, path: {:?}, command: {:?}", address, topic, partition, path, command);

            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_time()
                .enable_io()
                .build()
                .unwrap();

            rt.block_on(
                client(
                    path,
                    address,
                    topic,
                    partition,
                    max_frame_byte_size,
                    metrics_uri,
                    metrics_period,
                    command
                )
            );

            rt.shutdown_timeout(Duration::from_secs(1));


        }
    }

}
