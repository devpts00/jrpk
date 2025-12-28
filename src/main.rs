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
use prometheus_client::registry::Registry;
use rskafka::client::ClientBuilder;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio;
use tokio::spawn;
use tracing::info;

async fn run(args: args::Args) {

    let registry = Arc::new(Mutex::new(Registry::default()));

    match args.mode {
        Mode::Server {
            brokers,
            jsonrpc_bind: bind,
            max_frame_byte_size,
            send_buffer_byte_size,
            recv_buffer_byte_size,
            queue_len: queue_size,
            http_bind: metrics_bind,
        } => {

            let metrics = JrpkMetrics::new(registry.clone());

            info!("connect: {}", brokers.join(","));
            // TODO: handle error
            let kafka_client = ClientBuilder::new(brokers).build().await.unwrap();
            let kafka_clients: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>> = Arc::new(KfkClientCache::new(kafka_client, 1024, queue_size, metrics.clone()));

            let jh = spawn(
                listen_jsonrpc(
                    bind,
                    max_frame_byte_size.as_u64() as usize,
                    send_buffer_byte_size.as_u64() as usize,
                    recv_buffer_byte_size.as_u64() as usize,
                    queue_size,
                    kafka_clients.clone(),
                    registry.clone(),
                )
            );

            let hh = spawn(
                listen_http(
                    metrics_bind,
                    kafka_clients,
                    registry
                )
            );

            join_with_signal(
                join_all(vec!(jh, hh))
            ).await
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
            match command {
                Command::Produce {
                    max_batch_rec_count,
                    max_batch_byte_size,
                    max_rec_byte_size
                } => {
                    let tap = Tap::new(topic, partition);
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
                                metrics_uri,
                                Duration::from(&metrics_period),
                            )
                        )
                    ).await
                }
                Command::Consume {
                    from,
                    until,
                    max_batch_byte_size,
                    max_wait_ms
                } => {
                    let tap = Tap::new(topic, partition);
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
                                metrics_uri,
                                Duration::from(&metrics_period),
                            )
                        )
                    ).await
                }
            }
        }
    }
}

fn main() {
    let _guard = init_tracing();
    let args = args::Args::parse();
    info!("args: {:?}", args);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .unwrap();
    rt.block_on(run(args));
    rt.shutdown_timeout(Duration::from_secs(1));
}
