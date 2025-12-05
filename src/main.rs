mod args;
mod server;
mod util;
mod jsonrpc;
mod kafka;
mod codec;
mod produce;
mod metrics;
mod error;
mod consume;

use std::sync::{Arc, Mutex};
use crate::server::listen_jsonrpc;
use crate::util::{init_tracing, join_with_signal, Tap};
use clap::Parser;
use std::time::Duration;
use futures::future::join_all;
use prometheus_client::registry::Registry;
use tokio;
use tokio::spawn;
use tracing::info;
use crate::args::{Command, Mode};
use crate::consume::consume;
use crate::metrics::listen_prometheus_metrics;
use crate::produce::produce;

async fn run(args: args::Args) {

    let registry = Arc::new(Mutex::new(Registry::default()));
    match args.mode {
        Mode::Server {
            brokers,
            bind,
            max_frame_byte_size,
            send_buffer_byte_size,
            recv_buffer_byte_size,
            queue_len: queue_size,
            metrics_bind,
        } => {

            let jh = spawn(
                listen_jsonrpc(
                    brokers,
                    bind,
                    max_frame_byte_size.as_u64() as usize,
                    send_buffer_byte_size.as_u64() as usize,
                    recv_buffer_byte_size.as_u64() as usize,
                    queue_size,
                    registry.clone(),
                )
            );

            let ph = spawn(
                listen_prometheus_metrics(metrics_bind, registry)
            );

            join_with_signal(
                join_all(vec!(jh, ph))
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
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run(args));
    rt.shutdown_timeout(Duration::from_secs(1));
}
