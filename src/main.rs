mod args;
mod server;
mod util;
mod jsonrpc;
mod kafka;
mod codec;
mod client;
mod metrics;
mod error;

use crate::server::listen;
use crate::util::{init_tracing, join_with_signal};
use clap::Parser;
use std::time::Duration;
use tokio;
use tokio::spawn;
use tracing::info;
use crate::args::{Command, Mode};
use crate::client::{consume, produce};

async fn run(args: args::Args) {
    match args.mode {
        Mode::Server {
            brokers,
            bind,
            max_frame_byte_size: max_frame_size,
            send_buffer_byte_size: send_buffer_size,
            recv_buffer_byte_size: recv_buffer_size,
            queue_len: queue_size,
            metrics_uri,
            metrics_period
        } => {
            join_with_signal(
                spawn(
                    listen(
                        brokers,
                        bind,
                        max_frame_size,
                        send_buffer_size,
                        recv_buffer_size,
                        queue_size
                    )
                )
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
                    join_with_signal(
                        spawn(
                            produce(
                                path,
                                address,
                                topic,
                                partition,
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
                    join_with_signal(
                        spawn(
                            consume(
                                path,
                                address,
                                topic,
                                partition,
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
