mod args;
mod server;
mod util;
mod jsonrpc;
mod kafka;
mod codec;
mod client;

use crate::server::listen;
use crate::util::{handle_future_result, init_tracing, join_with_signal};
use clap::Parser;
use std::time::Duration;
use tokio;
use tracing::info;
use crate::args::{Command, Mode};
use crate::client::{consume, produce};

async fn run(args: args::Args) {
    match args.mode {
        Mode::Server { brokers, bind, max_frame_size, send_buffer_size, recv_buffer_size, queue_size } => {
            join_with_signal(
                "main",
                bind,
                tokio::spawn(
                    handle_future_result(
                        "listen",
                        bind,
                        listen(brokers, bind, max_frame_size, send_buffer_size, recv_buffer_size, queue_size)
                    )
                )
            ).await
        }
        Mode::Client { address, topic, partition, path, max_frame_size, command } => {
            info!("client, address: {}, topic: {}, partition: {:?}, path: {:?}, command: {:?}", address, topic, partition, path, command);
            match command {
                Command::Produce { max_batch_rec_count, max_batch_byte_size, max_rec_byte_size } => {
                    join_with_signal(
                        "produce",
                        address.clone(),
                        tokio::spawn(
                            handle_future_result(
                                "produce",
                                address.clone(),
                                produce(
                                    path,
                                    address,
                                    topic,
                                    partition,
                                    max_frame_size.as_u64() as usize,
                                    max_batch_rec_count as usize,
                                    max_batch_byte_size.as_u64() as usize,
                                    max_rec_byte_size.as_u64() as usize,
                                )
                            )
                        )
                    ).await
                }
                Command::Consume { from, until, batch_size, max_wait_ms } => {
                    join_with_signal(
                        "consume",
                        address.clone(),
                        tokio::spawn(
                            handle_future_result(
                                "consume",
                                address.clone(),
                                consume(path, address, topic, partition, from, until, batch_size, max_wait_ms, max_frame_size)
                            )
                        )
                    ).await
                }
            }
        }
    }
}

fn main() {
    init_tracing();
    let args = args::Args::parse();
    info!("args: {:?}", args);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run(args));
    rt.shutdown_timeout(Duration::from_secs(1));
}
