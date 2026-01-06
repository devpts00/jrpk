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
use crate::args::{Command, Format, Mode};
use crate::consume::consume;
use crate::http::listen_http;
use crate::jsonrpc::{listen_jsonrpc, JrpCtxTypes};
use crate::kafka::{KfkClientCache, KfkReq};
use crate::metrics::JrpkMetrics;
use crate::produce::produce;
use crate::util::{init_tracing, join_with_quit, join_with_signal, Tap};
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
use tracing::{info, instrument};

#[instrument]
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

    join_with_quit(
        join_all(vec!(jh, hh))
    ).await
}

#[instrument(skip(metrics_url))]
async fn client(
    path: FastStr,
    address: FastStr,
    topic: FastStr,
    partition: i32,
    max_frame_byte_size: ByteSize,
    metrics_url: Url,
    metrics_period: DurationHuman,
    format: Format,
    command: Command,
) {
    let metrics = Arc::new(JrpkMetrics::new());
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
                        metrics_url,
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
                        metrics_url,
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
            format,
            value_codec,
            key_codec,
            header_codecs,
            command
        } => {

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
                    format,
                    command
                )
            );

            rt.shutdown_timeout(Duration::from_secs(1));


        }
    }

}
