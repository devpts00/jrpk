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
mod serve;

use crate::args::{Client, Cmd};
use crate::consume::{consume};
use crate::produce::{produce, Load};
use crate::util::{init_tracing, log, run, Tap};
use clap::Parser;
use std::time::Duration;
use tracing::info;
use crate::model::JrpSelector;
use crate::serve::serve;

fn main() {

    let _guard = init_tracing();
    let cmd = args::Cmd::parse();
    info!("cmd: {:?}", cmd);

    match cmd {
        Cmd::Serve {
            brokers,
            jsonrpc_bind,
            http_bind,
            max_frame_byte_size,
            send_buffer_byte_size,
            recv_buffer_byte_size,
            queue_len,
            thread_count,
        } => {
            log(run(
                thread_count,
                serve(
                    brokers,
                    jsonrpc_bind,
                    http_bind,
                    max_frame_byte_size,
                    send_buffer_byte_size,
                    recv_buffer_byte_size,
                    queue_len
                )
            ));
        },
        Cmd::Produce {
            client: Client {
                path,
                address,
                topic,
                partition,
                max_frame_byte_size,
                thread_count,
                metrics_url,
                metrics_period,
                file_format,
                value_codec,
            },
            max_batch_rec_count,
            max_batch_byte_size,
            max_rec_byte_size,
        } => {
            log(run(
                thread_count,
                produce(
                    address,
                    Tap::new(topic, partition),
                    path,
                    max_frame_byte_size.as_u64() as usize,
                    max_batch_rec_count as usize,
                    max_batch_byte_size.as_u64() as usize,
                    max_rec_byte_size.as_u64() as usize,
                    Load::new(file_format, value_codec),
                    metrics_url,
                    Duration::from(&metrics_period),
                )
            ));
        },
        Cmd::Consume {
            client: Client {
                path,
                address,
                topic,
                partition,
                max_frame_byte_size,
                thread_count,
                metrics_url,
                metrics_period,
                file_format,
                value_codec,
            },
            from,
            until,
            max_batch_byte_size,
            max_wait_ms,
            key_codec,
            header_codecs,
            header_codec_default,
        } => {
            log(run(
                thread_count,
                consume(
                    address,
                    Tap::new(topic, partition),
                    path,
                    from,
                    until,
                    file_format,
                    JrpSelector::new(key_codec, value_codec, header_codecs.into_iter().map(|nc|nc.into()).collect(), header_codec_default),
                    max_batch_byte_size.as_u64() as i32,
                    max_wait_ms,
                    max_frame_byte_size.as_u64() as usize,
                    metrics_url,
                    Duration::from(&metrics_period),
                )
            ))
        }
    }

}
