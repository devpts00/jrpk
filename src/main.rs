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

use crate::args::Cmd;
use crate::consume::consume;
use crate::produce::produce;
use crate::util::{init_tracing, log, run};
use clap::Parser;
use std::time::Duration;
use tracing::info;
use crate::serve::serve;

fn main() {

    let _guard = init_tracing();
    let cmd = args::Cmd::parse();
    info!("cmd: {:?}", cmd);

    match cmd {
        Cmd::Serve {
            jrp_bind,
            jrp_max_frame_size,
            jrp_queue_len,
            http_bind,
            kfk_brokers,
            kfk_compression,
            tcp_send_buf_size,
            tcp_recv_buf_size,
            thread_count,
        } => {
            let jrp_max_frame_size = jrp_max_frame_size.as_u64() as usize;
            let tcp_send_buf_size = tcp_send_buf_size.as_u64() as usize;
            let tcp_recv_buf_size = tcp_recv_buf_size.as_u64() as usize;
            log(run(
                thread_count,
                serve(
                    jrp_bind,
                    jrp_max_frame_size,
                    jrp_queue_len,
                    http_bind,
                    kfk_brokers,
                    kfk_compression,
                    tcp_send_buf_size,
                    tcp_recv_buf_size,
                )
            ));
        },
        Cmd::Produce {
            jrp_address,
            jrp_frame_max_size,
            jrp_send_max_size,
            jrp_send_max_rec_count,
            jrp_send_max_rec_size,
            jrp_value_codec,
            kfk_topic,
            kfk_partition,
            file_path,
            file_format,
            file_buf_size,
            file_load_max_size,
            file_load_max_rec_count,
            prom_push_url,
            prom_push_period,
            thread_count,
        } => {
            let jrp_frame_max_size = jrp_frame_max_size.as_u64() as usize;
            let jrp_send_max_size = jrp_send_max_size.as_u64() as usize;
            let jrp_send_max_rec_size = jrp_send_max_rec_size.as_u64() as usize;
            let file_buf_size = file_buf_size.as_u64() as usize;
            let file_load_max_rec_count = file_load_max_rec_count.unwrap_or(usize::MAX);
            let file_load_max_size = file_load_max_size.map(|bs|bs.as_u64() as usize).unwrap_or(usize::MAX);
            let prom_push_period = Duration::from(&prom_push_period);
            log(run(
                thread_count,
                produce(
                    jrp_address,
                    jrp_frame_max_size,
                    jrp_send_max_size,
                    jrp_send_max_rec_count,
                    jrp_send_max_rec_size,
                    jrp_value_codec,
                    kfk_topic,
                    kfk_partition,
                    file_path,
                    file_format,
                    file_buf_size,
                    file_load_max_rec_count,
                    file_load_max_size,
                    prom_push_url,
                    prom_push_period,
                )
            ));
        },
        Cmd::Consume {
            jrp_address,
            jrp_frame_max_size,
            jrp_key_codec,
            jrp_value_codec,
            jrp_header_codecs,
            jrp_header_codec_default,
            kfk_topic,
            kfk_partition,
            kfk_offset_from,
            kfk_offset_until,
            kfk_fetch_min_size,
            kfk_fetch_max_size,
            kfk_fetch_max_wait_time_ms,
            file_path,
            file_format,
            file_buf_size,
            file_save_max_rec_count,
            file_save_max_size,
            prom_push_url,
            prom_push_period,
            thread_count,
        } => {
            let jrp_frame_max_size = jrp_frame_max_size.as_u64() as usize;
            let jrp_header_codecs = jrp_header_codecs.into_iter().map(|nc|nc.into()).collect();
            let kfk_fetch_min_size = kfk_fetch_min_size.as_u64() as i32;
            let kfk_fetch_max_size = kfk_fetch_max_size.as_u64() as i32;
            let file_buf_size = file_buf_size.as_u64() as usize;
            let file_save_max_rec_count = file_save_max_rec_count.unwrap_or(usize::MAX);
            let file_save_max_size = file_save_max_size.map(|bs|bs.as_u64() as usize).unwrap_or(usize::MAX);
            let prom_push_period = Duration::from(&prom_push_period);
            log(run(
                thread_count,
                consume(
                    jrp_address,
                    jrp_frame_max_size,
                    kfk_offset_from,
                    kfk_offset_until,
                    jrp_key_codec,
                    jrp_value_codec,
                    jrp_header_codecs,
                    jrp_header_codec_default,
                    kfk_topic,
                    kfk_partition,
                    kfk_fetch_min_size,
                    kfk_fetch_max_size,
                    kfk_fetch_max_wait_time_ms,
                    file_path,
                    file_format,
                    file_buf_size,
                    file_save_max_rec_count,
                    file_save_max_size,
                    prom_push_url,
                    prom_push_period,
                )
            ))
        }
    }

}
