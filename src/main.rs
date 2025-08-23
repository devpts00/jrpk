mod args;
mod server;
mod util;
mod jsonrpc;
mod kafka;
mod codec;
mod errors;
mod tests;

use crate::server::listen;
use crate::util::{handle_future, join_with_signal};
use clap::Parser;
use std::sync::Once;
use std::time::Duration;
use tokio;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

const QUEUE_SIZE: usize = 8;
const MAX_FRAME_SIZE: usize = 1024 * 1024;
const RECV_BUFFER_SIZE: usize = 8 * 1024;
const SEND_BUFFER_SIZE: usize = 8 * 1024;

async fn run(args: args::Args) {
    join_with_signal("main", tokio::spawn(handle_future("listen", listen(args.bind, args.brokers.0)))).await
}

static TRACING: Once = Once::new();

fn init_tracing() {
    TRACING.call_once(|| {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer()
                .pretty()
                .with_file(false)
                .with_line_number(false)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_span_events(FmtSpan::NONE)
            )
            .with(EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()
                .unwrap()
            )
            .init();
    })
}

fn main() {
    init_tracing();
    let args = args::Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run(args));
    rt.shutdown_timeout(Duration::from_secs(1));
}
