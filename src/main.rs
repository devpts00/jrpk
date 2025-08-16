mod args;
mod server;
mod client;
mod util;
mod jsonrpc;
mod kafka;
mod codec;

use std::sync::Once;
use std::time::Duration;
use tokio;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use clap::Parser;
use tracing_subscriber::fmt::format::FmtSpan;
use crate::args::Cmd;
use crate::client::send_text;
use crate::server::listen;
use crate::util::{handle_future, join_with_signal};


async fn run(args: args::Args) {
    join_with_signal(
        match args.cmd {
            Cmd::Server { bind, brokers} => {
                tokio::spawn(
                    handle_future(
                        listen(bind, brokers.0)
                    )
                )
            }
            Cmd::Client { file, target} => {
                tokio::spawn(
                    handle_future(
                        send_text(target)
                    )
                )
            }
        }
    ).await
}

static TRACING: Once = Once::new();

fn init_tracing() {
    TRACING.call_once(|| {
        tracing_subscriber::registry()
            .with(console_subscriber::spawn())
            .with(tracing_subscriber::fmt::layer()
                .pretty()
                .with_file(false)
                .with_line_number(false)
                .with_thread_ids(false)
                .with_thread_names(false)
                .with_span_events(FmtSpan::NONE)
            )
            .with(EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env().unwrap()
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
