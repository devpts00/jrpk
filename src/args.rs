use std::fmt::Display;
use bytesize::ByteSize;
use std::net::SocketAddr;
use std::str::FromStr;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use clap_duration::duration_range_value_parse;
use duration_human::{DurationHuman, DurationHumanValidator};
use hyper::Uri;
use ustr::Ustr;
use crate::error::JrpkError;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    #[command(subcommand)]
    pub mode: Mode,
}

#[derive(Debug, Clone, Copy)]
pub enum Offset {
    Earliest,
    Latest,
    Timestamp(DateTime<Utc>),
    Offset(i64)
}

impl Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Offset::Earliest => f.write_str("earliest"),
            Offset::Latest => f.write_str("latest"),
            Offset::Timestamp(ts) => ts.fmt(f),
            Offset::Offset(pos) => pos.fmt(f),
        }
    }
}

impl FromStr for Offset {
    type Err = JrpkError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("earliest") {
            Ok(Offset::Earliest)
        } else if s.eq_ignore_ascii_case("latest") {
            Ok(Offset::Latest)
        } else if let Ok(ts) = DateTime::<Utc>::from_str(s) {
            Ok(Offset::Timestamp(ts))
        } else if let Ok(offset) = i64::from_str(s) {
            Ok(Offset::Offset(offset))
        } else {
            Err(JrpkError::Parse(format!("invalid offset: {}", s)))
        }
    }
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    Produce {
        #[arg(long, default_value = "1024")]
        max_batch_rec_count: u16,
        #[arg(long, default_value = "64KiB")]
        max_batch_byte_size: ByteSize,
        #[arg(long, default_value = "16KiB")]
        max_rec_byte_size: ByteSize,
    },
    Consume {
        #[arg(long, default_value = "earliest", value_parser = clap::value_parser!(Offset))]
        from: Offset,
        #[arg(long, default_value = "latest", value_parser = clap::value_parser!(Offset))]
        until: Offset,
        #[arg(long, default_value = "64KiB")]
        max_batch_byte_size: ByteSize,
        #[arg(long, default_value = "100")]
        max_wait_ms: i32,
    }
}

#[derive(Debug, Clone, Subcommand)]
pub enum Mode {
    Server {
        #[arg(long, value_delimiter = ',')]
        brokers: Vec<String>,
        #[arg(long)]
        bind: SocketAddr,
        #[arg(long, default_value = "1MiB")]
        max_frame_byte_size: ByteSize,
        #[arg(long, default_value = "32KiB")]
        send_buffer_byte_size: ByteSize,
        #[arg(long, default_value = "32KiB")]
        recv_buffer_byte_size: ByteSize,
        #[arg(long, default_value_t = 32)]
        queue_len: usize,
        #[arg(long)]
        metrics_uri: Uri,
        #[arg(long, default_value = "10s", value_parser = duration_range_value_parse!(min: 1s, max: 1min))]
        metrics_period: DurationHuman,
    },
    Client {
        #[arg(long)]
        path: Ustr,
        #[arg(long)]
        address: Ustr,
        #[arg(long)]
        topic: Ustr,
        #[arg(long, required = false)]
        partition: i32,
        #[arg(long, default_value = "1MiB")]
        max_frame_byte_size: ByteSize,
        #[arg(long)]
        metrics_uri: Uri,
        #[arg(long, default_value = "10s", value_parser = duration_range_value_parse!(min: 1s, max: 1min))]
        metrics_period: DurationHuman,
        #[command(subcommand)]
        command: Command,
    }
}

