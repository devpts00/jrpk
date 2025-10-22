use std::fmt::Display;
use bytesize::ByteSize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use thiserror::Error;
use ustr::Ustr;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    #[command(subcommand)]
    pub mode: Mode
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ParseError(String);

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
    type Err = ParseError;
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
            Err(ParseError(format!("invalid offset: {}", s)))
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
        queue_len: usize
    },
    Client {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        address: Ustr,
        #[arg(long)]
        topic: Ustr,
        #[arg(long, required = false)]
        partition: i32,
        #[arg(long, default_value = "1MiB")]
        max_frame_byte_size: ByteSize,
        #[command(subcommand)]
        command: Command,
    }
}

