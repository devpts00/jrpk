use std::convert::Infallible;
use std::fmt::Display;
use bytesize::ByteSize;
use std::net::SocketAddr;
use std::str::FromStr;
use chrono::{DateTime, Utc};
use clap::{value_parser, Parser, Subcommand};
use clap_duration::duration_range_value_parse;
use duration_human::{DurationHuman, DurationHumanValidator};
use faststr::FastStr;
use reqwest::Url;
use crate::error::JrpkError;
use crate::model::JrpCodec;

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

#[derive(Debug, Clone)]
pub struct NamedCodec(FastStr, JrpCodec);

impl FromStr for NamedCodec {
    type Err = JrpkError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (name, codec) = s.split_once(':')
            .ok_or(JrpkError::Parse(format!("invalid named codec: {}", s)))?;
        let name = FastStr::new(name);
        let codec = JrpCodec::from_str(codec)?;
        Ok(NamedCodec(name, codec))
    }
}

impl From<NamedCodec> for (FastStr, JrpCodec) {
    fn from(value: NamedCodec) -> Self {
        let NamedCodec(name, codec) = value;
        (name, codec)
    }
}

#[derive(Debug, Clone)]
pub enum Header {
    All,
    Names(Vec<FastStr>)
}

impl FromStr for Header {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "*" {
            Ok(Header::All)
        } else {
            let names = s.split(',')
                .map(|n| n.trim())
                .filter_map(|n| if n.is_empty() { None } else { Some(FastStr::new(n)) })
                .collect();
            Ok(Header::Names(names))
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
        #[command(subcommand)]
        load: Load
    },
    Consume {
        #[arg(long, default_value = "earliest", value_parser = value_parser!(Offset))]
        from: Offset,
        #[arg(long, default_value = "latest", value_parser = value_parser!(Offset))]
        until: Offset,
        #[arg(long, default_value = "64KiB")]
        max_batch_byte_size: ByteSize,
        #[arg(long, default_value = "100")]
        max_wait_ms: i32,
        #[command(subcommand)]
        save: Save
    }
}

#[derive(Debug, Clone, Subcommand)]
pub enum Load {
    Value {
        #[arg(long)]
        codec: JrpCodec,
    },
    Record
}

#[derive(Debug, Clone, Subcommand)]
pub enum Save {
    Value {
        #[arg(long)]
        codec: JrpCodec,
    },
    Record {
        #[arg(long)]
        key: Option<JrpCodec>,
        #[arg(long)]
        value: JrpCodec,
        #[arg(long, value_delimiter = ',')]
        header: Vec<NamedCodec>,
        #[arg(long)]
        header_default: Option<JrpCodec>,
    }
}

#[derive(Debug, Clone, Subcommand)]
pub enum Mode {
    Server {
        #[arg(long, value_delimiter = ',')]
        brokers: Vec<String>,
        #[arg(long, default_value = "1133")]
        jsonrpc_bind: SocketAddr,
        #[arg(long, default_value = "1134")]
        http_bind: SocketAddr,
        #[arg(long, default_value = "1MiB")]
        max_frame_byte_size: ByteSize,
        #[arg(long, default_value = "32KiB")]
        send_buffer_byte_size: ByteSize,
        #[arg(long, default_value = "32KiB")]
        recv_buffer_byte_size: ByteSize,
        #[arg(long, default_value_t = 32)]
        queue_len: usize,
    },
    Client {
        #[arg(long)]
        path: FastStr,
        #[arg(long)]
        address: FastStr,
        #[arg(long)]
        topic: FastStr,
        #[arg(long, required = false)]
        partition: i32,
        #[arg(long, default_value = "1MiB")]
        max_frame_byte_size: ByteSize,
        #[arg(long)]
        metrics_uri: Url,
        #[arg(long, default_value = "10s", value_parser = duration_range_value_parse!(min: 1s, max: 1min))]
        metrics_period: DurationHuman,
        #[command(subcommand)]
        command: Command,
    }
}

