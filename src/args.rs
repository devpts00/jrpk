use std::fmt::{Display, Formatter};
use bytesize::ByteSize;
use std::net::SocketAddr;
use std::str::FromStr;
use chrono::{DateTime, Utc};
use clap::{value_parser, Arg, Args, Parser};
use clap_duration::duration_range_value_parse;
use duration_human::{DurationHuman, DurationHumanValidator};
use faststr::FastStr;
use reqwest::Url;
use serde::{Deserialize, Deserializer};
use serde::de::{Error, Visitor};
use strum::EnumString;
use crate::error::JrpkError;
use crate::model::{JrpCodec, JrpOffset};

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

#[derive(Debug, Clone, Copy, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum Format {
    Value,
    Record
}

#[derive(Debug, Clone, Parser)]
pub enum Cmd {
    Serve {
        #[arg(long, default_value = "1133")]
        jrp_bind: SocketAddr,
        #[arg(long, default_value = "1MiB")]
        jrp_max_frame_size: ByteSize,
        #[arg(long, default_value_t = 32)]
        jrp_queue_len: usize,

        #[arg(long, default_value = "1134")]
        http_bind: SocketAddr,

        #[arg(long, value_delimiter = ',')]
        kfk_brokers: Vec<String>,

        #[arg(long, default_value = "32KiB")]
        tcp_send_buf_size: ByteSize,
        #[arg(long, default_value = "32KiB")]
        tcp_recv_buf_size: ByteSize,

        #[arg(long)]
        thread_count: Option<usize>,
    },
    Produce {
        #[arg(long)]
        jrp_address: FastStr,
        #[arg(long, default_value = "1MiB")]
        jrp_frame_max_size: ByteSize,
        #[arg(long, default_value = "128KiB")]
        jrp_send_max_size: ByteSize,
        #[arg(long, default_value = "100")]
        jrp_send_max_rec_count: usize,
        #[arg(long, default_value = "1KiB")]
        jrp_send_max_rec_size: ByteSize,
        #[arg(long, default_value = "json")]
        jrp_value_codec: JrpCodec,

        #[arg(long)]
        kfk_topic: FastStr,
        #[arg(long, required = false)]
        kfk_partition: i32,

        #[arg(long)]
        file_path: FastStr,
        #[arg(long, default_value="value")]
        file_format: Format,
        #[arg(long)]
        file_load_max_size: Option<ByteSize>,
        #[arg(long)]
        file_load_max_rec_count: Option<usize>,

        #[arg(long)]
        prom_push_url: Url,
        #[arg(long, default_value = "10s", value_parser = duration_range_value_parse!(min: 1s, max: 1min))]
        prom_push_period: DurationHuman,

        #[arg(long, default_value = "1")]
        thread_count: Option<usize>,
    },
    Consume {
        #[arg(long)]
        jrp_address: FastStr,
        #[arg(long, default_value = "1MiB")]
        jrp_frame_max_size: ByteSize,
        #[arg(long, default_value = "str")]
        jrp_key_codec: Option<JrpCodec>,
        #[arg(long, default_value = "json")]
        jrp_value_codec: JrpCodec,
        #[arg(long, value_delimiter = ',')]
        jrp_header_codecs: Vec<NamedCodec>,
        #[arg(long)]
        jrp_header_codec_default: Option<JrpCodec>,

        #[arg(long)]
        kfk_topic: FastStr,
        #[arg(long, required = false)]
        kfk_partition: i32,
        #[arg(long, default_value = "earliest", value_parser = value_parser!(JrpOffset))]
        kfk_offset_from: JrpOffset,
        #[arg(long, default_value = "latest", value_parser = value_parser!(JrpOffset))]
        kfk_offset_until: JrpOffset,
        #[arg(long, default_value = "1KiB")]
        kfk_fetch_min_size: ByteSize,
        #[arg(long, default_value = "64KiB")]
        kfk_fetch_max_size: ByteSize,
        #[arg(long, default_value = "1s", value_parser = duration_range_value_parse!(min: 1s, max: 10s))]
        kfk_fetch_max_wait_time: DurationHuman,

        #[arg(long)]
        file_path: FastStr,
        #[arg(long, default_value="value")]
        file_format: Format,
        #[arg(long)]
        file_save_max_rec_count: Option<usize>,
        #[arg(long)]
        file_save_max_size: Option<ByteSize>,

        #[arg(long)]
        prom_push_url: Url,
        #[arg(long, default_value = "10s", value_parser = duration_range_value_parse!(min: 1s, max: 1min))]
        prom_push_period: DurationHuman,

        #[arg(long, default_value = "1")]
        thread_count: Option<usize>,
    },
}
