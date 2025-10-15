use crate::kafka::RsKafkaError;
use base64::prelude::BASE64_STANDARD;
use base64::{DecodeError, Engine};
use rskafka::chrono::{DateTime, Utc};
use rskafka::client::partition::OffsetAt;
use serde::de::{Error, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::str::FromStr;
use std::string::FromUtf8Error;
use serde_valid::Validate;
use thiserror::Error;


#[derive(Error, Debug)]
pub enum JrpError {
    #[error("syntax: {0}")]
    Syntax(&'static str),
    #[error("utf8: {0}")]
    Utf8(#[from] FromUtf8Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::error::Error),
    #[error("base64: {0}")]
    Base64(#[from] DecodeError),
}

/// Send codec defines how encode JSON to Kafka bytes
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JrpDataRef<'a> {
    /// JSON fragment utf-8 slice will be interpreted as bytes
    Json(&'a RawValue),
    /// JSON string (without quotes) utf-8 slice will be interpreted as bytes
    Str(&'a str),
    /// JSON string (without quotest) utf-8 slice will be base64 decoded to bytes
    Base64(&'a str),
}

impl <'a> JrpDataRef<'a> {
    pub fn to_bytes(self: JrpDataRef<'a>) -> Result<Vec<u8>, DecodeError> {
        match self {
            JrpDataRef::Json(fragment) => {
                Ok(fragment.get().as_bytes().to_vec())
            },
            JrpDataRef::Str(text) => {
                Ok(text.as_bytes().to_vec())
            },
            JrpDataRef::Base64(text) => {
                BASE64_STANDARD.decode(text)
            }
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JrpDataOwn {
    Json(Box<RawValue>),
    Str(String),
    Base64(String),
}

impl JrpDataOwn {
    fn json(string: String) -> Result<Self, serde_json::Error> {
        Ok(JrpDataOwn::Json(RawValue::from_string(string)?))
    }
    fn str(string: String) -> Self {
        JrpDataOwn::Str(string)
    }
    fn base64(v: Vec<u8>) -> Self {
        JrpDataOwn::Base64(BASE64_STANDARD.encode(v.as_slice()))
    }
    pub fn from_bytes(bytes: Vec<u8>, codec: JrpDataCodec) -> Result<Self, JrpError> {
        match codec {
            JrpDataCodec::Json => {
                let string = String::from_utf8(bytes)?;
                let data = JrpDataOwn::json(string)?;
                Ok(data)
            }
            JrpDataCodec::Str => {
                let string = String::from_utf8(bytes)?;
                let data = JrpDataOwn::str(string);
                Ok(data)
            }
            JrpDataCodec::Base64 => {
                let base64 = JrpDataOwn::base64(bytes);
                Ok(base64)
            }
        }
    }
}

/// Fetch codec defines how to decode Kafka bytes to JSON
#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum JrpDataCodec {
    /// bytes will be interpreted as utf-8 and output as they are, assuming valid JSON fragment
    Json,
    /// bytes will be interpreted as utf-8 and output double-quoted, assuming valid
    Str,
    /// bytes will be base64 encoded to string and output double-quoted
    Base64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JrpDataCodecs {
    pub key: JrpDataCodec,
    pub value: JrpDataCodec,
}

#[derive(Debug)]
pub enum JrpExtra {
    None,
    DataCodecs(JrpDataCodecs),
}

#[derive(Debug)]
pub struct JrpCtx {
    pub id: usize,
    pub extra: JrpExtra,
}

impl JrpCtx {
    pub fn new(id: usize, extra: JrpExtra) -> Self {
        Self { id, extra }
    }
}

/// JSONRPC send record captures payload as references to raw JSON,
/// because there is no way to turn RawValue into Vec<u8>.
#[derive(Debug, Deserialize, Serialize)]
pub struct JrpRecSend<'a> {
    #[serde(borrow)]
    pub key: Option<JrpDataRef<'a>>,
    #[serde(borrow)]
    pub value: Option<JrpDataRef<'a>>,
}

/// JSONRPC output record captures Kafka messages as owned raw JSON.
#[derive(Debug, Serialize)]
pub struct JrpRecFetch {
    offset: i64,
    key: Option<JrpDataOwn>,
    value: Option<JrpDataOwn>,
}

/// JSONRPC output record can fail to be created from Kafka record.
/// Kafka bytes might not be UTF-8 or JSON.
impl JrpRecFetch {
    pub fn new (offset: i64, key: Option<JrpDataOwn>, value: Option<JrpDataOwn>) -> Self {
        JrpRecFetch { offset, key, value }
    }
}

/// JSONRPC method as an enumeration.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JrpMethod {
    Send,
    Fetch,
    Offset,
}

#[derive(Debug)]
pub enum JrpOffset {
    Earliest,
    Latest,
    Timestamp(DateTime<Utc>),
    Offset(i64)
}

impl Serialize for JrpOffset {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            JrpOffset::Earliest => s.serialize_str("earliest"),
            JrpOffset::Latest => s.serialize_str("latest"),
            JrpOffset::Timestamp(t) => s.serialize_str(t.to_rfc3339().as_str()),
            JrpOffset::Offset(offset) => s.serialize_i64(*offset),
        }
    }
}

impl <'de> Deserialize<'de> for JrpOffset {

    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {

        struct JrpOffsetVisitor;

        impl <'de> Visitor<'de> for JrpOffsetVisitor {
            type Value = JrpOffset;
            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, r#""earliest", "latest", 1755794154497 or "2024-12-31T21:00:00.000Z""#)
            }

            fn visit_i8<E: Error>(self, v: i8) -> Result<Self::Value, E> {
                self.visit_i64(v as i64)
            }

            fn visit_i16<E: Error>(self, v: i16) -> Result<Self::Value, E> {
                self.visit_i64(v as i64)
            }

            fn visit_i32<E: Error>(self, v: i32) -> Result<Self::Value, E> {
                self.visit_i64(v as i64)
            }

            fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(JrpOffset::Offset(v))
            }

            fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
                if v == "earliest" {
                    Ok(JrpOffset::Earliest)
                } else if v == "latest" {
                    Ok(JrpOffset::Latest)
                } else if let Ok(ts) = DateTime::<Utc>::from_str(v) {
                    Ok(JrpOffset::Timestamp(ts))
                } else if let Ok(ms) = i64::from_str(v) {
                    self.visit_i64(ms)
                } else {
                    Err(E::custom(format!("invalid offset: {}", v)))
                }
            }
        }

        deserializer.deserialize_any(JrpOffsetVisitor)
    }
}

/// JSONRPC params is a superset of arguments for all methods.
/// That is a consequence of RawValue usage, that requires same structure of Rust and JSON.
#[derive(Debug, Deserialize, Serialize)]
pub struct JrpParams<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: Option<JrpOffset>,
    pub codecs: Option<JrpDataCodecs>,
    pub records: Option<Vec<JrpRecSend<'a>>>,
    pub bytes: Option<Range<i32>>,
    pub max_wait_ms: Option<i32>,
}

impl <'a> JrpParams<'a> {
    #[inline]
    pub fn new(
        topic: &'a str,
        partition: i32,
        offset: Option<JrpOffset>,
        codecs: Option<JrpDataCodecs>,
        records: Option<Vec<JrpRecSend<'a>>>,
        bytes: Option<Range<i32>>,
        max_wait_ms: Option<i32>,
    ) -> Self {
        JrpParams { topic, partition, offset, codecs, records, bytes, max_wait_ms }
    }

    #[inline]
    pub fn send (topic: &'a str, partition: i32, records: Vec<JrpRecSend<'a>>) -> Self {
        JrpParams::new(topic, partition, None, None, Some(records), None, None)
    }

    #[inline]
    pub fn fetch(topic: &'a str, partition: i32, offset: JrpOffset, codecs: JrpDataCodecs, bytes: Range<i32>, max_wait_ms: i32) -> Self {
        JrpParams::new(topic, partition, Some(offset), Some(codecs), None, Some(bytes), Some(max_wait_ms))
    }

    #[inline]
    pub fn offset (topic: &'a str, partition: i32, offset: JrpOffset) -> Self {
        JrpParams::new(topic, partition, Some(offset), None, None, None, None)
    }
}

/// JSONRPC request.
#[derive(Debug, Deserialize, Validate, Serialize)]
pub struct JrpReq<'a> {
    #[validate(enumerate = ["2.0"])]
    pub jsonrpc: &'a str,
    pub id: usize,
    pub method: JrpMethod,
    pub params: JrpParams<'a>,
}

impl <'a> JrpReq<'a> {
    pub fn new(id: usize, method: JrpMethod, params: JrpParams<'a>) -> Self {
        JrpReq { jsonrpc: "2.0", id, method, params }
    }
}

#[derive(Debug, Deserialize)]
pub struct JrpId {
    pub id: usize,
}

/// JSONRPC error.
#[derive(Debug, Serialize)]
pub struct JrpErrorMsg {
    message: String,
}

impl JrpErrorMsg {
    pub fn new(message: String) -> Self {
        JrpErrorMsg { message }
    }
}

impl From<JrpError> for JrpErrorMsg {
    fn from(error: JrpError) -> Self {
        JrpErrorMsg::new(format!("{}", error))
    }
}

impl From<RsKafkaError> for JrpErrorMsg {
    fn from(value: RsKafkaError) -> Self {
        JrpErrorMsg::new(format!("{}", value))
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JrpRspData {
    Send {
        offsets: Vec<i64>,
    },
    Fetch {
        #[serde(skip_serializing_if = "Option::is_none")]
        next_offset: Option<i64>,
        high_watermark: i64,
        records: Vec<JrpRecFetch>,
    },
    Offset(i64)
}

impl JrpRspData {
    pub fn send(offsets: Vec<i64>) -> Self {
        JrpRspData::Send { offsets }
    }
    pub fn fetch(records: Vec<JrpRecFetch>, high_watermark: i64) -> Self {
        let next_offset = records.iter()
            .map(|r|r.offset)
            .max()
            .map(|o| o + 1);
        JrpRspData::Fetch { records, next_offset, high_watermark }
    }
}

#[derive(Debug)]
pub struct JrpRsp {
    pub id: usize,
    pub result: Result<JrpRspData, JrpErrorMsg>,
}

impl JrpRsp {
    pub fn new(id: usize, result: Result<JrpRspData, JrpErrorMsg>) -> Self {
        JrpRsp { id, result }
    }
    pub fn ok(id: usize, data: JrpRspData) -> Self {
        JrpRsp::new(id, Ok(data))
    }
    pub fn err(id: usize, error: JrpErrorMsg) -> Self {
        JrpRsp::new(id, Err(error))
    }
}

impl Serialize for JrpRsp {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut r = s.serialize_struct("Response", 2)?;
        r.serialize_field("jsonrpc", "2.0")?;
        r.serialize_field("id", &self.id)?;
        match &self.result {
            Ok(data) => r.serialize_field("result", data)?,
            Err(error) => r.serialize_field("error", error)?,
        }
        r.end()
    }
}
