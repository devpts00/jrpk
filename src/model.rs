use crate::error::JrpkError;
use base64::prelude::BASE64_STANDARD;
use base64::{DecodeError, Engine};
use bytes::Bytes;
use rskafka::chrono::{DateTime, Utc};
use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use serde_valid::Validate;
use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::slice::from_raw_parts;
use std::str::FromStr;
use faststr::FastStr;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JrpData<'a> {
    /// JSON fragment utf-8 slice will be interpreted as bytes
    #[serde(borrow)]
    Json(Cow<'a, RawValue>),
    /// JSON string (without quotes) utf-8 slice will be interpreted as bytes
    #[serde(borrow)]
    Str(Cow<'a, str>),
    /// JSON string (without quotest) utf-8 slice will be base64 decoded to bytes
    #[serde(borrow)]
    Base64(Cow<'a, str>),
}

impl <'a> JrpData<'a> {
    pub fn json(json: Box<RawValue>) -> Self {
        JrpData::Json(Cow::Owned(json))
    }
    pub fn str(str: String) -> Self {
        JrpData::Str(Cow::Owned(str))
    }
    pub fn base64(str: String) -> Self {
        JrpData::Base64(Cow::Owned(str))
    }

    /// Returns byte slice of parsed JSON fragment
    pub fn as_bytes(self: &'a JrpData<'a>) -> Result<Cow<'a, [u8]>, DecodeError> {
        match self {
            // zero copy
            JrpData::Json(json) => {
                Ok(Cow::Borrowed(json.get().as_bytes()))
            }
            // zero copy
            JrpData::Str(text) => {
                Ok(Cow::Borrowed(text.as_bytes()))
            },
            // non-zero copy due to decoding
            JrpData::Base64(text) => {
                BASE64_STANDARD.decode(text.as_bytes()).map(|vec| Cow::Owned(vec))
            }
        }
    }

    pub fn into_bytes(self: JrpData<'a>) -> Result<Vec<u8>, DecodeError> {
        match self {
            // zero copy
            JrpData::Json(json) => {
                let br: Box<RawValue> = json.into_owned();
                let bs: Box<str> = br.into();
                let string: String = bs.into();
                Ok(string.into_bytes())
            }
            // zero copy
            JrpData::Str(text) => {
                let string = text.into_owned();
                Ok(string.into_bytes())
            },
            // non-zero copy due to deciding
            JrpData::Base64(text) => {
                BASE64_STANDARD.decode(text.as_bytes())
            }
        }
    }

    pub fn from_bytes(bytes: Vec<u8>, codec: JrpCodec) -> Result<Self, JrpkError> {
        match codec {
            JrpCodec::Json => {
                let string = String::from_utf8(bytes)?;
                let json = RawValue::from_string(string)?;
                let data = JrpData::json(json);
                Ok(data)
            }
            JrpCodec::Str => {
                let string = String::from_utf8(bytes)?;
                let data = JrpData::str(string);
                Ok(data)
            }
            JrpCodec::Base64 => {
                let base64= BASE64_STANDARD.encode(bytes);
                let data = JrpData::base64(base64);
                Ok(data)
            }
        }
    }

}



/// Fetch codec defines how to decode Kafka bytes to JSON
#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum JrpCodec {
    /// bytes will be interpreted as utf-8 and output as they are, assuming valid JSON fragment
    Json,
    /// bytes will be interpreted as utf-8 and output double-quoted, assuming valid
    Str,
    /// bytes will be base64 encoded to string and output double-quoted
    Base64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JrpCodecs {
    pub key: JrpCodec,
    pub value: JrpCodec,
}

impl Default for JrpCodecs {
    fn default() -> Self {
        JrpCodecs { key: JrpCodec::Str, value: JrpCodec::Json }
    }
}

impl JrpCodecs {
    pub fn new(key: JrpCodec, value: JrpCodec) -> Self {
        JrpCodecs { key, value }
    }
}

/// JSONRPC send record captures payload as references to raw JSON,
/// because there is no way to turn RawValue into Vec<u8>.
#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "'de: 'a"))]
pub struct JrpRecSend<'a> {
    pub key: Option<JrpData<'a>>,
    pub value: Option<JrpData<'a>>,
}

impl<'a> JrpRecSend<'a> {
    pub fn new(key: Option<JrpData<'a>>, value: Option<JrpData<'a>>) -> Self {
        Self { key, value }
    }
}

/// JSONRPC output record captures Kafka messages as owned raw JSON.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
pub struct JrpRecFetch<'a> {
    pub offset: i64,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<JrpData<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<JrpData<'a>>,
}

/// JSONRPC output record can fail to be created from Kafka record.
/// Kafka bytes might not be UTF-8 or JSON.
impl <'a> JrpRecFetch<'a> {
    pub fn new (offset: i64, timestamp: DateTime<Utc>, key: Option<JrpData<'a>>, value: Option<JrpData<'a>>) -> Self {
        JrpRecFetch { offset, timestamp, key, value }
    }
}

/// JSONRPC method as an enumeration.
#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
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
                write!(f, r#""earliest", "latest", 12345 or "2024-12-31T21:00:00.000Z""#)
            }

            fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(JrpOffset::Offset(v))
            }

            fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(JrpOffset::Offset(v as i64))
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
#[serde(bound(deserialize = "'de: 'a"))]
pub struct JrpParams<'a> {
    pub topic: FastStr,
    pub partition: i32,
    pub offset: Option<JrpOffset>,
    pub records: Option<Vec<JrpRecSend<'a>>>,
    pub bytes: Option<Range<i32>>,
    pub max_wait_ms: Option<i32>,
    pub codecs: Option<JrpCodecs>,
}

impl <'a> JrpParams<'a> {
    #[inline]
    pub fn new(
        topic: FastStr,
        partition: i32,
        offset: Option<JrpOffset>,
        records: Option<Vec<JrpRecSend<'a>>>,
        bytes: Option<Range<i32>>,
        max_wait_ms: Option<i32>,
        codecs: Option<JrpCodecs>,
    ) -> Self {
        JrpParams { topic, partition, offset, records, bytes, max_wait_ms, codecs }
    }

    #[inline]
    pub fn send (topic: FastStr, partition: i32, records: Vec<JrpRecSend<'a>>) -> Self {
        JrpParams::new(topic, partition, None, Some(records), None, None, None)
    }

    #[inline]
    pub fn fetch(topic: FastStr, partition: i32, offset: JrpOffset, bytes: Range<i32>, max_wait_ms: i32, codecs: JrpCodecs) -> Self {
        JrpParams::new(topic, partition, Some(offset), None, Some(bytes), Some(max_wait_ms), Some(codecs))
    }

    #[inline]
    pub fn offset (topic: FastStr, partition: i32, offset: JrpOffset) -> Self {
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
    #[allow(unused)]
    pub fn offset(id: usize, topic: FastStr, partition: i32, offset: JrpOffset) -> Self {
        JrpReq::new(id, JrpMethod::Offset, JrpParams::offset(topic, partition, offset))
    }
    pub fn fetch(id: usize, topic: FastStr, partition: i32, offset: JrpOffset, bytes: Range<i32>, max_wait_ms: i32, codecs: JrpCodecs) -> Self {
        JrpReq::new(id, JrpMethod::Fetch, JrpParams::fetch(topic, partition, offset, bytes, max_wait_ms, codecs))
    }
    pub fn send(id: usize, topic: FastStr, partition: i32, records: Vec<JrpRecSend<'a>>) -> Self {
        JrpReq::new(id, JrpMethod::Send, JrpParams::send(topic, partition, records))
    }
}

#[derive(Debug, Deserialize)]
pub struct JrpId {
    pub id: usize,
}

/// JSONRPC error.
#[derive(Debug, Serialize, Deserialize)]
pub struct JrpErrorMsg {
    pub message: String,
}

impl JrpErrorMsg {
    pub fn new(message: String) -> Self {
        JrpErrorMsg { message }
    }
}

impl Display for JrpErrorMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.serialize_str(&self.message)
    }
}

impl <E: std::error::Error> From<E> for JrpErrorMsg {
    fn from(value: E) -> Self {
        JrpErrorMsg::new(value.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", bound(deserialize = "'de: 'a"))]
pub enum JrpRspData<'a> {
    Send {
        offsets: Vec<i64>,
    },
    Fetch {
        high_watermark: i64,
        records: Vec<JrpRecFetch<'a>>,
    },
    Offset(i64)
}

impl <'a> JrpRspData<'a> {
    pub fn send(offsets: Vec<i64>) -> Self {
        JrpRspData::Send { offsets }
    }
    pub fn fetch(records: Vec<JrpRecFetch<'a>>, high_watermark: i64) -> Self {
        JrpRspData::Fetch { records, high_watermark }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
pub struct JrpRsp<'a> {
    pub id: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<JrpRspData<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JrpErrorMsg>,
}

impl <'a> JrpRsp<'a> {
    fn new(id: usize, result: Option<JrpRspData<'a>>, error: Option<JrpErrorMsg>) -> Self {
        JrpRsp { id, result, error  }
    }
    pub fn ok(id: usize, data: JrpRspData<'a>) -> Self {
        JrpRsp::new(id, Some(data), None)
    }
    pub fn err(id: usize, error: JrpErrorMsg) -> Self {
        JrpRsp::new(id, None, Some(error))
    }

    pub fn res(id: usize, result: Result<JrpRspData<'a>, JrpkError>) -> Self {
        match result {
            Ok(rsp_data) => JrpRsp::ok(id, rsp_data),
            Err(jrp_err) => JrpRsp::err(id, jrp_err.into())
        }
    }

    pub fn take_result(self) -> Result<JrpRspData<'a>, JrpErrorMsg> {
        match (self.result, self.error) {
            (Some(data), _) => Ok(data),
            (_, Some(err)) => Err(err),
            _ => unreachable!()
        }
    }
}

#[derive(Debug, Serialize)]
pub struct JrpBytes<J: Serialize> {
    #[serde(flatten)]
    json: J,
    #[serde(skip)]
    _bytes: Vec<Bytes>,
}

impl <J: Serialize> JrpBytes<J> {
    pub fn new(json: J, bytes: Vec<Bytes>) -> Self {
        JrpBytes { _bytes: bytes, json }
    }
}

impl <'a, J: Serialize + Deserialize<'a>> JrpBytes<J> {
    pub unsafe fn from_bytes(bytes: &Bytes) -> Result<J, serde_json::Error> {
        let p = bytes.as_ptr();
        let buf = unsafe { from_raw_parts(p, bytes.len()) };
        let json: J = serde_json::from_slice(buf)?;
        Ok(json)
    }
}
