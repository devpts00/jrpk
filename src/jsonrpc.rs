use crate::kafka::{KfkError, KfkResId, KfkRsp};
use rskafka::chrono::{DateTime, Utc};
use rskafka::record::{Record, RecordAndOffset};
use serde::ser::SerializeStruct;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::str::FromStr;
use rskafka::client::partition::OffsetAt;
use serde::de::{Error, Visitor};
use crate::errors::{JrpkError, JrpkResult};
use crate::errors::JrpkError::ParseTimestamp;

fn bytes_from_raw_value_ref(value: Option<&RawValue>) -> Option<Vec<u8>> {
    match value {
        Some(value) => Some(value.get().as_bytes().to_vec()),
        None => None,
    }
}

fn raw_value_from_bytes(bytes: Option<Vec<u8>>) -> Result<Option<Box<RawValue>>, JrpkError> {
    match bytes {
        Some(bytes) => {
            let string = String::from_utf8(bytes)?;
            let value = RawValue::from_string(string)?;
            Ok(Some(value))
        }
        None => {
            Ok(None)
        }
    }
}

/// Codec defines how to encode/decode between bytes and JSON.
#[derive(Debug, Deserialize, Serialize)]
pub enum JrpCodec {
    Base64
}

/// JSONRPC send record captures payload as references to raw JSON,
/// because there is no way to turn RawValue into Vec<u8>.
#[derive(Debug, Deserialize)]
pub struct JrpRecSend<'a> {
    #[serde(borrow)]
    key: Option<&'a RawValue>,
    #[serde(borrow)]
    value: Option<&'a RawValue>,
}

/// JSONRPC input record can always be created consuming Kafka record.
impl <'a> Into<Record> for JrpRecSend<'a> {
    fn into(self) -> Record {
        Record {
            key: bytes_from_raw_value_ref(self.key),
            value: bytes_from_raw_value_ref(self.value),
            headers: BTreeMap::new(),
            timestamp: DateTime::default(),
        }
    }
}

/// JSONRPC output record captures Kafka messages as owned raw JSON.
#[derive(Debug, Serialize)]
pub struct JrpRecFetch {
    offset: i64,
    key: Option<Box<RawValue>>,
    value: Option<Box<RawValue>>,
}

/// JSONRPC output record can fail to be created from Kafka record.
/// Kafka bytes might not be UTF-8 or JSON.
impl TryFrom<RecordAndOffset> for JrpRecFetch {
    type Error = JrpkError;
    fn try_from(rec_and_offset: RecordAndOffset) -> Result<Self, Self::Error> {
        let offset = rec_and_offset.offset;
        let key = raw_value_from_bytes(rec_and_offset.record.key)?;
        let value = raw_value_from_bytes(rec_and_offset.record.value)?;
        Ok(JrpRecFetch { offset, key, value })
    }
}

/// JSONRPC method as an enumeration.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JrpMethod {
    Send, Fetch, Offset
}

#[derive(Debug)]
pub enum JrpOffsetAt {
    Earliest, Latest, Timestamp(DateTime<Utc>)
}

impl Into<OffsetAt> for JrpOffsetAt {
    fn into(self) -> OffsetAt {
        match self {
            JrpOffsetAt::Earliest => OffsetAt::Earliest,
            JrpOffsetAt::Latest => OffsetAt::Latest,
            JrpOffsetAt::Timestamp(t) => OffsetAt::Timestamp(t)
        }
    }
}

impl <'de> Deserialize<'de> for JrpOffsetAt {

    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {

        struct JrpOffsetVisitor;

        impl <'de> Visitor<'de> for JrpOffsetVisitor {
            type Value = JrpOffsetAt;
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
                DateTime::<Utc>::from_timestamp_millis(v)
                    .map(|ms| JrpOffsetAt::Timestamp(ms))
                    .ok_or(E::custom(format!("invalid timestamp: {}", v)))
            }

            fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
                if v == "earliest" {
                    Ok(JrpOffsetAt::Earliest)
                } else if v == "latest" {
                    Ok(JrpOffsetAt::Latest)
                } else if let Ok(ts) = DateTime::<Utc>::from_str(v) {
                    Ok(JrpOffsetAt::Timestamp(ts))
                } else if let Ok(ms) = i64::from_str(v) {
                    self.visit_i64(ms)
                } else {
                    Err(E::custom(format!("invalid timestamp: {}", v)))
                }
            }
        }

        deserializer.deserialize_any(JrpOffsetVisitor)
    }
}

/// JSONRPC params is a superset of arguments for all methods.
/// That is a consequence of RawValue usage, that requires same structure of Rust and JSON.
#[derive(Debug, Deserialize)]
pub struct JrpParams<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub codec: Option<JrpCodec>,
    pub records: Option<Vec<JrpRecSend<'a>>>,
    pub offset: Option<i64>,
    pub bytes: Option<Range<i32>>,
    pub max_wait_ms: Option<i32>,
    pub at: Option<JrpOffsetAt>,
}

/// JSONRPC request.
#[derive(Debug, Deserialize)]
pub struct JrpReq<'a> {
    pub jsonrpc: &'a str,
    pub id: usize,
    pub method: JrpMethod,
    pub params: JrpParams<'a>,
}

/// JSONRPC error.
#[derive(Debug, Serialize)]
pub struct JrpError {
    message: String,
}

impl JrpError {
    pub fn new(message: String) -> Self {
        JrpError { message }
    }
}

impl From<JrpkError> for JrpError {
    fn from(error: JrpkError) -> Self {
        JrpError::new(format!("{}", error))
    }
}

impl From<KfkError> for JrpError {
    fn from(value: KfkError) -> Self {
        JrpError::new(format!("{}", value))
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JrpRspData {
    Send {
        offsets: Vec<i64>,
    },
    Fetch {
        records: Vec<JrpRecFetch>,
        high_watermark: i64,
    },
    Offset(i64)
}

impl JrpRspData {
    fn send(offsets: Vec<i64>) -> Self {
        JrpRspData::Send { offsets }
    }
    fn fetch(records: Vec<JrpRecFetch>, high_watermark: i64) -> Self {
        JrpRspData::Fetch { records, high_watermark }
    }
}

impl TryFrom<KfkRsp> for JrpRspData {
    type Error = JrpkError;
    fn try_from(value: KfkRsp) -> JrpkResult<Self> {
        match value {
            KfkRsp::Send { offsets } => {
                Ok(JrpRspData::send(offsets))
            }
            KfkRsp::Fetch { recs_and_offsets, high_watermark } => {
                let res_records: JrpkResult<Vec<JrpRecFetch>> = recs_and_offsets.into_iter()
                    .map(|ro| { ro.try_into() }).collect();
                res_records.map(|records| JrpRspData::Fetch { records, high_watermark })
            }
            KfkRsp::Offset(offset) => {
                Ok(JrpRspData::Offset(offset))
            }
        }
    }
}

#[derive(Debug)]
pub struct JrpRsp {
    pub id: usize,
    pub result: Result<JrpRspData, JrpError>,
}

impl JrpRsp {
    pub fn new(id: usize, result: Result<JrpRspData, JrpError>) -> Self {
        JrpRsp { id, result }
    }
    pub fn ok(id: usize, data: JrpRspData) -> Self {
        JrpRsp::new(id, Ok(data))
    }
    pub fn err(id: usize, error: JrpError) -> Self {
        JrpRsp::new(id, Err(error))
    }
}

impl From<KfkResId> for JrpRsp {
    fn from(value: KfkResId) -> Self {
        let id = value.id;
        match value.res {
            Ok(rsp) => {
                let res_data: JrpkResult<JrpRspData> = rsp.try_into();
                match res_data {
                    Ok(data) => JrpRsp::ok(id, data),
                    Err(err) => JrpRsp::err(id, err.into())
                }
            },
            Err(err) => {
                JrpRsp::err(id, err.into())
            }
        }
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
