use std::borrow::Borrow;
use crate::kafka::{KfkError, KfkResId, KfkRsp};
use rskafka::chrono::DateTime;
use rskafka::record::{Record, RecordAndOffset};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;

fn bytes_from_raw_value_ref(value: Option<&RawValue>) -> Option<Vec<u8>> {
    match value {
        Some(value) => Some(value.get().as_bytes().to_vec()),
        None => None,
    }
}

fn raw_value_from_bytes(bytes: Option<Vec<u8>>) -> Result<Option<Box<RawValue>>, anyhow::Error> {
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

impl Display for JrpCodec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JrpCodec::Base64 => f.write_str("Base64"),
        }
    }
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

impl Display for JrpRecSend<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ ")?;
        let mut comma = false;
        if let Some(key) = self.key {
            write!(f, "key: [{}]", key.get().len())?;
            comma = true;
        }
        if let Some(value) = self.value {
            if comma {
                write!(f, ", ")?;
            }
            write!(f, "value: [{}]", value.get().len())?;
        }
        write!(f, " }}")
    }
}

/// JSONRPC input record can always be created consuming Kafka record.
impl <'a> Into<Record> for JrpRecSend<'a> {
    fn into(self) -> Record {
        Record {
            key: bytes_from_raw_value_ref(self.key),
            value: bytes_from_raw_value_ref(self.key),
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

impl Display for JrpRecFetch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ ")?;
        write!(f, "offset: {}", self.offset)?;
        if let Some(key) = self.key.as_ref() {
            write!(f, ", key: [{}]", key.get().len())?;
        }
        if let Some(value) = self.value.as_ref() {
            write!(f, ", value: [{}]", value.get().len())?;
        }
        write!(f, " }}")
    }
}

/// JSONRPC output record can fail to be created from Kafka record.
/// Kafka bytes might not be UTF-8 or JSON.
impl TryFrom<RecordAndOffset> for JrpRecFetch {
    type Error = anyhow::Error;
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
    Send, Fetch
}

impl Display for JrpMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JrpMethod::Send => f.write_str("send"),
            JrpMethod::Fetch => f.write_str("fetch"),
        }
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
}

impl Display for JrpParams<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ ")?;
        write!(f, "topic: {}, partition: {}", self.topic, self.partition)?;
        if let Some(records) = self.records.as_ref() {
            write!(f, ", records: [{}]", records.len())?;
        }
        if let Some(offset) = self.offset {
            write!(f, ", offset: {}", offset)?;
        }
        if let Some(bytes) = self.bytes.as_ref() {
            write!(f, ", bytes: [{}..{})", bytes.start, bytes.end)?;
        }
        if let Some(max_wait_ms) = self.max_wait_ms {
            write!(f, ", max_wait_ms: {}", max_wait_ms)?;
        }
        write!(f, " }}")
    }
}

/// JSONRPC request.
#[derive(Debug, Deserialize)]
pub struct JrpReq<'a> {
    pub jsonrpc: &'a str,
    pub id: usize,
    pub method: JrpMethod,
    pub params: JrpParams<'a>,
}

impl Display for JrpReq<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ jsonrpc: {}, id: {}, method: {}, params: {} }}", self.jsonrpc, self.id, self.method, self.params)
    }
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

impl Display for JrpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.message, f)
    }
}

impl From<anyhow::Error> for JrpError {
    fn from(error: anyhow::Error) -> Self {
        JrpError::new(format!("{}", error))
    }
}

impl From<KfkError> for JrpError {
    fn from(value: KfkError) -> Self {
        JrpError::new(format!("{}", value))
    }
}

#[derive(Debug, Serialize)]
pub enum JrpRspData {
    Send {
        offsets: Vec<i64>,
    },
    Fetch {
        records: Vec<JrpRecFetch>,
        high_watermark: i64,
    }
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
    type Error = anyhow::Error;
    fn try_from(value: KfkRsp) -> Result<Self, Self::Error> {
        match value {
            KfkRsp::Send { offsets } => {
                Ok(JrpRspData::send(offsets))
            }
            KfkRsp::Fetch { recs_and_offsets, high_watermark } => {
                let res_records: Result<Vec<JrpRecFetch>, anyhow::Error> = recs_and_offsets.into_iter()
                    .map(|ro| { ro.try_into() }).collect();
                res_records.map(|records| JrpRspData::Fetch { records, high_watermark })
            }
        }
    }
}

impl Display for JrpRspData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JrpRspData::Send { offsets } => {
                write!(f, "{{ offsets: {:?} }}", offsets)
            }
            JrpRspData::Fetch { records, high_watermark } => {
                write!(f, "{{ ")?;
                write!(f, "offsets: [")?;
                let mut comma = false;
                for r in records {
                    if comma {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", r)?;
                    comma = true;
                }
                write!(f, "]")?;
                write!(f, ", high_watermark: {}", high_watermark)?;
                write!(f, " }}")
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
                let res_data: Result<JrpRspData, anyhow::Error> = rsp.try_into();
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

impl Display for JrpRsp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.result {
            Ok(data) => write!(f, "{{ id: {}, result: {} }}", self.id, data),
            Err(error) => write!(f, "{{ id: {}, error: {} }}", self.id, error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    fn read(name: &str) -> Vec<u8> {
        let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let path = format!("{}/files/{}", dir, name);
        let mut file = std::fs::File::open(path).unwrap();
        let mut content: Vec<u8> = Vec::new();
        file.read_to_end(&mut content).unwrap();
        content
    }

    // #[test]
    // fn test_deserialize() {
    //     init_tracing();
    //     let mut bytes = BytesMut::from(read("requests.json").as_slice());
    //     let mut framer = JsonCodec::new();
    //     loop {
    //         match framer.decode(&mut bytes) {
    //             Ok(Some(frame)) => {
    //                 let req: JrpReq = serde_json::from_slice(&frame).unwrap();
    //                 info!("request: {:?}", req);
    //                 if let Some(records) = req.params.records {
    //                    for record in records {
    //                        if let Some(value) = record.value {
    //                            let v = value.get().as_bytes().to_vec();
    //                            info!("vec: {:?}", v);
    //                        }
    //                    }
    //                 }
    //             }
    //             Ok(None) => {
    //                 break;
    //             }
    //             Err(error) => {
    //                 error!("error: {}", error);
    //             }
    //         }
    //     }
    // }
}
