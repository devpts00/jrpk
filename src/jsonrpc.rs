use std::fmt::{Debug, Display, Formatter};
use std::io::Read;
use std::ops::Range;
use anyhow::anyhow;
use futures::AsyncReadExt;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{EnumAccess, Error, IgnoredAny, Visitor};
use serde::ser::SerializeStruct;
use serde_json::Value;
use serde_json::value::RawValue;
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, info, trace};

#[derive(Debug, Deserialize, Serialize)]
pub enum JrpCodec {
    Base64
}

#[derive(Debug, Deserialize)]
pub struct JrpRecord {
    key: Option<String>,
    value: Box<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum JrpParams {
    #[serde(rename = "send")]
    Send {
        topic: String,
        partition: i32,
        codec: Option<JrpCodec>,
        records: Vec<JrpRecord>,
    },
    #[serde(rename = "fetch")]
    Fetch {
        topic: String,
        partition: i32,
        offset: i64,
        bytes: Range<i32>,
        max_wait_ms: Option<i32>,
        codec: Option<JrpCodec>,
    }
}

#[derive(Debug, Deserialize)]
pub struct JrpRequest {
    //pub jsonrpc: String,
    pub id: usize,
    #[serde(flatten)]
    pub params: JrpParams,
}

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
        write!(f, "error: {}", self.message)
    }
}

impl std::error::Error for JrpError {}

#[derive(Debug)]
pub struct JrpResponse {
    pub id: usize,
    pub result: Result<serde_json::value::Value, JrpError>,
}

impl JrpResponse {
    pub fn new(id: usize, result: Result<serde_json::value::Value, JrpError>) -> Self {
        JrpResponse { id, result }
    }
    pub fn success(id: usize, value: serde_json::value::Value) -> Self {
        JrpResponse::new(id, Ok(value))
    }
    pub fn error(id: usize, error: JrpError) -> Self {
        JrpResponse::new(id, Err(error))
    }
    pub fn ok(id: usize) -> Self {
        JrpResponse::success(id, serde_json::Value::String("ok".to_string()))
    }
}

impl Serialize for JrpResponse {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut r = s.serialize_struct("Response", 2)?;
        r.serialize_field("jsonrpc", "2.0")?;
        r.serialize_field("id", &self.id)?;
        match &self.result {
            Ok(result) => r.serialize_field("result", result)?,
            Err(error) => r.serialize_field("error", error)?,
        }
        r.end()
    }
}

#[derive(Debug)]
pub struct JrpFramer {
    level: u8,
    position: usize,
    quotes: bool,
    escape: bool,
}

impl JrpFramer {
    pub fn new() -> Self {
        JrpFramer {
            level: 0,
            position: 0,
            quotes: false,
            escape: false,
        }
    }
}

impl JrpFramer {
    #[inline(always)]
    fn dump(&self, b: u8) {
        trace!("char: '{}', state: {:?}", b as char, self);
    }
}

impl Decoder for JrpFramer {

    type Item = JrpRequest;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> anyhow::Result<Option<Self::Item>> {
        while self.position < src.len() {
            let b = src[self.position];
            self.dump(b);
            match b {
                _ if self.escape => {
                    self.escape = false;
                    self.position += 1;
                }
                b'\\' if self.quotes => {
                    self.escape = true;
                    self.position += 1;
                }
                b'"' => {
                    self.quotes = !self.quotes;
                    self.position += 1;
                }
                b'{' if !self.quotes => {
                    if self.level == 0 && self.position > 0 {
                        trace!("buf, skip: {}", self.position);
                        src.advance(self.position);
                        self.position = 1;
                    } else {
                        self.position += 1;
                    }
                    self.level += 1;
                }
                b'}' if !self.quotes => {
                    self.level -= 1;
                    self.position += 1;
                    if self.level == 0 {
                        trace!("buf, frame: {}", self.position);
                        let frame = src.split_to(self.position).freeze();
                        // let buf = frame.get(0..self.position)
                        //     .ok_or(anyhow!("bad slice indices"))?;
                        let json = serde_json::from_slice(&frame)?;
                        self.position = 0;
                        return Ok(Some(json))
                    }
                }
                _ => {
                    self.position += 1;
                }
            }
        }
        if self.level == 0 && src.len() > 0 {
            trace!("discard: {}", src.len());
            src.advance(src.len());
        }
        Ok(None)
    }
}

impl Encoder<JrpResponse> for JrpFramer {
    type Error = anyhow::Error;
    fn encode(&mut self, item: JrpResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(serde_json::to_writer(dst.writer(), &item)?)
    }
}


#[cfg(test)]
mod tests {
    use std::io::Read;
    use tokio_util::bytes::BytesMut;
    use tokio_util::codec::Decoder;
    use tracing::{error, info};
    use crate::init_tracing;
    use crate::jsonrpc::JrpFramer;

    fn read(name: &str) -> Vec<u8> {
        let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let path = format!("{}/files/{}", dir, name);
        let mut file = std::fs::File::open(path).unwrap();
        let mut content: Vec<u8> = Vec::new();
        file.read_to_end(&mut content).unwrap();
        content
    }

    #[test]
    fn test_deserialize() {
        init_tracing();
        let mut bytes = BytesMut::from(read("requests.json").as_slice());
        let mut framer = JrpFramer::new();
        loop {
            match framer.decode(&mut bytes) {
                Ok(Some(request)) => {
                    info!("request: {:?}", request);
                }
                Ok(None) => {
                    break;
                }
                Err(error) => {
                    error!("error: {}", error);
                }
            }
        }
    }
}
