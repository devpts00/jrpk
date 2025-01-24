use anyhow::anyhow;
use serde::Deserialize;
use tokio_util::bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, info, trace};

#[derive(Debug, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum Params {
    #[serde(rename = "send")]
    Send {
        topic: String,
        key: Option<String>,
        partition: Option<i32>,
        payload: serde_json::Value,
    },
    #[serde(rename = "poll")]
    Poll {
        topic: String,
        partition: Option<i32>,
        count: u16,
    }
}

#[derive(Debug, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    pub id: usize,
    #[serde(flatten)]
    pub params: Params,
}

#[derive(Debug)]
pub struct Codec {
    level: u8,
    position: usize,
    quotes: bool,
    escape: bool,
}

impl Codec {
    pub fn new() -> Self {
        Codec {
            level: 0,
            position: 0,
            quotes: false,
            escape: false,
        }
    }
}

impl Codec {
    #[inline(always)]
    fn dump(&self, b: u8) {
        trace!("char: '{}', state: {:?}", b as char, self);
    }
}

impl Decoder for Codec {

    type Item = Request;
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
                        let _ = src.split_to(self.position);
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
                        let frame = src.split_to(self.position);
                        let buf = frame.get(0..self.position)
                            .ok_or(anyhow!("bad slice indices"))?;
                        let json = serde_json::from_slice(buf)?;
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
            let _ = src.split_to(src.len());
        }
        Ok(None)
    }
}

impl Encoder<serde_json::Value> for Codec {
    type Error = anyhow::Error;
    fn encode(&mut self, item: serde_json::Value, dst: &mut BytesMut) -> std::result::Result<(), Self::Error> {
        Ok(serde_json::to_writer(dst.writer(), &item)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::serde::Request;
    #[test]
    fn test_deserialize() {
        let send: Request = serde_json::from_str(r#"{ "jsonrpc": "2.0", "id": 1, "method": "send", "params": { "topic": "posts", "key": "john", "payload": { "first": "john", "last": "doe", "age": 35 } } }"#).unwrap();
        println!("send: {:?}", send);
        let poll: Request = serde_json::from_str(r#"{ "jsonrpc": "2.0", "id": 1, "method": "poll", "params": { "topic": "posts", "partition": 7, "count": 1024 } }"#).unwrap();
        println!("poll: {:?}", poll);
    }
}
