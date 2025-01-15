use anyhow::anyhow;
use serde::{Deserialize, Deserializer};
use tokio_util::bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Deserialize)]
pub enum Partition {
    #[serde(rename = "num")]
    Num(u16),
    #[serde(rename = "key")]
    Key(String),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum Params {
    #[serde(rename = "send")]
    Send {
        topic: String,
        partition: Option<Partition>,
        body: serde_json::Value
    },
    #[serde(rename = "poll")]
    Poll {
        topic: String,
        partition: Option<Partition>,
        count: u16,
    }
}

#[derive(Debug, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    pub id: u64,
    #[serde(flatten)]
    pub params: Params,
}

pub struct Codec {
    level: u8,
    start: usize,
    position: usize,
    count: usize,
}

impl Codec {
    pub fn new() -> Self {
        Codec {
            level: 0,
            start: 0,
            position: 0,
            count: 0
        }
    }
}

impl Decoder for Codec {

    type Item = Request;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> anyhow::Result<Option<Self::Item>> {

        while self.position < src.len() {
            match src[self.position] {
                b'{'=> {
                    if self.level == 0 {
                        self.start = self.position;
                    }
                    self.level += 1;
                    self.position += 1;
                }
                b'}' => {
                    self.level -= 1;
                    self.position += 1;
                    if self.level == 0 {
                        let ready = src.split_to(self.position);
                        let buf = ready.get(self.start..self.position)
                            .ok_or(anyhow!("bad slice indices"))?;
                        let json = serde_json::from_slice(buf)?;
                        self.position = 0;
                        self.start = 0;
                        self.count += 1;
                        return Ok(Some(json))
                    }
                }
                _ => {
                    self.position += 1;
                }
            }
        }

        //info!("level: {}, count: {}, range: {} - {}", self.level, self.count, self.start, self.position);

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
    use crate::jsonrpc::Request;
    #[test]
    fn test_deserialize() {
        let send: Request = serde_json::from_str(r#"{ "jsonrpc": "2.0", "id": 1, "method": "send", "params": { "topic": "posts", "partition": { "key": "john" }, "body": { "first": "john", "last": "doe", "age": 35 } } }"#).unwrap();
        println!("send: {:?}", send);
        let poll: Request = serde_json::from_str(r#"{ "jsonrpc": "2.0", "id": 1, "method": "poll", "params": { "topic": "posts", "partition": { "num": 7 }, "count": 1024 } }"#).unwrap();
        println!("poll: {:?}", poll);
    }
}
