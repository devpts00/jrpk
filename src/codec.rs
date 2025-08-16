use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use tracing::trace;
use crate::jsonrpc::JrpRsp;

#[derive(Debug)]
pub struct JsonCodec {
    level: u8,
    position: usize,
    quotes: bool,
    escape: bool,
}

impl JsonCodec {
    pub fn new() -> Self {
        JsonCodec {
            level: 0,
            position: 0,
            quotes: false,
            escape: false,
        }
    }
}

impl JsonCodec {
    #[inline(always)]
    fn dump(&self, b: u8) {
        trace!("char: '{}', state: {:?}", b as char, self);
    }
}

impl Decoder for JsonCodec {

    type Item = Bytes;
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
                        self.position = 0;
                        let frame = src.split_to(self.position).freeze();
                        return Ok(Some(frame))
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

impl Encoder<JrpRsp> for JsonCodec {
    type Error = anyhow::Error;
    fn encode(&mut self, response: JrpRsp, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(serde_json::to_writer(dst.writer(), &response)?)
    }
}
