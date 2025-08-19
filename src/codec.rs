use std::cmp::min;
use std::str::from_utf8;
use crate::jsonrpc::JrpRsp;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use tracing::trace;

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
    pub fn reset(&mut self) {
        self.level = 0;
        self.position = 0;
        self.quotes = false;
        self.escape = false;
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
        let text = from_utf8(src.as_ref()).unwrap();
        let length = min(20, text.len());
        trace!("decode, start, position: {}, length: {}, src: {}", self.position, src.len(), &text[..length]);
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
                        //trace!("decode, discard: {}", self.position);
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
                        let frame = src.split_to(self.position).freeze();
                        let frame_str = from_utf8(&frame).unwrap();
                        let src_str = from_utf8(src).unwrap();
                        trace!("decode, frm : {}", frame_str);
                        trace!("decode, src: {}", src_str);
                        self.position = 0;
                        return Ok(Some(frame))
                    }
                }
                _ => {
                    self.position += 1;
                }
            }
        }
        if self.level == 0 && src.len() > 0 {
            trace!("decode, discard: {}", src.len());
            src.advance(src.len());
        }
        self.reset();
        trace!("decode, end, position: {}", self.position);
        Ok(None)
    }
}

impl Encoder<JrpRsp> for JsonCodec {
    type Error = anyhow::Error;
    fn encode(&mut self, response: JrpRsp, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(serde_json::to_writer(dst.writer(), &response)?)
    }
}

impl Encoder<&[u8]> for JsonCodec {
    type Error = anyhow::Error;
    fn encode(&mut self, response: &[u8], dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_slice(response);
        Ok(())
    }
}
