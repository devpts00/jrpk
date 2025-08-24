use std::cmp::min;
use std::str::{from_utf8, from_utf8_unchecked};
use crate::jsonrpc::JrpRsp;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, enabled, info, trace, Level};
use crate::MAX_FRAME_SIZE;

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

#[derive(Error, Debug)]
pub enum BytesFrameDecoderError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame too big: {0}")]
    FrameTooBig(usize),
}

impl Decoder for JsonCodec {

    type Item = Bytes;
    type Error = BytesFrameDecoderError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {

        if enabled!(Level::TRACE) {
            let text = unsafe { from_utf8_unchecked(src.as_ref()) };
            let length = min(50, text.len() - self.position);
            trace!("decode, start, position: {}, length: {}, src: {}", self.position, src.len(), &text[self.position..self.position + length]);
        }

        while self.position < src.len() {

            if self.position > MAX_FRAME_SIZE {
                return Err(BytesFrameDecoderError::FrameTooBig(self.position));
            }

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
                        debug!("frame, length: {}", frame.len());
                        trace!("decode, frm : {}", from_utf8(&frame).unwrap());
                        self.reset();
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
            self.reset()
        }

        trace!("decode, end, position: {}", self.position);
        Ok(None)
    }
}

#[derive(Error, Debug)]
pub enum JsonEncoderError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::error::Error),
}

impl Encoder<JrpRsp> for JsonCodec {
    type Error = JsonEncoderError;
    fn encode(&mut self, response: JrpRsp, dst: &mut BytesMut) -> Result<(), Self::Error> {
        serde_json::to_writer(dst.writer(), &response)?;
        dst.put_u8(b'\n');
        Ok(())
    }
}

impl Encoder<&[u8]> for JsonCodec {
    type Error = std::io::Error;
    fn encode(&mut self, response: &[u8], dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_slice(response);
        Ok(())
    }
}
