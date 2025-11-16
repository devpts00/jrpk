use crate::error::JrpkError;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;
use std::cmp::min;
use std::str::{from_utf8, from_utf8_unchecked};
use tokio_util::codec::{Decoder, Encoder};
use tracing::{enabled, instrument, trace, Level};

#[derive(Debug)]
pub struct JsonCodec {
    max_frame_size: usize,
    level: u8,
    position: usize,
    quotes: bool,
    escape: bool,
}

impl JsonCodec {
    pub fn new(max_frame_size: usize) -> Self {
        JsonCodec {
            max_frame_size,
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
    type Error = JrpkError;

    #[instrument(level="trace", skip(self))]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {

        if enabled!(Level::TRACE) {
            let text = unsafe { from_utf8_unchecked(src.as_ref()) };
            let length = min(50, text.len() - self.position);
            trace!("decode, start, position: {}, length: {}, src: {}", self.position, src.len(), &text[self.position..self.position + length]);
        }

        while self.position < src.len() {

            if self.position > self.max_frame_size {
                return Err(JrpkError::FrameTooBig(self.position));
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
                        trace!("decode, frame : {}", from_utf8(&frame)?);
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

pub trait Meter {
    fn meter(&self, length: usize);
}

impl <T: Serialize, M: Meter> Encoder<(T, M)> for JsonCodec {
    type Error = JrpkError;
    fn encode(&mut self, item_with_meter: (T, M), dst: &mut BytesMut) -> Result<(), Self::Error> {
        let (item, meter) = item_with_meter;
        let length = dst.len();
        serde_json::to_writer(dst.writer(), &item)?;
        dst.put_u8(b'\n');
        meter.meter(dst.len() - length);
        Ok(())
    }
}
