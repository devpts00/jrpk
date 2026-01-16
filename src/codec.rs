use crate::error::JrpkError;
use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use tokio_util::codec::{Decoder, Encoder};
use crate::metrics::MeteredItem;

pub struct LinesCodec {
    pub max_length: usize,
    position: usize,
}

impl <T: Serialize> Encoder<MeteredItem<T>> for LinesCodec {
    type Error = JrpkError;
    fn encode(&mut self, metered_item: MeteredItem<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let MeteredItem { item, metrics, labels } = metered_item;
        let length = dst.len();
        dst.reserve(self.max_length);
        serde_json::to_writer(dst.writer(), &item)?;
        dst.put_u8(b'\n');
        metrics.size_by_value(&labels, dst.len() - length);
        Ok(())
    }
}

impl LinesCodec {
    pub fn new_with_max_length(max_length: usize) -> Self {
        LinesCodec { max_length, position: 0 }
    }
}

impl Decoder for LinesCodec {
    type Item = Bytes;
    type Error = JrpkError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match &src[self.position..].iter().position(|b| *b == b'\n') {
            Some(p) => {
                self.position += p + 1;
                if self.position < self.max_length {
                    let bytes = src.split_to(self.position).freeze();
                    src.reserve(self.max_length);
                    self.position = 0;
                    Ok(Some(bytes))
                } else {
                    Err(JrpkError::FrameTooBig(self.position))
                }
            }
            None => {
                self.position = src.len();
                Ok(None)
            }
        }
    }
}
