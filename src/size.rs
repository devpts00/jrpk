use bytes::Bytes;
use chrono::{DateTime, TimeZone};
use faststr::FastStr;
use rskafka::client::partition::OffsetAt;
use rskafka::record::{Record, RecordAndOffset};
use std::collections::BTreeMap;
use std::mem;
use std::ops::Range;

#[macro_export]
macro_rules! size {
    ($name:ty) => {
        impl Size for $name {
            fn size(&self) -> usize {
                mem::size_of::<Self>()
            }
        }
    }
}

pub trait Size {
    fn size(&self) -> usize;
}

size!(u8);
size!(u16);
size!(i16);
size!(u32);
size!(i32);
size!(u64);
size!(i64);
size!(usize);

impl Size for [u8] {
    fn size(&self) -> usize {
        self.len()
    }
}

impl Size for str {
    fn size(&self) -> usize {
        self.len()
    }
}

impl Size for Bytes {
    fn size(&self) -> usize {
        self.len()
    }
}

impl Size for String {
    fn size(&self) -> usize {
        self.as_str().size()
    }
}

impl Size for FastStr {
    fn size(&self) -> usize {
        self.as_str().size()
    }
}

impl <T: TimeZone> Size for DateTime<T> {
    fn size(&self) -> usize {
        size_of::<DateTime<T>>()
    }
}

impl <T: Size> Size for Range<T> {
    fn size(&self) -> usize {
        self.start.size() + self.end.size()
    }
}

impl Size for OffsetAt {
    fn size(&self) -> usize {
        size_of::<OffsetAt>()
    }
}

impl Size for Record {
    fn size(&self) -> usize {
        self.key.size() + self.value.size() + self.timestamp.size() + self.headers.size()
    }
}

impl Size for RecordAndOffset {
    fn size(&self) -> usize {
        self.record.size() + self.offset.size()
    }
}

impl <T: Size> Size for Option<T> {
    fn size(&self) -> usize {
        match self {
            Some(x) => x.size(),
            None => 0
        }
    }
}

impl <T: Size> Size for Vec<T> {
    fn size(&self) -> usize {
        self.iter().map(|x| x.size()).sum()
    }
}

impl <K: Size, V: Size> Size for BTreeMap<K, V> {
    fn size(&self) -> usize {
        self.iter().map(|(k, v)| k.size() + v.size()).sum()
    }
}

impl <T: Size, U: Size> Size for (T, U) {
    fn size(&self) -> usize {
        let (t, u) = self;
        t.size() + u.size()
    }
}

impl <T: Size, U: Size, W: Size> Size for (T, U, W) {
    fn size(&self) -> usize {
        let (t, u, w) = self;
        t.size() + u.size() + w.size()
    }
}
