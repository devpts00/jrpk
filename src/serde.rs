use std::collections::BTreeMap;
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::JsonParser;
use anyhow::anyhow;
use bytes::{BufMut, Bytes, BytesMut};
use std::fmt::{Debug, Display, Write};
use std::mem::{swap, transmute};
use rskafka::chrono::DateTime;
use rskafka::record::Record;
use tokio::net::tcp::OwnedReadHalf;
use log::debug;
use tracing::info;

type JP = JsonParser<AsyncBufReaderJsonFeeder<OwnedReadHalf>>;
type RES = Result<(), anyhow::Error>;

#[inline] fn unexpected_start_object() -> RES { Err(anyhow!("unexpected start of json object")) }
#[inline] fn unexpected_end_object() -> RES { Err(anyhow!("unexpected end of json object")) }
#[inline] fn unexpected_start_array() -> RES { Err(anyhow!("unexpected start of json array")) }
#[inline] fn unexpected_end_array() -> RES { Err(anyhow!("unexpected end of json array")) }
#[inline] fn unexpected_field_name(name: &str) -> RES { Err(anyhow!("unexpected field name: {}", name)) }
#[inline] fn unexpected_value_string(value: &str) -> RES { Err(anyhow!("unexpected string value: {}", value)) }
#[inline] fn unexpected_value_int(value: i64) -> RES { Err(anyhow!("unexpected int value: {}", value)) }
#[inline] fn unexpected_value_float(value: f64) -> RES { Err(anyhow!("unexpected float value: {}", value)) }
#[inline] fn unexpected_value_bool(value: bool) -> RES { Err(anyhow!("unexpected bool value: {}", value)) }
#[inline] fn unexpected_value_null() -> RES { Err(anyhow!("unexpected null value")) }

pub trait DataBuilder {
    fn start_object(&mut self) -> RES { unexpected_start_object() }
    fn end_object(&mut self) -> RES { unexpected_end_object() }
    fn start_array(&mut self) -> RES { unexpected_start_array() }
    fn end_array(&mut self) -> RES { unexpected_end_array() }
    fn field_name(&mut self, name: &str) -> RES { unexpected_field_name(name) }
    fn value_string(&mut self, value: &str) -> RES { unexpected_value_string(value) }
    fn value_int(&mut self, value: i64) -> RES { unexpected_value_int(value) }
    fn value_float(&mut self, value: f64) -> RES { unexpected_value_float(value) }
    fn value_bool(&mut self, value: bool) -> RES { unexpected_value_bool(value) }
    fn value_null(&mut self) -> RES { unexpected_value_null() }
}

struct UnexpectedBuilder;

impl UnexpectedBuilder {
    fn new() -> Self { UnexpectedBuilder }
}

impl DataBuilder for UnexpectedBuilder {}

struct RootObjectBuilder;

impl DataBuilder for RootObjectBuilder {
    fn start_object(&mut self) -> RES { Ok(()) }
    fn end_object(&mut self) -> RES { Ok(()) }
}

static ROOT_OBJECT_BUILDER: RootObjectBuilder = RootObjectBuilder {};

fn root_object_builder_ptr() -> *mut dyn DataBuilder {
    unsafe { transmute(&ROOT_OBJECT_BUILDER as &dyn DataBuilder) }
}

struct RootArrayBuilder;

impl DataBuilder for RootArrayBuilder {
    fn start_array(&mut self) -> RES { Ok(()) }
    fn end_array(&mut self) -> RES { Ok(()) }
}

static ROOT_ARRAY_BUILDER: RootArrayBuilder = RootArrayBuilder {};

fn root_array_builder_ptr() -> *mut dyn DataBuilder {
    unsafe { transmute(&ROOT_ARRAY_BUILDER as &dyn DataBuilder) }
}

enum Level { Same, Up, Down }

trait ObjectBuilder {
    fn use_object_builder(&mut self, level: Level) -> &mut dyn DataBuilder;
    fn update_object_builder(&mut self, name: &str) -> RES;
}

impl <B: ObjectBuilder> DataBuilder for B {
    fn start_object(&mut self) -> RES { self.use_object_builder(Level::Down).start_object() }
    fn end_object(&mut self) -> RES { self.use_object_builder(Level::Up).end_object() }
    fn start_array(&mut self) -> RES { self.use_object_builder(Level::Down).start_array() }
    fn end_array(&mut self) -> RES { self.use_object_builder(Level::Up).end_array() }
    fn field_name(&mut self, name: &str) -> RES { self.update_object_builder(name) }
    fn value_string(&mut self, value: &str) -> RES { self.use_object_builder(Level::Same).value_string(value) }
    fn value_int(&mut self, value: i64) -> RES { self.use_object_builder(Level::Same).value_int(value) }
    fn value_float(&mut self, value: f64) -> RES { self.use_object_builder(Level::Same).value_float(value) }
    fn value_bool(&mut self, value: bool) -> RES { self.use_object_builder(Level::Same).value_bool(value) }
    fn value_null(&mut self) -> RES { self.use_object_builder(Level::Same).value_null() }
}

pub trait DataProvider {
    type Data;
    fn take(&mut self) -> Option<Self::Data>;
}

pub struct AtomicBuilder<A> {
    data: Option<A>
}

impl <A> AtomicBuilder<A> {
    fn new() -> Self {
        Self { data: None }
    }
}

impl <A> DataProvider for AtomicBuilder<A> {
    type Data = A;
    fn take(&mut self) -> Option<A> {
        self.data.take()
    }
}

impl DataBuilder for AtomicBuilder<String> {
    fn value_string(&mut self, value: &str) -> RES {
        self.data = Some(value.to_string());
        Ok(())
    }
}

impl DataBuilder for AtomicBuilder<usize> {
    fn value_int(&mut self, value: i64) -> RES {
        self.data = Some(value as usize);
        Ok(())
    }
}

impl DataBuilder for AtomicBuilder<bool> {
    fn value_bool(&mut self, value: bool) -> RES {
        self.data = Some(value);
        Ok(())
    }
}

pub struct BytesBuilder {
    bytes: BytesMut,
    comma: bool,
}

impl BytesBuilder {
    fn with_capacity(capacity: usize) -> Self {
        BytesBuilder {
            bytes: BytesMut::with_capacity(capacity),
            comma: false,
        }
    }
    #[inline]
    fn flush_comma(&mut self) {
        if self.comma {
            self.bytes.put_u8(b',');
        }
    }
    #[inline]
    fn next_comma(&mut self, comma: bool) {
        self.comma = comma;
    }
}

impl DataProvider for BytesBuilder {
    type Data = Bytes;
    fn take(&mut self) -> Option<Bytes> {
        self.comma = false;
        if self.bytes.is_empty() {
            None
        } else {
            let mut copy = BytesMut::with_capacity(self.bytes.len());
            swap(&mut copy, &mut self.bytes);
            Some(copy.freeze())
        }
    }
}

impl DataBuilder for BytesBuilder {
    fn start_object(&mut self) -> RES {
        info!("bytes: start object");
        self.flush_comma();
        self.bytes.put_u8(b'{');
        self.next_comma(false);
        Ok(())
    }
    fn end_object(&mut self) -> RES {
        info!("bytes: end object");
        self.bytes.put_u8(b'}');
        self.next_comma(true);
        Ok(())
    }
    fn start_array(&mut self) -> RES {
        self.flush_comma();
        self.bytes.put_u8(b'[');
        self.next_comma(false);
        Ok(())
    }
    fn end_array(&mut self) -> RES {
        self.bytes.put_u8(b']');
        self.next_comma(true);
        Ok(())
    }
    fn field_name(&mut self, name: &str) -> RES {
        self.flush_comma();
        self.bytes.put_u8(b'"');
        self.bytes.put_slice(name.as_bytes());
        self.bytes.put_u8(b'"');
        self.next_comma(false);
        Ok(())
    }
    fn value_string(&mut self, value: &str) -> RES {
        self.flush_comma();
        if self.bytes.is_empty() {
            self.bytes.put_slice(value.as_bytes());
        } else {
            self.bytes.put_u8(b'"');
            self.bytes.put_slice(value.as_bytes());
            self.bytes.put_u8(b'"');
        }
        self.next_comma(true);
        Ok(())
    }
    fn value_int(&mut self, value: i64) -> RES {
        self.flush_comma();
        self.bytes.write_fmt(format_args!("{}", value))?;
        self.next_comma(true);
        Ok(())
    }
    fn value_float(&mut self, value: f64) -> RES {
        self.flush_comma();
        self.bytes.write_fmt(format_args!("{}", value))?;
        self.next_comma(true);
        Ok(())
    }
    fn value_bool(&mut self, value: bool) -> RES {
        self.flush_comma();
        if value {
            self.bytes.put_slice(b"true")
        } else {
            self.bytes.put_slice(b"false")
        };
        self.next_comma(true);
        Ok(())
    }
    fn value_null(&mut self) -> RES {
        self.flush_comma();
        self.bytes.put_slice(b"null");
        self.next_comma(true);
        Ok(())
    }
}

struct ObjectBuilder2 {
    builders: BTreeMap<String, Box<dyn DataBuilder>>,
    current: Box<dyn DataBuilder>,
    level: usize,
}

impl ObjectBuilder2 {
    fn new(builders: BTreeMap<String, Box<dyn DataBuilder>>) -> Self {
        Self {
            builders,
            current: Box::new(UnexpectedBuilder::new()),
            level: 0,
        }
    }
}

impl DataBuilder for ObjectBuilder2 {

    fn start_object(&mut self) -> RES {
        if self.level == 0 {
            self.level += 1;
            Ok(())
        } else {
            self.level += 1;
            self.current.start_object()
        }
    }

    fn end_object(&mut self) -> RES {
        if self.level == 1 {
            self.level -= 1;
            Ok(())
        } else {
            self.level -= 1;
            self.current.end_object()
        }
    }

    fn start_array(&mut self) -> RES {
        self.current.start_array()
    }

    fn end_array(&mut self) -> RES {
        self.current.end_array()
    }

    fn field_name(&mut self, name: &str) -> RES {
        if self.level == 1 {
            match self.builders.remove(name) {
                Some(builder) => {
                    self.current = builder;
                    Ok(())
                },
                None => {
                    unexpected_field_name(name)
                }
            }
        } else {
            self.current.field_name(name)
        }
    }

    fn value_string(&mut self, value: &str) -> RES {
        self.current.value_string(value)
    }

    fn value_int(&mut self, value: i64) -> RES {
        self.current.value_int(value)
    }

    fn value_float(&mut self, value: f64) -> RES {
        self.current.value_float(value)
    }

    fn value_bool(&mut self, value: bool) -> RES {
        self.current.value_bool(value)
    }

    fn value_null(&mut self) -> RES {
        self.current.value_null()
    }
}

struct DelegateBuilderCtx {
    ptr: *mut dyn DataBuilder,
    level: usize,
}

impl DelegateBuilderCtx {
    fn new() -> Self {
        Self { ptr: root_object_builder_ptr(), level: 0 }
    }
    fn use_delegate(&mut self, level: Level) -> &mut dyn DataBuilder {
        let ptr = self.ptr;
        match level {
            Level::Same => {},
            Level::Down => { self.level += 1 },
            Level::Up => { self.level -= 1 },
        }
        if self.level == 0 {
            self.ptr = root_object_builder_ptr();
        }
        unsafe { transmute(ptr) }
    }
    fn set_delegate(&mut self, delegate: &mut dyn DataBuilder) -> RES {
        self.ptr = unsafe { transmute(delegate) };
        Ok(())
    }
}

struct RecordBuilder {
    current: DelegateBuilderCtx,
    key: AtomicBuilder<String>,
    value: BytesBuilder,
}

impl RecordBuilder {
    fn new() -> Self {
        Self {
            key: AtomicBuilder::new(),
            value: BytesBuilder::with_capacity(1024),
            current: DelegateBuilderCtx::new(),
        }
    }
}

impl ObjectBuilder for RecordBuilder {
    fn use_object_builder(&mut self, level: Level) -> &mut dyn DataBuilder {
        self.current.use_delegate(level)
    }
    fn update_object_builder(&mut self, name: &str) -> RES {
        if (self.current.level == 0) {
            if name == "key" {
                self.current.set_delegate(&mut self.key)
            } else if name == "value" {
                self.current.set_delegate(&mut self.value)
            } else {
                unexpected_field_name(name)
            }
        } else {
            self.current.use_delegate(Level::Same).field_name(name)
        }
    }
}

impl DataProvider for RecordBuilder {
    type Data = Record;
    fn take(&mut self) -> Option<Self::Data> {
        self.current.level = 0;
        self.current.ptr = root_object_builder_ptr();
        let key = self.key.take();
        let value = self.value.take();
        debug!("key: {:?}, value: {:?}", key, value);
        Some(Record {
            key: self.key.take().map(|k| k.into()),
            value: self.value.take().map(|v| v.into()),
            headers: BTreeMap::new(),
            timestamp: DateTime::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::BufRead;
    use actson::{JsonEvent, JsonParser};
    use actson::options::JsonParserOptionsBuilder;
    use actson::tokio::AsyncBufReaderJsonFeeder;
    use tokio::fs::File;
    use tokio::io::{AsyncRead, BufReader};
    use tracing::{debug, info, trace, warn};
    use crate::init_tracing;
    use crate::serde::{DataBuilder, DataProvider, RecordBuilder};

    async fn read(name: &str) -> Result<BufReader<File>, std::io::Error> {
        let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let path = format!("{}/files/{}", dir, name);
        let file = File::open(path).await?;
        Ok(BufReader::new(file))
    }

    #[tokio::test]
    async fn test_builders() {
        init_tracing();
        let br= read("builders.json").await.unwrap();
        let abr = AsyncBufReaderJsonFeeder::new(br);
        let opts = JsonParserOptionsBuilder::default()
            .with_streaming(true).build();
        let mut jp = JsonParser::new_with_options(abr, opts);
        let mut builder = RecordBuilder::new();
        let mut level = 0;
        while let Some(event) = jp.next_event().unwrap() {
            trace!("{:?}", event);
            match event {
                JsonEvent::NeedMoreInput => {
                    jp.feeder.fill_buf().await.unwrap();
                }
                JsonEvent::StartObject => {
                    level += 1;
                    builder.start_object().unwrap();
                }
                JsonEvent::EndObject => {
                    level -= 1;
                    builder.end_object().unwrap();
                    if level == 0 {
                        let record = builder.take().unwrap();
                        debug!("record: {:?}", record);
                    }
                }
                JsonEvent::StartArray => {
                    builder.start_array().unwrap();
                }
                JsonEvent::EndArray => {
                    builder.end_array().unwrap();
                }
                JsonEvent::FieldName => {
                    let name = jp.current_str().unwrap();
                    builder.field_name(name).unwrap();
                }
                JsonEvent::ValueString => {
                    let value = jp.current_str().unwrap();
                    builder.value_string(value).unwrap();
                }
                JsonEvent::ValueInt => {
                    let value: i64 = jp.current_int().unwrap();
                    builder.value_int(value).unwrap();
                }
                JsonEvent::ValueFloat => {
                    let value: f64 = jp.current_float().unwrap();
                    builder.value_float(value).unwrap();
                }
                JsonEvent::ValueTrue => {
                    builder.value_bool(true).unwrap();
                }
                JsonEvent::ValueFalse => {
                    builder.value_bool(false).unwrap();
                }
                JsonEvent::ValueNull => {
                    builder.value_null().unwrap();
                }
            }
        }
    }
}