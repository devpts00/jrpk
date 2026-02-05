use std::error::Error;
use crate::error::JrpkError;
use rskafka::record::Record;
use socket2::SockRef;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::io::Write;
use std::str::from_utf8;
use std::time::Duration;
use console::Term;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::runtime::{Builder, Runtime};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[macro_export]
macro_rules! async_clean_return {
    ($res:expr, $cleanup:expr) => {{
        match $res {
            Ok(x) => {
                x
            },
            Err(e1) => {
                $cleanup.unwrap();
                return Err(e1.into())
            }
        }
    }};
}

pub fn init_tracing() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer()
            .pretty()
            .with_file(false)
            .with_line_number(false)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .from_env()
                    .unwrap()
            )
        )
        .init();
}

#[derive(Debug)]
pub struct Ctx<C, T>(pub C, pub T);

impl <C, T> Ctx<C, T> {
    pub fn new(ctx: C, value: T) -> Self { Ctx(ctx, value) }
}

#[derive(Debug)]
pub struct Req<C, T, K> (pub Ctx<C, T>, pub Sender<K> );

pub async fn join_with_signal<F: Future>(f: F) {
    select! {
        _ = f => {
        },
        _ = tokio::signal::ctrl_c() => {
            info!("signal, exiting...");
        }
    }
}

pub async fn join_with_quit<F: Future>(f: F) {
    select! {
        _ = f => {
        },
        _ = quit() => {
        }
    }
}

pub fn debug_slice_u8(f: &mut Formatter<'_>, slice: &[u8]) -> std::fmt::Result {
    match from_utf8(slice) {
        Ok(s) => {
            f.write_str(s)
        }
        Err(_) => {
            f.write_str(&hex::encode(slice))
        }
    }
}

pub fn debug_vec_fn<T, F>(f: &mut Formatter<'_>, v: &Vec<T>, d: F) -> std::fmt::Result
    where F: Fn(&mut Formatter<'_>, &T) -> std::fmt::Result {
    write!(f, "[")?;
    if !v.is_empty() {
        let mut comma = false;
        for x in v.iter() {
            if comma {
                write!(f, ", ")?;
            }
            d(f, x)?;
            comma = true;
        }
    }
    write!(f, "]")
}

#[allow(dead_code)]
pub fn debug_vec<T: Display>(f: &mut Formatter<'_>, v: &Vec<T>) -> std::fmt::Result {
    debug_vec_fn(f, v, |f, x| x.fmt(f))
}

pub fn debug_record_and_offset(f: &mut Formatter<'_>, record: &Record, offset: Option<i64>) -> std::fmt::Result {
    write!(f, "Record {{ ")?;
    let mut comma = false;
    if let Some(o) = offset {
        write!(f, "offset: {}", o)?;
        comma = true;
    }
    if let Some(key) = record.key.as_ref() {
        if comma {
            write!(f, ", ")?;
        }
        write!(f, "key: ")?;
        debug_slice_u8(f, &key)?;
        comma = true;
    }
    if let Some(value) = record.value.as_ref() {
        if comma {
            write!(f, ", ")?;
        }
        write!(f, "value: ")?;
        debug_slice_u8(f, &value)?;
        comma = true;
    }
    if !record.headers.is_empty() {
        if comma {
            write!(f, ", ")?;
        }
        write!(f, "headers: {{ ")?;
        let mut comma2 = false;
        for (k, v) in record.headers.iter() {
            if comma2 {
                write!(f, ", ")?;
            }
            write!(f, "{}: ", k)?;
            debug_slice_u8(f, &v)?;
            comma2 = true;
        }
        write!(f, " }}")?;
        comma = true;
    }
    if comma {
        write!(f, ", ")?;
    }
    write!(f, "timestamp: {} }}", record.timestamp)
}

pub fn set_buf_sizes(stream: &TcpStream, recv: usize, send: usize) -> std::io::Result<()> {
    let socket = SockRef::from(&stream);
    socket.set_recv_buffer_size(recv)?;
    socket.set_send_buffer_size(send)?;
    Ok(())
}

pub struct CancellableHandle<T> {
    token: CancellationToken,
    handle: JoinHandle<T>,
}

impl <T> CancellableHandle<T> {
    pub fn new(token: CancellationToken, handle: JoinHandle<T>) -> Self {
        CancellableHandle { token, handle }
    }
    pub async fn cancel(self) -> Result<T, JoinError> {
        self.token.cancel();
        self.handle.await
    }
}

#[instrument(level="info", err)]
pub async fn quit() -> Result<(), std::io::Error> {
    tokio::task::spawn_blocking(|| {
        let term = Term::stdout();
        while term.read_char()? != 'q' {};
        Ok(())
    }).await?
}

pub fn make_runtime(threads: Option<usize>) -> std::io::Result<Runtime> {
    let mut builder = match threads {
        None => {
            Builder::new_multi_thread()
        },
        Some(1) => {
            Builder::new_current_thread()
        },
        Some(n) => {
            let mut b = Builder::new_multi_thread();
            b.worker_threads(n);
            b
        },
    };
    builder.enable_io().enable_time().build()
}

#[instrument(ret, err, skip(f))]
pub fn run<E, F>(threads: Option<usize>, f: F) -> Result<(), E>
where E: Error + From<std::io::Error>, F: Future<Output = Result<(), E>> {
    let runtime = make_runtime(threads)?;
    runtime.block_on(f)?;
    runtime.shutdown_timeout(Duration::from_secs(1));
    Ok(())
}

pub fn log<E: Error>(result: Result<(), E>) {
    if let Err(err) = result {
        error!("error: {}", err);
    }
}


#[inline]
pub fn json_to_writer<W: Write, S: Serialize>(writer: &mut W, json: &S) -> serde_json::Result<()> {
    serde_json::to_writer(writer, json)
}

pub trait Length {
    fn len(&self) -> usize;
}

pub trait Truncate {
    fn truncate(&mut self, len: usize);
}

#[derive(Debug)]
pub struct VecBufWriter<W> {
    buf: Vec<u8>,
    inner: W,
}

impl <W> VecBufWriter<W> {
    pub fn with_capacity(capacity: usize, writer: W) -> Self {
        VecBufWriter { buf: Vec::with_capacity(capacity), inner: writer }
    }
}

impl <W: Write> Write for VecBufWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            self.inner.write_all(self.buf.as_slice())?;
            self.buf.clear();
        }
        Ok(())
    }
}

impl <W> Length for VecBufWriter<W> {
    fn len(&self) -> usize {
        self.buf.len()
    }
}

impl <W> Truncate for VecBufWriter<W> {
    fn truncate(&mut self, len: usize) {
        self.buf.truncate(len)
    }
}

impl <T> Length for Vec<T> {
    fn len(&self) -> usize {
        self.len()
    }
}

impl <T> Truncate for Vec<T> {
    fn truncate(&mut self, len: usize) {
        self.truncate(len)
    }
}

#[derive(Debug)]
pub struct Budget {
    size: usize,
    count: usize,
}

impl Budget {

    pub fn new(size: usize, count: usize) -> Self {
        Budget { size, count }
    }

    fn spend(&mut self, size: usize) -> bool {
        if self.count > 0 && self.size >= size {
            self.size -= size;
            self.count -= 1;
            true
        } else {
            false
        }
    }

    pub fn write_ser<WLT, S>(&mut self, writer: &mut WLT, ser: &S) -> Result<bool, JrpkError>
    where WLT: Write + Length + Truncate, S: Serialize {
        let length = writer.len();
        json_to_writer(writer, ser)?;
        writer.write_all(b"\n")?;
        if self.spend(writer.len() - length) {
            Ok(true)
        } else {
            writer.truncate(length);
            Ok(false)
        }
    }

    pub fn write_slice<W>(&mut self, writer: &mut W, slice: &[u8]) -> Result<bool, JrpkError>
    where W: Write {
        if self.spend(slice.len() + 1) {
            writer.write_all(slice)?;
            writer.write_all(b"\n")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
