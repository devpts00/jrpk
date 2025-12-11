use rskafka::record::Record;
use socket2::SockRef;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::str::from_utf8;
use faststr::FastStr;
use reqwest::Url;
use tokio::net::TcpStream;
use tokio::task::{JoinError, JoinHandle};
use tokio::select;
use tokio::sync::mpsc::error::SendError;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use crate::error::JrpkError;

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

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Tap {
    pub topic: FastStr,
    pub partition: i32
}

impl Tap {
    pub fn new<S: Into<FastStr>>(topic: S, partition: i32) -> Self {
        Tap { topic: topic.into(), partition }
    }
}

impl Display for Tap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.topic, self.partition)
    }
}

pub type BoundedSender<T> = tokio::sync::mpsc::Sender<T>;
pub type BoundedReceiver<T> = tokio::sync::mpsc::Receiver<T>;
pub type OneshotSender<T> = tokio::sync::oneshot::Sender<T>;
pub type OneshotReceiver<T> = tokio::sync::mpsc::Receiver<T>;

/*
pub enum Respond<T> {
    Bounded(BoundedSender<T>),
    Oneshot(OneshotSender<T>),
}

impl <T> Respond<T> {
    pub fn bounded(snd: tokio::sync::mpsc::Sender<T>) -> Self {
        Respond::Bounded(snd)
    }
    pub fn oneshot(snd: tokio::sync::oneshot::Sender<T>) -> Self {
        Respond::Oneshot(snd)
    }
    pub async fn respond(self, value: T) -> Result<(), SendError<T>> {
        match self {
            Respond::Bounded(snd) => {
                snd.send(value).await
            }
            Respond::Oneshot(snd) => {
                snd.send(value).map_err(|v| SendError(v))
            }
        }
    }
}
 */

#[derive(Debug)]
pub struct ResId<T, E> {
    pub id: usize,
    pub res: Result<T, E>,
}

impl <T, E> ResId<T, E> {
    pub fn new(id: usize, res: Result<T, E>) -> Self {
        ResId { id, res }
    }
    pub fn ok(id: usize, value: T) -> Self {
        ResId { id, res: Ok(value) }
    }
    pub fn err(err: E) -> Self {
        ResId { id: 0, res: Err(err) }
    }
}

impl <T: Display, E: Error> Display for ResId<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.res.as_ref() {
            Ok(value) => {
                write!(f, "id: {}, ok: {}", self.id, value)
            }
            Err(err) => {
                write!(f, "id: {}, err: {}", self.id, err)
            }
        }
    }
}

pub struct Req<T, U, E> {
    pub req: T,
    pub snd: OneshotSender<Result<U, E>>,
}

impl <T, U, E> Req<T, U, E> {
    pub fn new(req: T, snd: OneshotSender<Result<U, E>>) -> Self {
        Req { req, snd }
    }
}

pub struct ReqId<T, U, E> {
    pub id: usize,
    pub req: T,
    pub snd: BoundedSender<ResId<U, E>>,
}

impl <T, U, E> ReqId<T, U, E> {
    pub fn new(id: usize, req: T, snd: BoundedSender<ResId<U, E>>) -> Self {
        ReqId { id, req, snd }
    }
}

pub async fn join_with_signal<F: Future>(f: F) {
    select! {
        _ = f => {
        },
        _ = tokio::signal::ctrl_c() => {
            info!("signal, exiting...");
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
    socket.set_send_buffer_size(send)
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

#[inline]
pub fn url_append_tap(url: &mut Url, tap: &Tap) -> Result<(), JrpkError> {
    match url.path_segments_mut() {
        Ok(mut segments) => {
            segments.push("topic");
            segments.push(tap.topic.as_str());
            segments.push("partition");
            segments.push(tap.partition.to_string().as_str());
            Ok(())
        },
        Err(_) => {
            Err(JrpkError::Url)
        }
    }
}
