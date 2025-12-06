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
        write!(f, "{}@{}", self.topic, self.partition)
    }
}

#[derive(Debug)]
pub struct ResCtx<RSP, ERR: Error, CTX> {
    pub res: Result<RSP, ERR>,
    pub ctx: CTX,
}

impl <RSP, CTX, ERR: Error> ResCtx<RSP, ERR, CTX> {
    pub fn new(res: Result<RSP, ERR>, ctx: CTX) -> Self {
        ResCtx { res, ctx }
    }
    pub fn ok(data: RSP, ctx: CTX) -> Self {
        ResCtx::new(Ok(data), ctx)
    }
    pub fn err(err: ERR, ctx: CTX) -> Self {
        ResCtx::new(Err(err), ctx)
    }
}

impl <RSP: Display, ERR: Error, CTX: Display> Display for ResCtx<RSP, ERR, CTX> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.res.as_ref() {
            Ok(rsp) => {
                write!(f, "tag: {}: {}", self.ctx, rsp)
            }
            Err(err) => {
                write!(f, "tag: {}: {}", self.ctx, err)
            }
        }
    }
}

//#[derive(Clone)]
pub enum Respond123<T> {
    Bounded(tokio::sync::mpsc::Sender<T>),
    Oneshot(tokio::sync::oneshot::Sender<T>),
}

impl <T> Respond123<T> {
    pub fn bounded(snd: tokio::sync::mpsc::Sender<T>) -> Self {
        Respond123::Bounded(snd)
    }
    pub fn oneshot(snd: tokio::sync::oneshot::Sender<T>) -> Self {
        Respond123::Oneshot(snd)
    }
    pub async fn respond(self, value: T) -> Result<(), SendError<T>> {
        match self {
            Respond123::Bounded(snd) => {
                snd.send(value).await
            }
            Respond123::Oneshot(snd) => {
                snd.send(value).map_err(|v| SendError(v))
            }
        }
    }
}

pub type BoundedSnd<T> = tokio::sync::mpsc::Sender<T>;
pub type OneshotSnd<T> = tokio::sync::oneshot::Sender<T>;

pub struct ReqCtx<REQ, RSP, ERR: Error, CTX> {
    pub req: REQ,
    pub ctx: CTX,
    pub res_rsp_ctx_snd: tokio::sync::mpsc::Sender<ResCtx<RSP, ERR, CTX>>,
}

impl <REQ: Debug, RSP, ERR: Error, CTX: Debug> Debug for ReqCtx<REQ, RSP, ERR, CTX> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReqTag {{ ctx: {:?}, req: {:?} }}>", self.ctx, self.req)
    }
}

impl <REQ, RSP, ERR: Error, CTX> ReqCtx<REQ, RSP, ERR, CTX> {
    pub fn new(req: REQ, ctx: CTX, res_rsp_ctx_snd: BoundedSnd<ResCtx<RSP, ERR, CTX>>) -> Self {
        ReqCtx { req, ctx, res_rsp_ctx_snd }
    }
}

impl <REQ: Display, RSP, ERR: Error, CTX: Display> Display for ReqCtx<REQ, RSP, ERR, CTX> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ctx: {}, req: {}", self.ctx, self.req)
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
