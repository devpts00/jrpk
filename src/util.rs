use rskafka::record::Record;
use socket2::SockRef;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::str::from_utf8;
use faststr::FastStr;
use reqwest::Url;
use tokio::net::TcpStream;
use tokio::task::{JoinError, JoinHandle};
use tokio::select;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
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

pub struct Id<T>(pub usize, pub T);

impl <T> Id<T> {
    pub fn new(id: usize, value: T) -> Self {
        Id(id, value)
    }
}

pub enum KfkPayload<S, F, O> {
    Send(S),
    Fetch(F),
    Offset(O),
}

pub struct Ctx<C, T>(pub C, pub T);

impl <C, T> Ctx<C, T> {
    pub fn new(ctx: C, value: T) -> Self { Ctx(ctx, value) }
}

pub struct Response<T, U, E> {
    snd: Sender<Id<Result<U, E>>>,
    _phantom: PhantomData<T>
}

impl <T, U, E> Response<T, U, E> where U: From<T> {
    pub fn new (snd: Sender<Id<Result<U, E>>>) -> Self {
        Response { snd, _phantom: PhantomData }
    }
    pub async fn send(self, id: usize, res: Result<T, E>) -> Result<(), SendError<Id<Result<U, E>>>> {
        self.snd.send(Id(id, res.map(|r|r.into()))).await
    }
}

pub struct Request<T, U, W: From<U>, E> ( pub Id<T>, pub Response<U, W, E> );

impl <T, U, W, E> Request<T, U, W, E> where W: From<U> {
    pub fn new(id: usize, value: T, snd: Sender<Id<Result<W, E>>>) -> Self {
        Request(Id::new(id, value), Response::new(snd))
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
