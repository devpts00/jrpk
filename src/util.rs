use rskafka::record::Record;
use socket2::SockRef;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::str::from_utf8;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct ResCtx<RSP, CTX, ERR: Error> {
    pub ctx: CTX,
    pub res: Result<RSP, ERR>,
}

impl <RSP, CTX, ERR: Error> ResCtx<RSP, CTX, ERR> {
    pub fn new(ctx: CTX, res: Result<RSP, ERR>) -> Self {
        Self { ctx, res }
    }
    pub fn ok(ctx: CTX, data: RSP) -> Self {
        ResCtx { ctx, res: Ok(data) }
    }
    pub fn err(ctx: CTX, err: ERR) -> Self {
        ResCtx { ctx, res: Err(err) }
    }
}

impl <RSP: Display, CTX: Display, ERR: Error> Display for ResCtx<RSP, CTX, ERR> {
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

pub struct ReqCtx<REQ, RSP, CTX, ERR: Error> {
    pub ctx: CTX,
    pub req: REQ,
    pub rsp_snd: Sender<ResCtx<RSP, CTX, ERR>>,
}

impl <REQ: Debug, RSP, CTX: Debug, ERR: Error> Debug for ReqCtx<REQ, RSP, CTX, ERR> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReqTag {{ ctx: {:?}, req: {:?} }}>", self.ctx, self.req)
    }
}

impl <REQ, RSP, CTX, ERR: Error> ReqCtx<REQ, RSP, CTX, ERR> {
    pub fn new(ctx: CTX, req: REQ, rsp_snd: Sender<ResCtx<RSP, CTX, ERR>>) -> Self {
        ReqCtx { ctx, req, rsp_snd }
    }
}

impl <REQ: Display, RSP, TAG: Display, ERR: Error> Display for ReqCtx<REQ, RSP, TAG, ERR> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "tag: {}, req: {}", self.ctx, self.req)
    }
}

// pub fn unwrap_err(ae: Arc<anyhow::Error>) -> anyhow::Error {
//     Arc::try_unwrap(ae).unwrap_or_else(|ae| {
//         anyhow!(ae.clone())
//     })
// }

pub fn handle_result<T, C, E> (name: &str, ctx: C, r: Result<T, E>) -> T
where T: Default + Debug, C: Display, E: Display {
    match r {
        Ok(value) => {
            info!("{}, ctx: {}, res: {:?} - END", name, ctx, value);
            value
        }
        Err(error) => {
            error!("{}, ctx: {}, err: '{}' - END", name, ctx, error);
            T::default()
        }
    }
}

pub async fn handle_future_result<T, E, C, F>(name: &str, ctx: C, future: F) -> T
where T: Debug + Default, E: Display, C: Display, F: Future<Output = Result<T, E>> {
    info!("{}, ctx: {} - START", name, ctx);
    handle_result(name, ctx, future.await)
}

pub async fn join_with_signal<T, C>(name: &str, ctx: C, jh: JoinHandle<T>) -> ()
where T: Default + Debug, C: Display {
    select! {
        res = jh => {
            handle_result(name, ctx, res);
        },
        _ = tokio::signal::ctrl_c() => {
            info!("{}, ctx: {} - signal, exiting...", name, ctx);
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
