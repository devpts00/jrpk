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
pub struct ResId<RSP, E: Error> {
    pub id: usize,
    pub res: Result<RSP, E>,
}

impl <RSP, E: Error> ResId<RSP, E> {
    pub fn new(id: usize, result: Result<RSP, E>) -> Self {
        Self { id, res: result }
    }
    pub fn ok(id: usize, data: RSP) -> Self {
        ResId { id, res: Ok(data) }
    }
    pub fn err(id: usize, err: E) -> Self {
        ResId { id, res: Err(err) }
    }
}

impl <RSP: Display, E: Error> Display for ResId<RSP, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.res {
            Ok(ref rsp) => {
                write!(f, "id: {}: {}", self.id, rsp)
            }
            Err(ref err) => {
                write!(f, "id: {}: {}", self.id, err)
            }
        }
    }
}

pub struct ReqId<REQ, RSP, E: Error> {
    pub id: usize,
    pub req: REQ,
    pub res_id_snd: Sender<ResId<RSP, E>>,
}

impl <REQ: Debug, RSP, E: Error> Debug for ReqId<REQ, RSP, E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReqId {{ id: {:?}, req: {:?} }}>", self.id, self.req)
    }
}

impl <REQ, RES, E: Error> ReqId<REQ, RES, E> {
    pub fn new(id: usize, request: REQ, responses: Sender<ResId<RES, E>>) -> Self {
        ReqId { id, req: request, res_id_snd: responses }
    }
}

impl <REQ: Display, RSP, E: Error> Display for ReqId<REQ, RSP, E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {}, {}", self.id, self.req)
    }
}

// pub fn unwrap_err(ae: Arc<anyhow::Error>) -> anyhow::Error {
//     Arc::try_unwrap(ae).unwrap_or_else(|ae| {
//         anyhow!(ae.clone())
//     })
// }

pub fn handle_result<T: Default + Debug, E: Display> (ctx: &str, r: Result<T, E>) -> T {
    match r {
        Ok(value) => {
            debug!("{}, success: {:?}", &ctx, value);
            value
        }
        Err(error) => {
            error!("{}, error: {}", &ctx, error);
            T::default()
        }
    }
}

pub async fn handle_future<T: Debug + Default, E: Display, F: Future<Output = Result<T, E>>>(ctx: &str, future: F) -> T {
    handle_result(ctx, future.await)
}

pub async fn join_with_signal<T: Default + Debug>(ctx: &str, jh: JoinHandle<T>) -> () {
    select! {
        res = jh => {
            handle_result(ctx, res);
        },
        _ = tokio::signal::ctrl_c() => {
            info!("{} - signal, exiting...", &ctx);
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
