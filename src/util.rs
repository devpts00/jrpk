use std::cmp::min;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::io::Read;
use std::ops::Index;
use std::str::{from_utf8, Utf8Error};
use std::sync::Arc;
use anyhow::{anyhow};
use rskafka::record::{Record, RecordAndOffset};
use substring::Substring;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tracing::{error, info};

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

impl <REQ, RES, E: Error> ReqId<REQ, RES, E> {
    pub fn new(id: usize, request: REQ, responses: Sender<ResId<RES, E>>) -> Self {
        ReqId { id, req: request, res_id_snd: responses }
    }
}

impl <REQ: Display, RSP, E: Error> Display for ReqId<REQ, RSP, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {}, {}", self.id, self.req)
    }
}

pub fn unwrap_err(ae: Arc<anyhow::Error>) -> anyhow::Error {
    Arc::try_unwrap(ae).unwrap_or_else(|ae| {
        anyhow!(ae.clone())
    })
}

pub fn handle_result<T: Default + Debug, E: Display> (r: Result<T, E>) -> T {
    match r {
        Ok(value) => {
            info!("done, success: {:?}", value);
            value
        }
        Err(error) => {
            error!("done, error: {}", error);
            T::default()
        }
    }
}

pub async fn handle_future<T: Debug + Default, E: Display, F: Future<Output = Result<T, E>>>(future: F) -> T {
    handle_result(future.await)
}

pub async fn join_with_signal<T: Default + Debug>(jh: JoinHandle<T>) -> () {
    select! {
        res = jh => {
            handle_result(res);
        },
        _ = tokio::signal::ctrl_c() => {
            info!("signal, exiting...");
        }
    }
}

pub fn display_slice_bytes(f: &mut Formatter<'_>, bs: &[u8]) -> std::fmt::Result {
    match from_utf8(bs) {
        Ok(s) => {
            write!(f, "{}...", s.substring(0, 5))
        }
        Err(_) => {
            let limit = min(bs.len(), 5);
            write!(f, "{:?}...", &bs[0..limit])
        }
    }
}

pub fn display_vec_fn<T, F>(f: &mut Formatter<'_>, v: &Vec<T>, d: F) -> std::fmt::Result
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

pub fn display_vec<T: Display>(f: &mut Formatter<'_>, v: &Vec<T>) -> std::fmt::Result {
    display_vec_fn(f, v, |f, x| x.fmt(f))
}

pub fn display_record(f: &mut Formatter<'_>, r: &Record, braces: bool) -> std::fmt::Result {
    if braces {
        write!(f, "{{ ")?;
    }
    let mut comma = false;
    if let Some(key) = r.key.as_ref() {
        display_slice_bytes(f, &key)?;
        comma = true;
    }
    if let Some(value) = r.value.as_ref() {
        if comma {
            write!(f, ", ")?;
        }
        display_slice_bytes(f, &value)?;
    }
    if !r.headers.is_empty() {
        if comma {
            write!(f, ", ")?;
        }
        write!(f, "headers: {{ ")?;
        let mut comma = false;
        for (k, v) in r.headers.iter() {
            if comma {
                comma = true;
            }
            write!(f, "{}: ", k)?;
            display_slice_bytes(f, &v)?;
        }
        write!(f, " }}")?;
    }
    if comma {
        write!(f, ", ")?;
    }
    write!(f, "timestamp: {}", r.timestamp)?;
    if braces {
        write!(f, " }}")
    } else {
        Ok(())
    }
}

pub fn display_rec_and_offset(f: &mut Formatter<'_>, ro: &RecordAndOffset) -> std::fmt::Result {
    write!(f, "{{ ")?;
    write!(f, "offset: {}, ", ro.offset)?;
    display_record(f, &ro.record, false)?;
    write!(f, " }}")
}

#[cfg(test)]
mod tests {
    use substring::Substring;

    #[test]
    fn test_display_bytes() {
        let s = "123";
        let ss = s.substring(0, 5);
        println!("{:?}", ss);
    }
}