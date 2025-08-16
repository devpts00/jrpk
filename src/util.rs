use std::error::Error;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::sync::Arc;
use anyhow::{anyhow};
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
