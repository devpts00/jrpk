use std::fmt::{Debug, Display};
use std::future::Future;
use std::sync::Arc;
use anyhow::{anyhow, Error};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tracing::{error, info};

pub type Response<U, E> = (usize, Result<U, E>);

pub type Request<T, U, E> = (usize, T, Sender<Response<U, E>>);

pub fn unwrap_err(ae: Arc<Error>) -> Error {
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