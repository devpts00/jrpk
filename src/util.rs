use std::fmt::{Debug, Display};
use tokio::select;
use tokio::task::JoinHandle;
use tracing::{error, info};

pub fn handle<T: Default + Debug, E: Display> (r: Result<T, E>) -> T {
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

pub async fn join_with_signal<T: Default + Debug>(jh: JoinHandle<T>) -> () {
    select! {
        res = jh => {
            handle(res);
        },
        _ = tokio::signal::ctrl_c() => {
            info!("signal, exiting...");
        }
    }
}