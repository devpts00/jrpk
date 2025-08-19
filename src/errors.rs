use std::string::FromUtf8Error;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

#[derive(Error, Debug)]
pub enum JrpkError {

    #[error("general: {0}")]
    General(&'static str),

    #[error("syntax: {0}")]
    Syntax(&'static str),

    #[error("parse: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("join: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("json: {0}")]
    Json(#[from] serde_json::error::Error),

    #[error("utf8: {0}")]
    UTF8(#[from] FromUtf8Error),

    #[error("send: {0}")]
    Send(tokio::sync::mpsc::error::SendError<()>),

    #[error("kafka: {0}")]
    Kafka(#[from] rskafka::client::error::Error),

    #[error("{0}")]
    Wrapped(#[from] Arc<JrpkError>),
}

pub type JrpkResult<T> = Result<T, JrpkError>;

/// deliberately drop payload
impl <T> From<tokio::sync::mpsc::error::SendError<T>> for JrpkError {
    fn from(value: SendError<T>) -> Self {
        JrpkError::Send(tokio::sync::mpsc::error::SendError(()))
    }
}
