use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::sync::Arc;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use base64::DecodeError;
use hyper::http;
use hyper::http::uri::InvalidUri;
use strum::IntoStaticStr;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinError;
use tokio_util::codec::LinesCodecError;
use crate::kafka::KfkError;

#[derive(Error, Debug, IntoStaticStr)]
pub enum JrpkError {
    #[error("internal: {0}")]
    Internal(String),

    #[error("unexpected: {0}")]
    Unexpected(&'static str),

    #[error("syntax: {0}")]
    Syntax(&'static str),

    #[error("parse: {0}")]
    Parse(String),

    #[error("parse: {0}")]
    Strum(#[from] strum::ParseError),

    #[error("frame too big: {0}")]
    FrameTooBig(usize),

    #[error("codec: {0}")]
    Codec(#[from] LinesCodecError),
    
    #[error("utf8: {0}")]
    Utf8(#[from] Utf8Error),

    #[error("from utf8: {0}")]
    FromUtf8(#[from] FromUtf8Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::error::Error),

    #[error("bounded send: {0}")]
    BoundedSend(SendError<()>),

    #[error("oneshot send")]
    OneshotSend,

    #[error("oneshot receive")]
    OneshotReceive(#[from] RecvError),

    #[error("rs kafka: {0}")]
    Rs(#[from] KfkError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("base64: {0}")]
    Base64(#[from] DecodeError),

    #[error("join: {0}")]
    Join(#[from] JoinError),

    #[error("format: {0}")]
    Format(#[from] std::fmt::Error),

    #[error("http: {0}")]
    Http(#[from] http::Error),

    #[error("uri: {0}")]
    Uri(#[from] InvalidUri),

    #[error("wrapped: {0}")]
    Wrapped(#[from] Arc<JrpkError>),
    
    #[error("hyper: {0}")]
    Hyper(#[from] hyper::Error),

    #[error("reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("url")]
    Url,
}

/// deliberately drop payload
impl <T> From<SendError<T>> for JrpkError {
    fn from(_: SendError<T>) -> Self {
        JrpkError::BoundedSend(SendError(()))
    }
}

impl IntoResponse for JrpkError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
    }
}
