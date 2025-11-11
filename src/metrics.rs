use std::convert::Infallible;
use std::io::Read;
use std::mem::transmute;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::{BufMut, Bytes, BytesMut};
use futures::SinkExt;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::client::conn::http1::{handshake, SendRequest};
use hyper::{http, Method, Request};
use hyper::body::{Body, Frame};
use hyper::rt::Write;
use hyper_util::rt::TokioIo;
use log::error;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::{Registry, Unit};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::spawn;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::oneshot::Receiver;
use tracing::{info, instrument};
use crate::util::{log_result_handle, logf};

#[derive(Error, Debug)]
pub enum MetricsError {
    #[error("io: {0}")]
    IO(#[from] std::io::Error),
    #[error("hyper: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("format: {0}")]
    Format(#[from] std::fmt::Error),
    #[error("http: {0}")]
    Http(#[from] http::Error),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct Labels {
    mode: &'static str,
    command: &'static str,
    io: &'static str,
    traffic: &'static str,
}

impl Labels {
    pub fn new(mode: &'static str, command: &'static str, traffic: &'static str, io: &'static str) -> Self {
        Labels { mode, command, traffic, io }
    }
}

#[derive(Clone, Debug)]
pub struct Metrics {
    count: Family<Labels, Counter>,
    bytes: Family<Labels, Counter>,
}

impl Metrics {

    pub fn new(registry: &mut Registry) -> Self {
        let count = Family::<Labels, Counter>::default();
        registry.register("count", "io operation count", count.clone());
        let bytes = Family::<Labels, Counter>::default();
        registry.register_with_unit("bytes", "io operation bytes", Unit::Bytes, bytes.clone());
        Metrics { count, bytes }
    }

    fn counter(
        family: &Family<Labels, Counter>,
        mode: &'static str,
        command: &'static str,
        io: &'static str,
        traffic: &'static str
    ) -> Counter {
        let labels = Labels::new(mode, command, traffic, io);
        family.get_or_create_owned(&labels)
    }

    pub fn count(
        &self,
        mode: &'static str,
        command: &'static str,
        io: &'static str,
        traffic: &'static str
    ) -> Counter {
        Metrics::counter(&self.count, mode, command, io, traffic)
    }

    pub fn bytes(
        &self,
        mode: &'static str,
        command: &'static str,
        io: &'static str,
        traffic: &'static str
    ) -> Counter {
        Metrics::counter(&self.bytes, mode, command, io, traffic)
    }

}

struct RefBody {
    buf: &'static [u8]
}

impl RefBody {
    fn new(buf: &'static [u8]) -> Self {
        RefBody { buf }
    }
}

impl Body for RefBody {
    type Data = &'static [u8];
    type Error = Infallible;
    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(Some(Ok(Frame::data(self.buf))))
    }
}

async fn push(
    registry: &Registry,
    buf: &mut String,
    snd_req: &mut SendRequest<Full<Bytes>>
) -> Result<(), MetricsError> {
    info!("pushing metrics");
    buf.clear();

    let mut text = String::with_capacity(1024);
    encode(&mut text, registry)?;

    info!("metrics:\n{}", text.as_str());
    // ugly but the string will live long enough
    let bytes: &'static [u8] = unsafe { transmute(buf.as_bytes()) };
    let body = RefBody::new(bytes);

    let bb: Bytes = text.into();
    let full = Full::new(bb);

    let url: hyper::Uri = "http://pmg:9091/metrics/job/jrpk".parse().unwrap();
    let authority = url.authority().unwrap().clone();

    let req = hyper::Request::builder()
        .method(Method::POST)
        .header(hyper::header::HOST, authority.as_str())
        .uri(url)
        .body(full)?;
    let mut res = snd_req.send_request(req).await?;
    while let Some(res_frame) = res.frame().await {
        let frame = res_frame?;
        if let Some(chunk) = frame.data_ref() {
            info!("received data: {:?}", chunk);
        }
    }

    Ok(())
}

#[instrument(ret, skip(registry, buf, done_rcv, snd_req))]
async fn push_loop(
    registry: Registry,
    mut buf: String,
    mut done_rcv: Receiver<()>,
    mut snd_req: SendRequest<Full<Bytes>>
) -> Result<(), MetricsError> {
    info!("push loop");
    while let Err(TryRecvError::Empty) = done_rcv.try_recv() {
        push(&registry, &mut buf, &mut snd_req).await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    push(&registry, &mut buf, &mut snd_req).await?;
    Ok(())
}

#[instrument(ret, skip(registry, done_rcv))]
pub async fn prometheus_pushgateway(registry: Registry, done_rcv: Receiver<()>) -> Result<(), MetricsError> {
    // open tcp stream and convert it to hyper facade
    info!("connect to prometheus gateway");
    let tcp = TcpStream::connect("pmg:9091").await?;
    let io = TokioIo::new(tcp);
    let (snd_req, conn) = handshake(io).await?;
    // let tokio pump the bytes
    info!("spawn connection pump");
    let ch = spawn(async move {
        info!("connection pump");
        if let Err(err) = conn.await {
            error!("connection error: {}", err);
        }
    });
    // pass the requests to the sender entry point
    info!("spawn push loop");
    let buf = String::with_capacity(1024);
    let ph = spawn(async move {
        push_loop(registry, buf, done_rcv, snd_req).await
    });
    ch.await;
    ph.await;
    Ok(())
}
