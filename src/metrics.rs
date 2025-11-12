use std::io::{Read, Write};
use std::convert::Infallible;
use std::time::Duration;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::client::conn::http1::{handshake, SendRequest};
use hyper::{http, Method, Uri};
use hyper::http::uri::{Authority, InvalidUri};
use hyper_util::rt::TokioIo;
use log::error;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::{Registry, Unit};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::{spawn, task_local};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::oneshot::Receiver;
use tracing::{debug, info, instrument, warn};
use crate::util::logh;

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
    #[error("uri: {0}")]
    Uri(#[from] InvalidUri)
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

#[instrument(level="debug", ret, skip(registry, snd_req))]
async fn push(
    registry: &Registry,
    uri: &Uri,
    auth: &Authority,
    snd_req: &mut SendRequest<Full<Bytes>>
) -> Result<(), MetricsError> {
    let mut buf = String::with_capacity(1024);
    encode(&mut buf, registry)?;
    let req = hyper::Request::builder()
        .method(Method::POST)
        .header(hyper::header::HOST, auth.as_str())
        .uri(uri)
        .body(Full::new(buf.into()))?;
    let mut res = snd_req.send_request(req).await?;
    if res.status() == hyper::StatusCode::OK {
        debug!("status: {}", res.status());
    } else {
        warn!("status: {}", res.status());
    }
    while let Some(res_frame) = res.frame().await {
        let frame = res_frame?;
        if let Some(chunk) = frame.data_ref() {
            debug!("data: {:?}", chunk);
        }
    }
    Ok(())
}

#[instrument(ret, skip(registry, done_rcv, snd_req))]
async fn push_loop(
    registry: Registry,
    period: Duration,
    uri: Uri,
    auth: Authority,
    mut done_rcv: Receiver<()>,
    mut snd_req: SendRequest<Full<Bytes>>
) -> Result<(), MetricsError> {
    while let Err(TryRecvError::Empty) = done_rcv.try_recv() {
        push(&registry, &uri, &auth, &mut snd_req).await?;
        tokio::time::sleep(period).await;
    }
    push(&registry, &uri, &auth, &mut snd_req).await?;
    Ok(())
}

#[instrument(ret, skip(registry, done_rcv))]
pub async fn prometheus_pushgateway(
    address: String,
    period: Duration,
    registry: Registry,
    done_rcv: Receiver<()>
) -> Result<(), MetricsError> {
    // open tcp stream and convert it to hyper facade
    let auth: Authority = address.parse()?;
    let uri = Uri::builder()
        .scheme("http")
        .authority(auth.clone())
        .path_and_query("/metrics/job/jrpk")
        .build()?;
    let tcp = TcpStream::connect(address).await?;
    let io = TokioIo::new(tcp);
    let (snd_req, conn) = handshake(io).await?;
    // let tokio pump the bytes
    let ch = spawn(async move {
        if let Err(err) = conn.await {
            error!("connection error: {}", err);
        }
        Result::<(), Infallible>::Ok(())
    });
    // pass the requests to the sender entry point
    let ph = spawn(
        push_loop(registry, period, uri, auth, done_rcv, snd_req)
    );

    logh("prometheus-pushgateway, connection", ch).await;
    logh("prometheus-pushgateway, pusher", ph).await;

    Ok(())
}
