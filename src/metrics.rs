use std::convert::Infallible;
use std::time::Duration;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::{handshake, Connection, SendRequest};
use hyper::{Method, Uri};
use hyper::http::uri::Authority;
use hyper_util::rt::TokioIo;
use log::error;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::{Registry, Unit};
use tokio::net::TcpStream;
use tokio::{spawn, try_join};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};
use crate::codec::Meter;
use crate::error::JrpkError;
use crate::util::CancellableHandle;

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

#[derive(Clone)]
pub struct ByteMeter {
    count: Counter,
    bytes: Counter,
}

impl Meter for ByteMeter {
    fn meter(&self, length: usize) {
        self.count.inc();
        self.bytes.inc_by(length as u64);
    }
}

#[derive(Clone, Debug)]
pub struct ByteMeters {
    count: Family<Labels, Counter>,
    bytes: Family<Labels, Counter>,
}

impl ByteMeters {
    pub fn new(registry: &mut Registry) -> Self {
        let count = Family::<Labels, Counter>::default();
        registry.register("io_op_count", "io operation count", count.clone());
        let bytes = Family::<Labels, Counter>::default();
        registry.register_with_unit("io_op_volume", "io operation volume", Unit::Bytes, bytes.clone());
        ByteMeters { count, bytes }
    }

    pub fn meter(
        &self,
        mode: &'static str,
        command: &'static str,
        io: &'static str,
        traffic: &'static str
    ) -> ByteMeter {
        let labels = Labels::new(mode, command, traffic, io);
        let count = self.count.get_or_create_owned(&labels);
        let bytes = self.bytes.get_or_create_owned(&labels);
        ByteMeter { count, bytes }
    }
    
}

#[instrument(level="debug", ret, err, skip(registry, snd_req))]
async fn push(
    uri: &Uri,
    auth: &Authority,
    registry: &Registry,
    snd_req: &mut SendRequest<Full<Bytes>>
) -> Result<(), JrpkError> {
    let mut buf = String::with_capacity(16 * 1024);
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

#[instrument(ret, err, skip(registry, cancel, snd_req))]
async fn push_loop(
    uri: Uri,
    auth: Authority,
    period: Duration,
    registry: Registry,
    cancel: CancellationToken,
    mut snd_req: SendRequest<Full<Bytes>>
) -> Result<(), JrpkError> {
    while !cancel.is_cancelled() {
        push(&uri, &auth, &registry, &mut snd_req).await?;
        tokio::time::sleep(period).await;
    }
    push(&uri, &auth, &registry, &mut snd_req).await?;
    Ok(())
}

#[instrument(ret, err, skip(conn))]
async fn conn_loop(conn: Connection<TokioIo<TcpStream>, Full<Bytes>>) -> Result<(), Infallible> {
    if let Err(err) = conn.await {
        error!("connection error: {}", err);
    }
    Ok(())
}

#[instrument(ret, err, skip(registry, cancel))]
async fn prometheus_pushgateway(
    uri: Uri,
    period: Duration,
    registry: Registry,
    cancel: CancellationToken,
) -> Result<(), JrpkError> {
    let auth = uri.authority().ok_or(JrpkError::Unexpected("URI must have an authority"))?.clone();
    let tcp = TcpStream::connect(auth.as_str()).await?;
    let io = TokioIo::new(tcp);
    let (snd_req, conn) = handshake(io).await?;
    let ch = spawn(conn_loop(conn));
    let ph = spawn(push_loop(uri, auth, period, registry, cancel, snd_req));
    let _ = try_join!(ch, ph)?;
    Ok(())
}

pub fn spawn_prometheus_gateway(
    uri: Uri,
    period: Duration,
    registry: Registry,
) -> CancellableHandle<Result<(), JrpkError>> {
    let token = CancellationToken::new();
    let handle = spawn(prometheus_pushgateway(uri, period, registry, token.clone()));
    CancellableHandle::new(token, handle)
}
