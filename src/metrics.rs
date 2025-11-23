use std::convert::Infallible;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::{handshake, Connection, SendRequest};
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper::body::{Body, Incoming};
use hyper::header::CONTENT_TYPE;
use hyper::http::uri::Authority;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper::server::conn::http1;
use log::error;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::{Registry, Unit};
use tokio::net::{TcpListener, TcpStream};
use tokio::{spawn, try_join};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, trace, warn};
use crate::error::JrpkError;
use crate::util::CancellableHandle;

pub trait Meter {
    fn meter(&self, bytes: usize, duration: Option<Instant>);
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct JrpkLabels {
    mode: &'static str,
    command: &'static str,
    io: &'static str,
    traffic: &'static str,
}

impl JrpkLabels {
    pub fn new(mode: &'static str, command: &'static str, traffic: &'static str, io: &'static str) -> Self {
        JrpkLabels { mode, command, traffic, io }
    }
}

#[derive(Clone)]
pub struct JrpkMeter {
    count: Counter,
    bytes: Counter,
    times: Histogram
}

impl Meter for JrpkMeter {
    fn meter(&self, bytes: usize, timestamp: Option<Instant>) {
        self.count.inc();
        self.bytes.inc_by(bytes as u64);
        if let Some(timestamp) = timestamp {
            self.times.observe(timestamp.elapsed().as_secs_f64());
        }
    }
}

#[derive(Clone, Debug)]
pub struct JrpkMeters {
    count: Family<JrpkLabels, Counter>,
    bytes: Family<JrpkLabels, Counter>,
    times: Family<JrpkLabels, Histogram>,
}

impl JrpkMeters {
    pub fn new(registry: &mut Registry) -> Self {
        let count = Family::<JrpkLabels, Counter>::default();
        registry.register("io_op_count", "io operation count", count.clone());
        let bytes = Family::<JrpkLabels, Counter>::default();
        registry.register_with_unit("io_op_volume", "io operation volume", Unit::Bytes, bytes.clone());
        let times = Family::<JrpkLabels, Histogram>::new_with_constructor(|| { Histogram::new(exponential_buckets(0.000001, 2.0, 20)) });
        registry.register_with_unit("io_op_duration", "io operation duration", Unit::Seconds, times.clone());
        JrpkMeters { count, bytes, times }
    }

    pub fn meter(
        &self,
        mode: &'static str,
        command: &'static str,
        io: &'static str,
        traffic: &'static str
    ) -> JrpkMeter {
        let labels = JrpkLabels::new(mode, command, traffic, io);
        let count = self.count.get_or_create_owned(&labels);
        let bytes = self.bytes.get_or_create_owned(&labels);
        let times = self.times.get_or_create_owned(&labels);
        JrpkMeter { count, bytes, times }
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
async fn push_prometheus(
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

pub fn spawn_push_prometheus(
    uri: Uri,
    period: Duration,
    registry: Registry,
) -> CancellableHandle<Result<(), JrpkError>> {
    let token = CancellationToken::new();
    let handle = spawn(push_prometheus(uri, period, registry, token.clone()));
    CancellableHandle::new(token, handle)
}

async fn encode_registry(registry: Arc<Mutex<Registry>>) -> Result<Bytes, std::fmt::Error> {
    let mut buf = String::with_capacity(64 * 1024);
    let rg = registry.lock().unwrap();
    encode(&mut buf, &rg)?;
    trace!("metrics:\n{}", buf);
    Ok(buf.into())
}

#[inline]
fn rsp<D, B>(data: D, status: StatusCode) -> Result<Response<B>, hyper::http::Error>
where B: Body + From<D> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(B::from(data))
}

#[inline]
fn res2rsp<D, B, E>(res: Result<D, E>) -> Result<Response<B>, hyper::http::Error>
where B: Body + Default + From<D>, E: Error {
    match res {
        Ok(data) => {
            rsp(data, StatusCode::OK)
        },
        Err(err) => {
            error!("error: {}", err);
            rsp(B::default(), StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[instrument(level="debug", ret, err, skip(reg))]
async fn serve_prometheus(req: Request<Incoming>, reg: Arc<Mutex<Registry>>) -> Result<Response<Full<Bytes>>, hyper::http::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            res2rsp(encode_registry(reg).await)
        },
        (method, uri) => {
            warn!("unexpected, method: {}, uri: {}", method, uri);
            rsp(Full::default(), StatusCode::NOT_FOUND)
        }
    }
}

#[instrument(ret, err, skip(registry))]
pub async fn listen_prometheus(bind_addr: SocketAddr, registry: Arc<Mutex<Registry>>) -> Result<(), JrpkError> {
    let listener = TcpListener::bind(bind_addr).await?;
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        info!("accepted: {}", peer_addr);
        // 1. clone for every service fn
        let registry = registry.clone();
        spawn(
            http1::Builder::new().serve_connection(
                TokioIo::new(stream),
                service_fn(move |req| {
                    // 2. clone for every future
                    serve_prometheus(req, registry.clone())
                })
            )
        );
    }
}
