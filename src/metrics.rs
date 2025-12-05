use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use faststr::FastStr;
use hyper::StatusCode;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::{Registry, Unit};
use reqwest::{Client, Url};
use reqwest::header::HOST;
use tokio::spawn;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace, warn};
use crate::error::JrpkError;
use crate::model::JrpMethod;
use crate::util::{CancellableHandle, Tap};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct FastStrExt(FastStr);

impl From<FastStr> for FastStrExt {
    fn from(s: FastStr) -> Self {
        FastStrExt(s)
    }
}

impl EncodeLabelValue for FastStrExt {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        EncodeLabelValue::encode(&self.0.as_str(), encoder)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum LblTier {
    Kafka, Server, Client
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum LblMethod {
    Send, Fetch, Offset
}

impl From<JrpMethod> for LblMethod {
    fn from(value: JrpMethod) -> Self {
        match value {
            JrpMethod::Send => LblMethod::Send,
            JrpMethod::Fetch => LblMethod::Fetch,
            JrpMethod::Offset => LblMethod::Offset
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum LblTraffic {
    In, Out
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct Labels {
    tier: LblTier,
    method: Option<LblMethod>,
    traffic: Option<LblTraffic>,
    topic: Option<FastStrExt>,
    partition: Option<i32>,
}

impl Labels {
    pub fn new(tier: LblTier) -> Self {
        Labels { tier, method: None, traffic: None, topic: None, partition: None }
    }
    pub fn method<M: Into<LblMethod>>(&mut self, method: M) -> &mut Self {
        self.method = Some(method.into());
        self
    }
    pub fn traffic(&mut self, traffic: LblTraffic) -> &mut Self {
        self.traffic = Some(traffic);
        self
    }
    pub fn tap(&mut self, tap: Tap) -> &mut Self {
        self.topic = Some(tap.topic.into());
        self.partition = Some(tap.partition);
        self
    }
    pub fn build(&self) -> Self {
        self.clone()
    }
}

#[derive(Clone, Debug)]
pub struct JrpkMetrics {
    pub throughputs: Family<Labels, Counter>,
    pub latencies: Family<Labels, Histogram>,
}

impl JrpkMetrics {
    pub fn new(registry: Arc<Mutex<Registry>>) -> Self {
        let mut r = registry.lock().unwrap();
        let throughputs = Family::<Labels, Counter>::default();
        let x = throughputs.clone();
        r.register_with_unit(IO_OP_THROUGHPUT, "i/o operation throughput", Unit::Bytes, x);
        let latencies = Family::<Labels, Histogram>::new_with_constructor(|| { Histogram::new(exponential_buckets(0.001, 2.0, 20)) });
        r.register_with_unit(IO_OP_LATENCY, "i/o operation latency", Unit::Seconds, latencies.clone());
        JrpkMetrics { throughputs, latencies }
    }
}

pub static IO_OP_THROUGHPUT: &str = "io_op_throughput";
pub static IO_OP_LATENCY: &str = "io_op_latency";

pub fn encode_registry(registry: Arc<Mutex<Registry>>) -> Result<String, std::fmt::Error> {
    let mut buf = String::with_capacity(64 * 1024);
    let rg = registry.lock().unwrap();
    encode(&mut buf, &rg)?;
    trace!("metrics:\n{}", buf);
    Ok(buf)
}

#[instrument(level="debug", ret, err, skip(url, cli, registry))]
async fn push_prometheus(
    url: Url,
    auth: FastStr,
    cli: &Client,
    registry: Arc<Mutex<Registry>>
) -> Result<(), JrpkError> {

    let buf = encode_registry(registry)?;
    let res = cli.post(url)
        .header(HOST, auth.as_str())
        .body(buf)
        .send()
        .await?;

    match res.status() {
        StatusCode::OK => {
            debug!("status: {}", StatusCode::OK);
        }
        status => {
            warn!("status: {}", status);
        }
    }

    Ok(())
}

#[instrument(ret, err, skip(url, registry, cancel))]
async fn loop_push_prometheus(
    url: Url,
    period: Duration,
    registry: Arc<Mutex<Registry>>,
    cancel: CancellationToken,
) -> Result<(), JrpkError> {
    let cli = Client::new();
    let auth: FastStr = url.authority().to_owned().into();
    loop {
        match tokio::time::timeout(period, cancel.cancelled()).await {
            Ok(_) => {
                break
            },
            Err(_) => {
                push_prometheus(url.clone(), auth.clone(), &cli, registry.clone()).await?;
            }
        }
    }
    push_prometheus(url.clone(), auth.clone(), &cli, registry).await?;
    Ok(())
}

#[instrument(skip(url, registry))]
pub fn spawn_push_prometheus(
    url: Url,
    period: Duration,
    registry: Arc<Mutex<Registry>>,
) -> CancellableHandle<Result<(), JrpkError>> {
    let cancel = CancellationToken::new();
    let handle = spawn(loop_push_prometheus(url, period, registry, cancel.clone()));
    CancellableHandle::new(cancel, handle)
}
