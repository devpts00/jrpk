use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use faststr::FastStr;
use hyper::StatusCode;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{exponential_buckets, linear_buckets, Histogram};
use prometheus_client::registry::{Registry, Unit};
use reqwest::{Client, Url};
use reqwest::header::HOST;
use tokio::spawn;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace, warn};
use crate::error::JrpkError;
use crate::model::JrpMethod;
use crate::size::Size;
use crate::util::{CancellableHandle, Tap};

pub static IO_OP_THROUGHPUT: &str = "io_op_throughput";
pub static IO_OP_SIZE: &str = "io_op_size";
pub static IO_OP_LATENCY: &str = "io_op_latency";

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
    Kafka,
    Jsonrpc,
    Http,
    Client
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
pub struct JrpkLabels {
    tier: LblTier,
    method: Option<LblMethod>,
    traffic: Option<LblTraffic>,
    topic: Option<FastStrExt>,
    partition: Option<i32>,
}

impl JrpkLabels {
    pub fn new(tier: LblTier) -> Self {
        JrpkLabels { tier, method: None, traffic: None, topic: None, partition: None }
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

pub struct JrpkMetrics {
    registry: Mutex<Registry>,
    throughputs: Family<JrpkLabels, Counter>,
    sizes: Family<JrpkLabels, Histogram>,
    latencies: Family<JrpkLabels, Histogram>,
}

impl JrpkMetrics {

    fn register(
        registry: &Mutex<Registry>,
        throughputs: Family<JrpkLabels, Counter>,
        sizes: Family<JrpkLabels, Histogram>,
        latencies: Family<JrpkLabels, Histogram>
    ) {
        let mut registry = registry.lock().unwrap();
        registry.register_with_unit(IO_OP_THROUGHPUT, "i/o operation throughput", Unit::Bytes, throughputs);
        registry.register_with_unit(IO_OP_SIZE, "i/o operation size", Unit::Bytes, sizes);
        registry.register_with_unit(IO_OP_LATENCY, "i/o operation latency", Unit::Seconds, latencies);
    }

    pub fn new() -> Self {
        let registry = Mutex::new(Registry::default());
        let throughputs = Family::<JrpkLabels, Counter>::default();
        let sizes = Family::<JrpkLabels, Histogram>::new_with_constructor(|| { Histogram::new(exponential_buckets(1024.0, 1.6, 20)) });
        let latencies = Family::<JrpkLabels, Histogram>::new_with_constructor(|| { Histogram::new(exponential_buckets(0.001, 2.0, 20)) });
        JrpkMetrics::register(&registry, throughputs.clone(), sizes.clone(), latencies.clone());
        JrpkMetrics { registry, throughputs, sizes, latencies }
    }

    pub fn size_by_value(&self, labels: &JrpkLabels, value: usize)  {
        let size = value as u64;
        self.throughputs.get_or_create(labels).inc_by(size);
        self.sizes.get_or_create(labels).observe(size as f64);
    }

    pub fn size<S: Size>(&self, labels: &JrpkLabels, data: &S) {
        self.size_by_value(labels, data.size())
    }

    pub fn time(&self, labels: &JrpkLabels, since: Instant) {
        self.latencies.get_or_create(labels).observe(since.elapsed().as_secs_f64());
    }

    pub fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut buf = String::with_capacity(64 * 1024);
        // TODO: see if we can handle 
        let registry = self.registry.lock().unwrap();
        encode(&mut buf, &registry)?;
        trace!("metrics:\n{}", buf);
        Ok(buf)
    }
}

pub struct MeteredItem<T> {
    pub item: T,
    pub metrics: Arc<JrpkMetrics>,
    pub labels: JrpkLabels,
}

impl <T> MeteredItem<T> {
    pub fn new(item: T, metrics: Arc<JrpkMetrics>, labels: JrpkLabels) -> Self {
        MeteredItem { item, metrics, labels }
    }
}

#[instrument(level="debug", ret, err, skip(url, cli, metrics))]
async fn push_prometheus(
    url: Url,
    auth: FastStr,
    cli: &Client,
    metrics: Arc<JrpkMetrics>,
) -> Result<(), JrpkError> {

    let buf = metrics.encode()?;
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

#[instrument(ret, err, skip(url, metrics, cancel))]
async fn loop_push_prometheus(
    url: Url,
    period: Duration,
    metrics: Arc<JrpkMetrics>,
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
                push_prometheus(url.clone(), auth.clone(), &cli, metrics.clone()).await?;
            }
        }
    }
    push_prometheus(url.clone(), auth.clone(), &cli, metrics.clone()).await?;
    Ok(())
}

#[instrument(skip(url, metrics))]
pub fn spawn_push_prometheus(
    url: Url,
    period: Duration,
    metrics: Arc<JrpkMetrics>,
) -> CancellableHandle<Result<(), JrpkError>> {
    let cancel = CancellationToken::new();
    let handle = spawn(loop_push_prometheus(url, period, metrics, cancel.clone()));
    CancellableHandle::new(cancel, handle)
}
