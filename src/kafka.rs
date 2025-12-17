use std::error::Error;
use crate::error::JrpkError;
use crate::util::{Id, Request, Tap};
use moka::future::Cache;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::{Record, RecordAndOffset};
use std::future::Future;
use std::ops::Range;
use std::time::Instant;
use log::info;
use tokio::{spawn};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{instrument, trace};
use crate::metrics::{JrpkMetrics, Labels, LblMethod, LblTier, LblTraffic};
use crate::size::Size;

pub enum KfkOffset {
    At(OffsetAt),
    Pos(i64)
}

#[derive(Debug)]
pub enum KfkOut {
    Send(Vec<i64>),
    Fetch(Vec<RecordAndOffset>, i64),
    Offset(i64),
}

impl From<Vec<i64>> for KfkOut {
    fn from(value: Vec<i64>) -> Self {
        KfkOut::Send(value)
    }
}

impl From<(Vec<RecordAndOffset>, i64)> for KfkOut {
    fn from(value: (Vec<RecordAndOffset>, i64)) -> Self {
        let (records_and_offsets, high_watermark) = value;
        KfkOut::Fetch(records_and_offsets, high_watermark)
    }
}
impl From<i64> for KfkOut {
    fn from(value: i64) -> Self {
        KfkOut::Offset(value)
    }
}

pub enum KfkReq {
    Send(Request<Vec<Record>, Vec<i64>, KfkOut, JrpkError>),
    Fetch(Request<(KfkOffset, Range<i32>, i32), (Vec<RecordAndOffset>, i64), KfkOut, JrpkError>),
    Offset(Request<KfkOffset, i64, KfkOut, JrpkError>),
}

pub type RsKafkaError = rskafka::client::error::Error;

/// Helper function to encapsulate cumbersome metrics manipulations
async fn meter<IN, OUT, ERR, FUT, FUN>(
    input: IN,
    metrics: &JrpkMetrics,
    labels: &mut Labels,
    func: FUN) -> Result<OUT, JrpkError>
where
    IN: Size,
    OUT: Size,
    ERR: Error + Into<JrpkError>,
    FUT: Future<Output=Result<OUT, ERR>>,
    FUN: FnOnce(IN) -> FUT {

    let labels = labels.traffic(LblTraffic::In);
    metrics.throughput(labels, &input);
    let timestamp = Instant::now();
    let res = func(input).await.map_err(Into::into);
    let labels = labels.traffic(LblTraffic::Out);
    // TODO: add error label
    match res {
        Ok(output) => {
            metrics.latency(labels, timestamp);
            metrics.throughput(labels, &output);
            Ok(output)
        },
        Err(error) => {
            metrics.latency(labels, timestamp);
            Err(error)
        }
    }
}

async fn kafka_send(
    cli: &PartitionClient,
    metrics: &JrpkMetrics,
    labels: &mut Labels,
    records: Vec<Record>
) -> Result<Vec<i64>, JrpkError> {
    let labels = labels.method(LblMethod::Send);
    let func = |rs| cli.produce(rs, Compression::Snappy);
    meter(records, metrics, labels, func).await
}

async fn kafka_fetch(
    cli: &PartitionClient,
    metrics: &JrpkMetrics,
    labels: &mut Labels,
    offset: KfkOffset,
    bytes: Range<i32>,
    max_wait_ms: i32
) -> Result<(Vec<RecordAndOffset>, i64), JrpkError> {
    let offset = kafka_offset(cli, metrics, labels, offset).await?;
    let labels = labels.method(LblMethod::Fetch);
    let func = |(offset, bytes, max_wait_ms)| cli.fetch_records(offset, bytes, max_wait_ms);
    meter((offset, bytes, max_wait_ms), metrics, labels, func).await
}

async fn kafka_offset(
    cli: &PartitionClient,
    metrics: &JrpkMetrics,
    labels: &mut Labels,
    offset: KfkOffset
) -> Result<i64, JrpkError> {
    match offset {
        KfkOffset::At(at) => {
            let labels = labels.method(LblMethod::Offset);
            let func = |at| cli.get_offset(at);
            meter(at, &metrics, labels, func).await
        }
        KfkOffset::Pos(pos) => {
            Ok(pos)
        }
    }
}

async fn kafka_loop(
    tap: Tap,
    cli: PartitionClient,
    metrics: JrpkMetrics,
    mut rcv: Receiver<KfkReq>,
) -> Result<(), JrpkError> {
    let mut labels = Labels::new(LblTier::Kafka).tap(tap).build();
    while let Some(req) = rcv.recv().await {
        match req {
            KfkReq::Send(Request(Id(id, records), rsp)) => {
                let res = kafka_send(&cli, &metrics, &mut labels, records).await;
                rsp.send(id, res).await?
            }
            KfkReq::Fetch(Request(Id(id, (offset, bytes, max_wait_ms)), rsp)) => {
                let res = kafka_fetch(&cli, &metrics, &mut labels, offset, bytes, max_wait_ms).await;
                rsp.send(id, res).await?
            }
            KfkReq::Offset(Request(Id(id, offset), rsp)) => {
                let res = kafka_offset(&cli, &metrics, &mut labels, offset).await;
                rsp.send(id, res).await?
            }
        }
    }
    Ok(())
}

pub struct KfkClientCache<REQ> {
    client: Client,
    cache: Cache<Tap, Sender<REQ>>,
    queue_size: usize,
    metrics: JrpkMetrics,
}

impl KfkClientCache<KfkReq> {
    pub fn new(client: Client, capacity: u64, queue_size: usize, metrics: JrpkMetrics) -> Self {
        Self { client, cache: Cache::new(capacity), queue_size, metrics }
    }

    #[instrument(err, skip(self))]
    async fn init_kafka_loop(&self, tap: Tap) -> Result<Sender<KfkReq>, JrpkError> {
        info!("init: {}, queue length: {}", tap, self.queue_size);
        let cli = self.client.partition_client(tap.topic.as_str(), tap.partition, UnknownTopicHandling::Error).await?;
        let (snd, rcv) = tokio::sync::mpsc::channel(self.queue_size);
        spawn(kafka_loop(tap, cli, self.metrics.clone(), rcv));
        Ok(snd)
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn lookup_sender(&self, tap: Tap) -> Result<Sender<KfkReq>, JrpkError> {
        trace!("lookup: {}", tap);
        self.cache.run_pending_tasks().await;
        if let Some(snd) = self.cache.get(&tap).await {
            Ok(snd)
        } else {
            let init = self.init_kafka_loop(tap.clone());
            let snd = self.cache.try_get_with(tap, init).await?;
            Ok(snd)
        }
    }
    
}
