use std::error::Error;
use crate::error::JrpkError;
use crate::util::{ReqCtx, ResCtx, Tap};
use moka::future::Cache;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::{Record, RecordAndOffset};
use std::future::Future;
use std::ops::Range;
use std::time::Instant;
use log::info;
use tokio::{select, spawn};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{instrument, trace};
use crate::metrics::{JrpkMetrics, Labels, LblMethod, LblTier, LblTraffic};
use crate::request_response;
use crate::size::Size;

request_response!(KfkReqSend, Vec<Record>, KfkResSend, Vec<i64>, S, JrpkError);
request_response!(KfkReqFetch, (i64, Range<i32>, i32), KfkResFetch, (Vec<RecordAndOffset>, i64), F, JrpkError);
request_response!(KfkReqOffset, OffsetAt, KfkResOffset, i64, O, JrpkError);

pub enum KfkReq<S, F, O> {
    Send(KfkReqSend<S>),
    Fetch(KfkReqFetch<F>),
    Offset(KfkReqOffset<O>),
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

async fn kafka_loop<S, F, O>(
    tap: Tap,
    cli: PartitionClient,
    metrics: JrpkMetrics,
    mut rcv: Receiver<KfkReq<S, F, O>>,
) -> Result<(), JrpkError> {
    let mut labels = Labels::new(LblTier::Kafka).tap(tap).build();
    while let Some(req) = rcv.recv().await {
        match req {
            KfkReq::Send(KfkReqSend { req, ctx, snd }) => {
                let res = meter(
                    req, 
                    &metrics,
                    labels.method(LblMethod::Send), 
                    |records| cli.produce(records, Compression::Snappy)
                ).await;
                snd.send(KfkResSend::new(res, ctx)).await?
            }
            KfkReq::Fetch(KfkReqFetch { req, ctx, snd }) => {
                let res = meter(
                    req, 
                    &metrics,
                    labels.method(LblMethod::Fetch),
                    |(offset, bytes, max_wait_ms)| cli.fetch_records(offset, bytes, max_wait_ms)
                ).await;
                snd.send(KfkResFetch::new(res, ctx)).await?
            }
            KfkReq::Offset(KfkReqOffset { req, ctx, snd }) => {
                let res = meter(
                    req, 
                    &metrics, 
                    labels.method(LblMethod::Offset),
                    |at| cli.get_offset(at)
                ).await;
                snd.send(KfkResOffset::new(res, ctx)).await?
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

impl <S: Send + 'static, F: Send + 'static, O: Send + 'static> KfkClientCache<KfkReq<S,F,O>> {
    pub fn new(client: Client, capacity: u64, queue_size: usize, metrics: JrpkMetrics) -> Self {
        Self { client, cache: Cache::new(capacity), queue_size, metrics }
    }

    #[instrument(err, skip(self))]
    async fn init_kafka_loop(&self, tap: Tap) -> Result<Sender<KfkReq<S, F, O>>, JrpkError> {
        info!("init: {}, queue length: {}", tap, self.queue_size);
        let cli = self.client.partition_client(tap.topic.as_str(), tap.partition, UnknownTopicHandling::Error).await?;
        let (snd, rcv) = tokio::sync::mpsc::channel(self.queue_size);
        spawn(kafka_loop(tap, cli, self.metrics.clone(), rcv));
        Ok(snd)
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn lookup_senders(&self, tap: Tap) -> Result<Sender<KfkReq<S, F, O>>, JrpkError> {
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
