use std::cmp::Ordering;
use std::error::Error;
use crate::error::JrpkError;
use crate::util::{Ctx, Req, Tap};
use moka::future::Cache;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::{Record, RecordAndOffset};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;
use log::info;
use tokio::{spawn};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tracing::{instrument, trace};
use crate::metrics::{JrpkMetrics, JrpkLabels, LblMethod, LblTier, LblTraffic};
use crate::model::JrpOffset;
use crate::size::Size;

pub type KfkError = rskafka::client::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KfkOffset {
    At(OffsetAt),
    Pos(i64)
}

impl From<JrpOffset> for KfkOffset {
    fn from(value: JrpOffset) -> Self {
        match value {
            JrpOffset::Earliest => KfkOffset::At(OffsetAt::Earliest),
            JrpOffset::Latest => KfkOffset::At(OffsetAt::Latest),
            JrpOffset::Timestamp(ts) => KfkOffset::At(OffsetAt::Timestamp(ts)),
            JrpOffset::Offset(pos) => KfkOffset::Pos(pos)
        }
    }
}

#[inline]
fn cmp_offset_at_2_record_and_offset(oa: &OffsetAt, ro: &RecordAndOffset) -> Ordering {
    match oa {
        OffsetAt::Earliest => Ordering::Less,
        OffsetAt::Latest => Ordering::Greater,
        OffsetAt::Timestamp(ts) => ts.cmp(&ro.record.timestamp)
    }
}

#[inline]
fn cmp_kfk_offset_2_record_and_offset(ko: &KfkOffset, ro: &RecordAndOffset) -> Ordering {
    match ko {
        KfkOffset::At(at) => cmp_offset_at_2_record_and_offset(at, ro),
        KfkOffset::Pos(pos) => pos.cmp(&ro.offset)
    }
}

impl PartialEq<RecordAndOffset> for KfkOffset {
    fn eq(&self, other: &RecordAndOffset) -> bool {
        cmp_kfk_offset_2_record_and_offset(self, other) == Ordering::Equal
    }
}

impl PartialOrd<RecordAndOffset> for KfkOffset {
    fn partial_cmp(&self, other: &RecordAndOffset) -> Option<Ordering> {
        Some(cmp_kfk_offset_2_record_and_offset(self, other))
    }
}

pub trait KfkTypes {
    type S;
    type F;
    type O;
}

pub enum KfkData<T: KfkTypes> {
    Send(T::S),
    Fetch(T::F),
    Offset(T::O),
}

impl <T: KfkTypes> KfkData<T> {

    pub fn send_or<E>(self, err: E) -> Result<T::S, E> {
        if let KfkData::Send(value) = self {
            Ok(value)
        } else {
            Err(err)
        }
    }
    pub fn fetch_or<E>(self, err: E) -> Result<T::F, E> {
        if let KfkData::Fetch(value) = self {
            Ok(value)
        } else {
            Err(err)
        }
    }
    pub fn offset_or<E>(self, err: E) -> Result<T::O, E> {
        if let KfkData::Offset(value) = self {
            Ok(value)
        } else {
            Err(err)
        }
    }
}

pub struct KfkTypesIn;

impl KfkTypes for KfkTypesIn {
    type S = Vec<Record>;
    type F = (KfkOffset, Range<i32>, i32);
    type O = KfkOffset;
}

pub struct KfkTypesRsp;

impl KfkTypes for KfkTypesRsp {
    type S = Vec<i64>;
    type F = (Vec<RecordAndOffset>, i64);
    type O = i64;
}

pub struct KfkResTypes<T, E>(PhantomData<T>, PhantomData<E>);

impl <T: KfkTypes, E: Error> KfkTypes for KfkResTypes<T, E> {
    type S = Result<T::S, E>;
    type F = Result<T::F, E>;
    type O = Result<T::O, E>;
}

pub struct KfkCtxTypes<C, T>(PhantomData<C>, PhantomData<T>);

impl <C: KfkTypes, T: KfkTypes> KfkTypes for KfkCtxTypes<C, T> {
    type S = Ctx<C::S, T::S>;
    type F = Ctx<C::F, T::F>;
    type O = Ctx<C::O, T::O>;
}

pub type KfkIn<C> = KfkData<KfkCtxTypes<C, KfkTypesIn>>;

pub type KfkRsp<C> = KfkData<KfkCtxTypes<C, KfkResTypes<KfkTypesRsp, KfkError>>>;

pub struct KfkCtxReqTypes<C, T, U, E, K>(PhantomData<C>, PhantomData<T>, PhantomData<U>, PhantomData<E>, PhantomData<K>);

impl <C: KfkTypes, T: KfkTypes, U: KfkTypes, E: Error, K> KfkTypes for KfkCtxReqTypes<C, T, U, E, K> {
    type S = Req<C::S, T::S, K>;
    type F = Req<C::F, T::F, K>;
    type O = Req<C::O, T::O, K>;
}

pub type KfkReq<C> = KfkData<KfkCtxReqTypes<C, KfkTypesIn, KfkTypesRsp, KfkError, KfkRsp<C>>>;

/// Helper function to encapsulate cumbersome metrics manipulations
async fn meter<IN, OUT, ERR, FUT, FUN>(
    input: IN,
    metrics: &JrpkMetrics,
    labels: &mut JrpkLabels,
    func: FUN) -> Result<OUT, ERR>
where
    IN: Size,
    OUT: Size,
    ERR: Error,
    FUT: Future<Output=Result<OUT, ERR>>,
    FUN: FnOnce(IN) -> FUT {

    let labels = labels.traffic(LblTraffic::In);
    metrics.size(labels, &input);
    let timestamp = Instant::now();
    let res = func(input).await;
    let labels = labels.traffic(LblTraffic::Out);
    // TODO: add error label
    match res {
        Ok(output) => {
            metrics.time(labels, timestamp);
            metrics.size(labels, &output);
            Ok(output)
        },
        Err(error) => {
            metrics.time(labels, timestamp);
            Err(error)
        }
    }
}

async fn kafka_send(
    cli: &PartitionClient,
    metrics: &JrpkMetrics,
    labels: &mut JrpkLabels,
    records: Vec<Record>
) -> Result<Vec<i64>, KfkError> {
    let labels = labels.method(LblMethod::Send);
    let func = |rs| cli.produce(rs, Compression::Snappy);
    let offsets = meter(records, metrics, labels, func).await?;
    Ok(offsets)
}

async fn kafka_fetch(
    cli: &PartitionClient,
    metrics: &JrpkMetrics,
    labels: &mut JrpkLabels,
    offset: KfkOffset,
    bytes: Range<i32>,
    max_wait_ms: i32
) -> Result<(Vec<RecordAndOffset>, i64), KfkError> {
    let offset = kafka_offset(cli, metrics, labels, offset).await?;
    let labels = labels.method(LblMethod::Fetch);
    let func = |(offset, bytes, max_wait_ms)| cli.fetch_records(offset, bytes, max_wait_ms);
    let (records_and_offsets, high_watermark) = meter((offset, bytes, max_wait_ms), metrics, labels, func).await?;
    Ok((records_and_offsets, high_watermark))
}

async fn kafka_offset(
    cli: &PartitionClient,
    metrics: &JrpkMetrics,
    labels: &mut JrpkLabels,
    offset: KfkOffset
) -> Result<i64, KfkError> {
    match offset {
        KfkOffset::At(at) => {
            let labels = labels.method(LblMethod::Offset);
            let func = |at| cli.get_offset(at);
            let offset = meter(at, &metrics, labels, func).await?;
            Ok(offset)
        }
        KfkOffset::Pos(pos) => {
            Ok(pos)
        }
    }
}

async fn kafka_loop<C: KfkTypes>(
    tap: Tap,
    cli: PartitionClient,
    metrics: Arc<JrpkMetrics>,
    mut rcv: Receiver<KfkReq<C>>,
) -> Result<(), SendError<KfkRsp<C>>> {
    let mut labels = JrpkLabels::new(LblTier::Kafka).tap(tap).build();
    while let Some(req) = rcv.recv().await {
        match req {
            KfkReq::Send(Req(Ctx(ctx, records), rsp)) => {
                let res = kafka_send(&cli, &metrics, &mut labels, records).await;
                rsp.send(KfkRsp::Send(Ctx::new(ctx, res))).await?
            }
            KfkReq::Fetch(Req(Ctx(ctx, (offset, bytes, max_wait_ms)), rsp)) => {
                let res = kafka_fetch(&cli, &metrics, &mut labels, offset, bytes, max_wait_ms).await;
                rsp.send(KfkRsp::Fetch(Ctx::new(ctx, res))).await?
            }
            KfkReq::Offset(Req(Ctx(ctx, offset), rsp)) => {
                let res = kafka_offset(&cli, &metrics, &mut labels, offset).await;
                rsp.send(KfkRsp::Offset(Ctx::new(ctx, res))).await?
            }
        }
    }
    Ok(())
}

pub struct KfkClientCache<REQ> {
    client: Client,
    cache: Cache<Tap, Sender<REQ>>,
    queue_size: usize,
    metrics: Arc<JrpkMetrics>,
}

impl <C: KfkTypes> KfkClientCache<KfkReq<C>>
where C::S: Send + Sync, C::F: Send + Sync, C::O: Send + Sync, C: 'static {

    pub fn new(client: Client, capacity: u64, queue_size: usize, metrics: Arc<JrpkMetrics>) -> Self {
        Self { client, cache: Cache::new(capacity), queue_size, metrics }
    }

    #[instrument(err, skip(self))]
    async fn init_kafka_loop(&self, tap: Tap) -> Result<Sender<KfkReq<C>>, JrpkError> {
        info!("init: {}, queue length: {}", tap, self.queue_size);
        let cli = self.client.partition_client(tap.topic.as_str(), tap.partition, UnknownTopicHandling::Error).await?;
        let (snd, rcv) = tokio::sync::mpsc::channel(self.queue_size);
        spawn(kafka_loop(tap, cli, self.metrics.clone(), rcv));
        Ok(snd)
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn lookup_sender(&self, tap: Tap) -> Result<Sender<KfkReq<C>>, JrpkError> {
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
