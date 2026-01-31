use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::error::JrpkError;
use crate::util::{Ctx, Req};
use moka::future::Cache;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::{Record, RecordAndOffset};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;
use faststr::FastStr;
use tokio::{spawn};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tracing::instrument;
use crate::args::KfkCompression;
use crate::metrics::{JrpkMetrics, JrpkLabels, LblMethod, LblTier, LblTraffic};
use crate::model::JrpOffset;
use crate::size::Size;

pub type KfkError = rskafka::client::error::Error;

// TODO: see if we can have a kfk_tap param like "posts@10"
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct KfkTap {
    pub topic: FastStr,
    pub partition: i32
}

impl KfkTap {
    pub fn new<S: Into<FastStr>>(topic: S, partition: i32) -> Self {
        KfkTap { topic: topic.into(), partition }
    }
}

impl Display for KfkTap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.topic, self.partition)
    }
}

pub fn a2k_compression(compression: Option<KfkCompression>) -> Compression {
    match compression {
        None => Compression::NoCompression,
        Some(KfkCompression::Gzip) => Compression::Gzip,
        Some(KfkCompression::Lz4) => Compression::Lz4,
        Some(KfkCompression::Snappy) => Compression::Snappy,
        Some(KfkCompression::Zstd) => Compression::Zstd,
    }
}

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

#[derive(Debug)]
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

#[derive(Debug)]
pub struct KfkTypesIn;

impl KfkTypes for KfkTypesIn {
    type S = Vec<Record>;
    type F = (KfkOffset, Range<i32>, i32);
    type O = KfkOffset;
}

#[derive(Debug)]
pub struct KfkTypesRsp;

impl KfkTypes for KfkTypesRsp {
    type S = Vec<i64>;
    type F = (Vec<RecordAndOffset>, i64);
    type O = i64;
}

#[derive(Debug)]
pub struct KfkResTypes<T, E>(PhantomData<T>, PhantomData<E>);

impl <T: KfkTypes, E: Error> KfkTypes for KfkResTypes<T, E> {
    type S = Result<T::S, E>;
    type F = Result<T::F, E>;
    type O = Result<T::O, E>;
}

#[derive(Debug)]
pub struct KfkCtxTypes<C, T>(PhantomData<C>, PhantomData<T>);

impl <C: KfkTypes, T: KfkTypes> KfkTypes for KfkCtxTypes<C, T> {
    type S = Ctx<C::S, T::S>;
    type F = Ctx<C::F, T::F>;
    type O = Ctx<C::O, T::O>;
}

pub type KfkIn<C> = KfkData<KfkCtxTypes<C, KfkTypesIn>>;

pub type KfkRsp<C> = KfkData<KfkCtxTypes<C, KfkResTypes<KfkTypesRsp, KfkError>>>;

#[derive(Debug)]
pub struct KfkCtxReqTypes<C, T, U, E, K>(PhantomData<C>, PhantomData<T>, PhantomData<U>, PhantomData<E>, PhantomData<K>);

impl <C: KfkTypes, T: KfkTypes, U: KfkTypes, E: Error, K> KfkTypes for KfkCtxReqTypes<C, T, U, E, K> {
    type S = Req<C::S, T::S, K>;
    type F = Req<C::F, T::F, K>;
    type O = Req<C::O, T::O, K>;
}

pub type KfkReq<C> = KfkData<KfkCtxReqTypes<C, KfkTypesIn, KfkTypesRsp, KfkError, KfkRsp<C>>>;

/// Helper function to encapsulate cumbersome metrics manipulations
#[instrument(level="trace", err, skip(input, metrics, func))]
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

#[instrument(level="debug", err, skip(kfk_client, kfk_records, metrics, labels))]
async fn kafka_send(
    kfk_client: &PartitionClient,
    kfk_records: Vec<Record>,
    kfk_compression: Compression,
    metrics: &JrpkMetrics,
    labels: &mut JrpkLabels,
) -> Result<Vec<i64>, KfkError> {
    let labels = labels.method(LblMethod::Send);
    let func = |rs| kfk_client.produce(rs, kfk_compression);
    let offsets = meter(kfk_records, metrics, labels, func).await?;
    Ok(offsets)
}

#[instrument(level="debug", err, skip(kfk_client, metrics, labels))]
async fn kafka_fetch(
    kfk_client: &PartitionClient,
    kfk_offset: KfkOffset,
    kfk_bytes: Range<i32>,
    kfk_max_wait_ms: i32,
    metrics: &JrpkMetrics,
    labels: &mut JrpkLabels,
) -> Result<(Vec<RecordAndOffset>, i64), KfkError> {
    let offset = match kfk_offset {
        KfkOffset::At(kfk_at) => {
            kafka_offset(kfk_client, kfk_at, metrics, labels).await?
        }
        KfkOffset::Pos(offset) => {
            offset
        }
    };
    let labels = labels.method(LblMethod::Fetch);
    let func = |(offset, bytes, max_wait_ms)| kfk_client.fetch_records(offset, bytes, max_wait_ms);
    let (records_and_offsets, high_watermark) = meter((offset, kfk_bytes, kfk_max_wait_ms), metrics, labels, func).await?;
    Ok((records_and_offsets, high_watermark))
}

#[instrument(level="debug", err, skip(kfk_client, metrics, labels))]
async fn kafka_offset(
    kfk_client: &PartitionClient,
    kfk_at: OffsetAt,
    metrics: &JrpkMetrics,
    labels: &mut JrpkLabels,
) -> Result<i64, KfkError> {
    let labels = labels.method(LblMethod::Offset);
    let func = |at| kfk_client.get_offset(at);
    let offset = meter(kfk_at, &metrics, labels, func).await?;
    Ok(offset)
}

#[instrument(level="info", err, skip(kfk_client, metrics, kfk_rcv))]
async fn kafka_loop<C: KfkTypes>(
    kfk_tap: KfkTap,
    kfk_client: PartitionClient,
    kfk_compression: Compression,
    metrics: Arc<JrpkMetrics>,
    mut kfk_rcv: Receiver<KfkReq<C>>,
) -> Result<(), SendError<KfkRsp<C>>> {
    let mut labels = JrpkLabels::new(LblTier::Kafka).tap(kfk_tap).build();
    while let Some(req) = kfk_rcv.recv().await {
        match req {
            KfkReq::Send(Req(Ctx(ctx, records), rsp)) => {
                let res = kafka_send(&kfk_client, records, kfk_compression, &metrics, &mut labels).await;
                rsp.send(KfkRsp::Send(Ctx::new(ctx, res))).await?
            }
            KfkReq::Fetch(Req(Ctx(ctx, (offset, bytes, max_wait_ms)), rsp)) => {
                let res = kafka_fetch(&kfk_client, offset, bytes, max_wait_ms, &metrics, &mut labels).await;
                rsp.send(KfkRsp::Fetch(Ctx::new(ctx, res))).await?
            }
            KfkReq::Offset(Req(Ctx(ctx, offset), rsp)) => {
                let res = match offset {
                    KfkOffset::At(kfk_at) => {
                        kafka_offset(&kfk_client, kfk_at, &metrics, &mut labels).await
                    }
                    KfkOffset::Pos(offset) => {
                        Ok(offset)
                    }
                };
                rsp.send(KfkRsp::Offset(Ctx::new(ctx, res))).await?
            }
        }
    }
    Ok(())
}

pub struct KfkClientCache<REQ> {
    client: Client,
    cache: Cache<KfkTap, Sender<REQ>>,
    compression: Compression,
    queue_size: usize,
    metrics: Arc<JrpkMetrics>,
}

impl <C: KfkTypes> KfkClientCache<KfkReq<C>>
where C::S: Send + Sync, C::F: Send + Sync, C::O: Send + Sync, C: 'static {

    pub fn new(client: Client, compression: Compression, capacity: u64, queue_size: usize, metrics: Arc<JrpkMetrics>) -> Self {
        Self { client, cache: Cache::new(capacity), compression, queue_size, metrics }
    }

    #[instrument(level="info", err, skip(self))]
    async fn init_kafka_loop(&self, tap: KfkTap) -> Result<Sender<KfkReq<C>>, JrpkError> {
        let cli = self.client.partition_client(tap.topic.as_str(), tap.partition, UnknownTopicHandling::Error).await?;
        let (snd, rcv) = tokio::sync::mpsc::channel(self.queue_size);
        spawn(kafka_loop(tap, cli, self.compression, self.metrics.clone(), rcv));
        Ok(snd)
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn lookup_sender(&self, tap: KfkTap) -> Result<Sender<KfkReq<C>>, JrpkError> {
        if let Some(snd) = self.cache.get(&tap).await {
            Ok(snd)
        } else {
            let init = self.init_kafka_loop(tap.clone());
            let snd = self.cache.try_get_with(tap, init).await?;
            Ok(snd)
        }
    }
    
}
