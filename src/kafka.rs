use crate::error::JrpkError;
use crate::util::{debug_record_and_offset, debug_vec_fn, BoundedReceiver, BoundedSender, OneshotReceiver, OneshotSender, Req, ReqId, ResId, Tap};
use moka::future::Cache;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::{Record, RecordAndOffset};
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::time::Instant;
use log::info;
use tokio::{select, spawn};
use tracing::{instrument, trace};
use crate::metrics::{JrpkMetrics, Labels, LblMethod, LblTier, LblTraffic};

#[derive(Debug)]
pub enum KfkOffset {
    Implicit(OffsetAt),
    Explicit(i64)
}

pub enum KfkReq {
    Send {
        records: Vec<Record>
    },
    Fetch {
        offset: KfkOffset,
        bytes: Range<i32>,
        max_wait_ms: i32,
    },
    Offset {
        offset: KfkOffset
    }
}

impl Debug for KfkReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KfkReq::Send { records} => {
                write!(f, "Send {{ ")?;
                debug_vec_fn(f, records, |f, r| { debug_record_and_offset(f, r, None) })?;
                write!(f, " }}")
            }
            KfkReq::Fetch { offset, bytes, max_wait_ms} => {
                f.debug_struct("Fetch")
                    .field("offset", offset)
                    .field("bytes", bytes)
                    .field("max_wait_ms", max_wait_ms)
                    .finish()
            }
            KfkReq::Offset { offset } => {
                f.debug_struct("Offset")
                    .field("offset", offset)
                    .finish()
            }
        }
    }
}

impl KfkReq {
    pub fn send(records: Vec<Record>) -> Self {
        KfkReq::Send { records }
    }
    pub fn fetch(offset: KfkOffset, bytes: Range<i32>, max_wait_ms: i32) -> Self {
        KfkReq::Fetch { offset, bytes, max_wait_ms }
    }
    pub fn offset(offset: KfkOffset) -> Self {
        KfkReq::Offset { offset }
    }
}

pub enum KfkRsp {
    Send {
        offsets: Vec<i64>,
    },
    Fetch {
        recs_and_offsets: Vec<RecordAndOffset>,
        high_watermark: i64,
    },
    Offset(i64)
}

impl KfkRsp {
    pub fn to_send(self) -> Result<Vec<i64>, JrpkError> {
        if let KfkRsp::Send { offsets } = self {
            Ok(offsets)
        } else {
            Err(JrpkError::Unexpected("Unexpected kafka response"))
        }
    }
    pub fn to_fetch(self) -> Result<(Vec<RecordAndOffset>, i64), JrpkError> {
        if let KfkRsp::Fetch { recs_and_offsets, high_watermark } = self {
            Ok((recs_and_offsets, high_watermark))
        } else {
            Err(JrpkError::Unexpected("Unexpected kafka response"))
        }
    }
    pub fn to_offset(self) -> Result<i64, JrpkError> {
        if let KfkRsp::Offset(pos) = self {
            Ok(pos)
        } else {
            Err(JrpkError::Unexpected("Unexpected kafka response"))
        }
    }
}

impl From<Vec<i64>> for KfkRsp {
    fn from(offsets: Vec<i64>) -> Self {
        KfkRsp::Send { offsets }
    }
}

impl From<(Vec<RecordAndOffset>, i64)> for KfkRsp {
    fn from((recs_and_offsets, high_watermark): (Vec<RecordAndOffset>, i64)) -> Self {
        KfkRsp::Fetch { recs_and_offsets, high_watermark }
    }
}

impl From<i64> for KfkRsp {
    fn from(pos: i64) -> Self {
        KfkRsp::Offset(pos)
    }
}

impl Debug for KfkRsp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KfkRsp::Send { offsets } => {
                f.debug_struct("Send").field("offsets", offsets).finish()
            }
            KfkRsp::Fetch { recs_and_offsets, high_watermark} => {
                write!(f, "Fetch {{ ")?;
                debug_vec_fn(f, recs_and_offsets, |f, r| {
                    debug_record_and_offset(f, &r.record, Some(r.offset))
                })?;
                write!(f, ", high_watermark: {:?}", high_watermark)?;
                write!(f, " }}")
            }
            KfkRsp::Offset(offset) => {
                f.debug_tuple("Offset").field(offset).finish()
            }
        }
    }
}

pub type RsKafkaError = rskafka::client::error::Error;

#[inline]
fn record_length(r: &Record) -> usize {
    r.key.as_ref().map(|k| k.len()).unwrap_or(0) +
        r.value.as_ref().map(|k| k.len()).unwrap_or(0)
}

#[inline]
fn records_length(rs: &Vec<Record>) -> usize {
    rs.iter().map(|r| record_length(r)).sum()
}

#[inline]
fn records_and_offsets_length(ros: &Vec<RecordAndOffset>) -> usize {
    ros.iter().map(|ro| record_length(&ro.record) + size_of::<RecordAndOffset>()).sum()
}

async fn proc_kafka_send(
    cli: &PartitionClient,
    records: Vec<Record>,
    metrics: &JrpkMetrics,
    labels: &mut Labels,
) -> Result<KfkRsp, JrpkError> {
    let labels = labels.method(LblMethod::Send).traffic(LblTraffic::In);
    metrics.throughputs.get_or_create(labels)
        .inc_by(records_length(&records) as u64);
    let ts = Instant::now();
    let offsets = cli.produce(records, Compression::Snappy).await?;
    metrics.throughputs.get_or_create(labels)
        .inc_by((size_of::<i64>() * offsets.len()) as u64);
    metrics.latencies.get_or_create(labels)
        .observe(ts.elapsed().as_secs_f64());
    Ok(offsets.into())
}

async fn proc_kafka_offset(
    cli: &PartitionClient,
    offset: KfkOffset,
    metrics: &JrpkMetrics,
    labels: &mut Labels,
) -> Result<KfkRsp, JrpkError> {
    match offset {
        KfkOffset::Implicit(at) => {
            let labels = labels.method(LblMethod::Offset).traffic(LblTraffic::In);
            metrics.throughputs.get_or_create(labels)
                .inc_by(size_of_val(&at) as u64);
            let ts = Instant::now();
            let pos = cli.get_offset(at).await?;
            let labels = labels.traffic(LblTraffic::Out);
            metrics.throughputs.get_or_create(labels)
                .inc_by(size_of_val(&pos) as u64);
            metrics.latencies.get_or_create(labels)
                .observe(ts.elapsed().as_secs_f64());
            Ok(pos.into())
        }
        KfkOffset::Explicit(pos) => {
            Ok(pos.into())
        }
    }
}

async fn proc_kafka_fetch(
    cli: &PartitionClient,
    offset: KfkOffset,
    bytes: Range<i32>,
    max_wait_ms: i32,
    metrics: &JrpkMetrics,
    labels: &mut Labels,
) -> Result<KfkRsp, JrpkError> {
    if let KfkRsp::Offset(pos) = proc_kafka_offset(cli, offset, metrics, labels).await? {
        let labels = labels.method(LblMethod::Fetch).traffic(LblTraffic::In);
        metrics.throughputs.get_or_create(labels)
            .inc_by((size_of_val(&pos) + size_of_val(&bytes) + size_of_val(&max_wait_ms)) as u64);
        let ts = Instant::now();
        let (recs_and_offsets, high_watermark) = cli.fetch_records(pos, bytes, max_wait_ms).await?;
        let labels = labels.traffic(LblTraffic::Out);
        metrics.throughputs.get_or_create(labels)
            .inc_by((records_and_offsets_length(&recs_and_offsets) + size_of_val(&high_watermark)) as u64);
        metrics.latencies.get_or_create(labels)
            .observe(ts.elapsed().as_secs_f64());
        Ok((recs_and_offsets, high_watermark).into())
    } else {
        Err(JrpkError::Unexpected("fetch returns unexpected response"))
    }
}

async fn proc_kafka_req(
    cli: &PartitionClient,
    req: KfkReq,
    metrics: &JrpkMetrics,
    labels: &mut Labels,
) -> Result<KfkRsp, JrpkError> {
    match req {
        KfkReq::Send { records } => {
            proc_kafka_send(cli, records, metrics, labels).await
        },
        KfkReq::Fetch { offset, bytes, max_wait_ms } => {
            proc_kafka_fetch(cli, offset, bytes, max_wait_ms, metrics, labels).await
        },
        KfkReq::Offset { offset } => {
            proc_kafka_offset(cli, offset, metrics, labels).await
        },
    }
}

pub type KfkResIdSender = BoundedSender<ResId<KfkRsp, JrpkError>>;
pub type KfkResIdReceiver = BoundedReceiver<ResId<KfkRsp, JrpkError>>;
pub type KfkResSender = OneshotSender<Result<KfkRsp, JrpkError>>;
pub type KfkResReceiver = OneshotReceiver<Result<KfkRsp, JrpkError>>;
pub type KfkReqIdSender = BoundedSender<ReqId<KfkReq, KfkRsp, JrpkError>>;
pub type KfkReqIdReceiver = BoundedReceiver<ReqId<KfkReq, KfkRsp, JrpkError>>;
pub type KfkReqSender = BoundedSender<Req<KfkReq, KfkRsp, JrpkError>>;
pub type KfkReqReceiver = BoundedReceiver<Req<KfkReq, KfkRsp, JrpkError>>;

#[instrument(ret, err, skip(cli, metrics, jsonrpc_rcv, http_rcv))]
async fn run_kafka_loop(
    tap: Tap,
    cli: PartitionClient,
    metrics: JrpkMetrics,
    mut jsonrpc_rcv: KfkReqIdReceiver,
    mut http_rcv: KfkReqReceiver,
) -> Result<(), JrpkError> {
    let labels = Labels::new(LblTier::Kafka).tap(tap).build();
    loop {
        let mut labels = labels.clone();
        select! {
            biased;
            Some(ReqId { id, req, snd }) = jsonrpc_rcv.recv() => {
                trace!("jrpk, id: {}, req: {:?}", id, req);
                let res_rsp = proc_kafka_req(&cli, req, &metrics, &mut labels).await;
                let res_id_rsp = ResId::new(id, res_rsp);
                snd.send(res_id_rsp).await?
            }
            Some(Req { req, snd }) = http_rcv.recv() => {
                trace!("http, req: {:?}", req);
                let res_rsp = proc_kafka_req(&cli, req, &metrics, &mut labels).await;
                snd.send(res_rsp).map_err(|_| JrpkError::OneshotSend)?
            }
            else => {
                break;
            }
        }
    }
    Ok(())
}

pub struct KfkClientCache {
    client: Client,
    cache: Cache<Tap, (KfkReqIdSender, KfkReqSender)>,
    queue_size: usize,
    metrics: JrpkMetrics,
}

impl KfkClientCache {
    pub fn new(client: Client, capacity: u64, queue_size: usize, metrics: JrpkMetrics) -> Self {
        Self { client, cache: Cache::new(capacity), queue_size, metrics }
    }

    #[instrument(err, skip(self))]
    async fn init_kafka_loop(&self, tap: Tap) -> Result<(KfkReqIdSender, KfkReqSender), JrpkError> {
        info!("init: {}, queue length: {}", tap, self.queue_size);
        let cli = self.client.partition_client(tap.topic.as_str(), tap.partition, UnknownTopicHandling::Error).await?;
        let (req_id_snd, req_id_rcv) = tokio::sync::mpsc::channel(self.queue_size);
        let (req_snd, req_rcv) = tokio::sync::mpsc::channel(self.queue_size);
        spawn(run_kafka_loop(tap, cli, self.metrics.clone(), req_id_rcv, req_rcv));
        Ok((req_id_snd, req_snd))
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn lookup_jsonrpc_sender(&self, tap: Tap) -> Result<KfkReqIdSender, JrpkError> {
        trace!("lookup: {}", tap);
        if let Some((jsonrpc_snd, _)) = self.cache.get(&tap).await {
            Ok(jsonrpc_snd)
        } else {
            let init = self.init_kafka_loop(tap.clone());
            let (jsonrpc_snd, _) = self.cache.try_get_with(tap, init).await?;
            self.cache.run_pending_tasks().await;
            Ok(jsonrpc_snd)
        }
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn lookup_http_sender(&self, tap: Tap) -> Result<KfkReqSender, JrpkError> {
        trace!("lookup: {}", tap);
        if let Some((_, http_snd)) = self.cache.get(&tap).await {
            Ok(http_snd)
        } else {
            let init = self.init_kafka_loop(tap.clone());
            let (_, http_snd) = self.cache.try_get_with(tap, init).await?;
            self.cache.run_pending_tasks().await;
            Ok(http_snd)
        }
    }
    
}
