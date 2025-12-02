use crate::error::JrpkError;
use crate::util::{debug_record_and_offset, debug_vec_fn, ReqCtx, ResCtx, Tap};
use moka::future::Cache;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::{Record, RecordAndOffset};
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::time::Instant;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, instrument, trace};
use crate::metrics::{JrpkMetrics, Labels, LblMethod, LblTier, LblTraffic};

#[derive(Debug)]
pub enum KfkOffset {
    Implicit(OffsetAt),
    Explicit(i64)
}

pub enum KfkReq<C> {
    Send {
        records: Vec<Record>
    },
    Fetch {
        offset: KfkOffset,
        bytes: Range<i32>,
        max_wait_ms: i32,
        codecs: C,
    },
    Offset {
        offset: KfkOffset
    }
}

impl <C: Debug> Debug for KfkReq<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KfkReq::Send { records} => {
                write!(f, "Send {{ ")?;
                debug_vec_fn(f, records, |f, r| { debug_record_and_offset(f, r, None) })?;
                write!(f, " }}")
            }
            KfkReq::Fetch { offset, bytes, max_wait_ms, codecs } => {
                f.debug_struct("Fetch")
                    .field("offset", offset)
                    .field("bytes", bytes)
                    .field("max_wait_ms", max_wait_ms)
                    .field("codecs: ", codecs)
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

impl <C> KfkReq<C> {
    pub fn send(records: Vec<Record>) -> Self {
        KfkReq::Send { records }
    }
    pub fn fetch(offset: KfkOffset, bytes: Range<i32>, max_wait_ms: i32, codecs: C) -> Self {
        KfkReq::Fetch { offset, bytes, max_wait_ms, codecs }
    }
    pub fn offset(offset: KfkOffset) -> Self {
        KfkReq::Offset { offset }
    }
}

pub enum KfkRsp<C> {
    Send {
        offsets: Vec<i64>,
    },
    Fetch {
        recs_and_offsets: Vec<RecordAndOffset>,
        high_watermark: i64,
        codecs: C
    },
    Offset(i64)
}

impl <C> KfkRsp<C> {
    fn send(offsets: Vec<i64>) -> Self {
        KfkRsp::Send { offsets }
    }
    fn fetch(recs_and_offsets: Vec<RecordAndOffset>, high_watermark: i64, codecs: C) -> Self {
        KfkRsp::Fetch { recs_and_offsets, high_watermark, codecs }
    }
    fn offset(value: i64) -> Self {
        KfkRsp::Offset(value)
    }
}

impl <C: Debug> Debug for KfkRsp<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KfkRsp::Send { offsets } => {
                f.debug_struct("Send").field("offsets", offsets).finish()
            }
            KfkRsp::Fetch { recs_and_offsets, high_watermark, codecs } => {
                write!(f, "Fetch {{ ")?;
                debug_vec_fn(f, recs_and_offsets, |f, r| {
                    debug_record_and_offset(f, &r.record, Some(r.offset))
                })?;
                write!(f, ", high_watermark: {:?}", high_watermark)?;
                write!(f, ", codecs: {:?}", codecs)?;
                write!(f, " }}")
            }
            KfkRsp::Offset(offset) => {
                f.debug_tuple("Offset").field(offset).finish()
            }
        }
    }
}

pub type RsKafkaError = rskafka::client::error::Error;
pub type KfkReqCtx<COD, CTX> = ReqCtx<KfkReq<COD>, KfkRsp<COD>, JrpkError, CTX>;
pub type KfkResCtx<COD, CTX> = ResCtx<KfkRsp<COD>, JrpkError, CTX>;

// R: From<KfkResId<C>>
pub type KfkResCtxSnd<COD, CTX> = Sender<KfkResCtx<COD, CTX>>;
pub type KfkResCtxRcv<COD, CTX> = Receiver<KfkResCtx<COD, CTX>>;
pub type KfkReqCtxSnd<COD, CTX> = Sender<KfkReqCtx<COD, CTX>>;
pub type KfkReqCtxRcv<COD, CTX> = Receiver<KfkReqCtx<COD, CTX>>;

#[inline]
fn record_length(r: &Record) -> usize {
    r.key.as_ref().map(|k| k.len()).unwrap_or(0) + r.value.as_ref().map(|k| k.len()).unwrap_or(0)
}

#[inline]
fn records_length(rs: &Vec<Record>) -> usize {
    rs.iter().map(|r| record_length(r)).sum()
}

#[inline]
fn records_and_offsets_length(ros: &Vec<RecordAndOffset>) -> usize {
    ros.iter().map(|ro| record_length(&ro.record)).sum()
}

#[instrument(ret, err, skip(cli, metrics, req_ctx_rcv))]
async fn run_kafka_loop<COD: Debug, CTX: Debug>(
    tap: Tap,
    cli: PartitionClient,
    metrics: JrpkMetrics,
    mut req_ctx_rcv: KfkReqCtxRcv<COD, CTX>
) -> Result<(), JrpkError> {
    while let Some(KfkReqCtx { req, ctx, res_rsp_ctx_snd}) = req_ctx_rcv.recv().await {
        trace!("request: {:?}", req);
        let mut labels = Labels::new(LblTier::Kafka).tap(tap.clone()).build();
        let ts = Instant::now();
        let res_rsp = match req {
            KfkReq::Send { records } => {
                let length = records_length(&records);
                metrics.throughputs
                    .get_or_create(labels.method(LblMethod::Send).traffic(LblTraffic::In))
                    .inc_by(length as u64);
                cli.produce(records, Compression::Snappy).await
                    .map(|offsets| {
                        let labels = labels.traffic(LblTraffic::Out);
                        metrics.latencies.get_or_create(&labels)
                            .observe(ts.elapsed().as_secs_f64());
                        metrics.throughputs.get_or_create(&labels)
                            .inc_by(8 * offsets.len() as u64);
                        KfkRsp::send(offsets)
                    })
            }
            KfkReq::Fetch { offset, bytes, max_wait_ms, codecs } => {
                let offset_explicit = match offset {
                    KfkOffset::Implicit(at) => cli.get_offset(at).await?,
                    KfkOffset::Explicit(n) => n
                };
                metrics.throughputs
                    .get_or_create(labels.method(LblMethod::Fetch).traffic(LblTraffic::In))
                    .inc_by(20);
                cli.fetch_records(offset_explicit, bytes, max_wait_ms).await
                    .map(|(recs_and_offsets, highwater_mark)| {
                        let labels = labels.traffic(LblTraffic::Out).build();
                        metrics.throughputs.get_or_create(&labels)
                            .inc_by(records_and_offsets_length(&recs_and_offsets) as u64);
                        metrics.latencies.get_or_create(&labels)
                            .observe(ts.elapsed().as_secs_f64());
                        KfkRsp::fetch(recs_and_offsets, highwater_mark, codecs)
                    })
            }
            KfkReq::Offset { offset } => {
                match offset {
                    KfkOffset::Implicit(at) => {
                        metrics.throughputs
                            .get_or_create(labels.method(LblMethod::Offset).traffic(LblTraffic::In))
                            .inc_by(4);
                        cli.get_offset(at).await
                            .map(|offset| {
                                let labels = labels.traffic(LblTraffic::Out).build();
                                metrics.throughputs.get_or_create(&labels)
                                    .inc_by(4);
                                metrics.latencies.get_or_create(&labels)
                                    .observe(ts.elapsed().as_secs_f64());
                                KfkRsp::offset(offset)
                            })
                    }
                    KfkOffset::Explicit(pos) => {
                        Ok(KfkRsp::offset(pos))
                    }
                }
            }
        };

        let res_rsp_ctx = match res_rsp {
            Ok(rsp) => KfkResCtx::ok(rsp, ctx),
            Err(err) => KfkResCtx::err(err.into(), ctx)
        };
        trace!("client: {}/{}, response: {:?}", cli.topic(), cli.partition(), res_rsp_ctx);
        res_rsp_ctx_snd.send(res_rsp_ctx).await?;
    }
    Ok(())
}

pub struct KfkClientCache<COD, CTX> {
    client: Client,
    cache: Cache<Tap, KfkReqCtxSnd<COD, CTX>>,
    queue_size: usize,
    metrics: JrpkMetrics,
}

impl <COD, CTX> KfkClientCache<COD, CTX>
where CTX: Debug + Send + 'static, COD: Debug + Send + 'static {
    pub fn new(client: Client, capacity: u64, queue_size: usize, metrics: JrpkMetrics) -> Self {
        Self { client, cache: Cache::new(capacity), queue_size, metrics }
    }

    #[instrument(err, skip(self))]
    async fn init_kafka_loop(&self, tap: Tap) -> Result<KfkReqCtxSnd<COD, CTX>, JrpkError> {
        info!("init: {}, queue length: {}", tap, self.queue_size);
        let pc = self.client.partition_client(tap.topic.as_str(), tap.partition, UnknownTopicHandling::Error).await?;
        let (req_id_snd, req_id_rcv) = mpsc::channel(self.queue_size);
        spawn(run_kafka_loop(tap, pc, self.metrics.clone(), req_id_rcv));
        Ok(req_id_snd)
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn lookup_kafka_sender(&self, tap: Tap) -> Result<KfkReqCtxSnd<COD, CTX>, JrpkError> {
        trace!("lookup: {}", tap);
        let init = self.init_kafka_loop(tap.clone());
        let req_id_snd = self.cache.try_get_with(tap, init).await?;
        self.cache.run_pending_tasks().await;
        //trace!("cached: {}", self.cache.entry_count());
        Ok(req_id_snd)
    }
}
