use crate::error::JrpkError;
use crate::util::{debug_record_and_offset, debug_vec_fn, ReqCtx, ResCtx};
use moka::future::Cache;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::{Record, RecordAndOffset};
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::time::Instant;
use faststr::FastStr;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, instrument, trace};
use crate::metrics::{JrpkMeters, LBL_SERVER, LBL_SEND, LBL_FETCH, LBL_KAFKA, LBL_READ, LBL_OFFSET, LBL_WRITE};

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
            KfkReq::Fetch { offset, bytes, max_wait_ms } => {
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
    fn send(offsets: Vec<i64>) -> Self {
        KfkRsp::Send { offsets }
    }
    fn fetch(recs_and_offsets: Vec<RecordAndOffset>, high_watermark: i64) -> Self {
        KfkRsp::Fetch { recs_and_offsets, high_watermark }
    }
    fn offset(value: i64) -> Self {
        KfkRsp::Offset(value)
    }
}

impl Debug for KfkRsp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KfkRsp::Send { offsets } => {
                f.debug_struct("Send").field("offsets", offsets).finish()
            }
            KfkRsp::Fetch { recs_and_offsets, high_watermark } => {
                write!(f, "Send {{ ")?;
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
pub type KfkReqCtx<CTX> = ReqCtx<KfkReq, KfkRsp, JrpkError, CTX>;
pub type KfkResCtx<CTX> = ResCtx<KfkRsp, JrpkError, CTX>;

// R: From<KfkResId<C>>
pub type KfkResCtxSnd<CTX> = Sender<KfkResCtx<CTX>>;
pub type KfkResCtxRcv<CTX> = Receiver<KfkResCtx<CTX>>;
pub type KfkReqCtxSnd<CTX> = Sender<KfkReqCtx<CTX>>;
pub type KfkReqCtxRcv<CTX> = Receiver<KfkReqCtx<CTX>>;

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

#[instrument(ret, err, skip(cli, meters, req_ctx_rcv))]
async fn run_kafka_loop<CTX: Debug>(
    cli: PartitionClient,
    meters: JrpkMeters,
    mut req_ctx_rcv: KfkReqCtxRcv<CTX>
) -> Result<(), JrpkError> {
    let key = KfkKey::new(cli.topic().to_owned().into(), cli.partition());
    while let Some(KfkReqCtx { req, ctx, res_rsp_ctx_snd}) = req_ctx_rcv.recv().await {
        trace!("client: {}, request: {:?}", key, req);
        let res_rsp = match req {
            KfkReq::Send { records } => {
                let length = records_length(&records);
                let key = key.clone();
                cli.produce(records, Compression::Snappy).await
                    .map(|offsets| {
                        meters.throughput_owned(LBL_SERVER, LBL_SEND, LBL_WRITE, LBL_KAFKA, Some(key))
                            .inc_by(length as u64);
                        KfkRsp::send(offsets)
                    })
            }
            KfkReq::Fetch { offset, bytes, max_wait_ms } => {
                let offset_explicit = match offset {
                    KfkOffset::Implicit(at) => cli.get_offset(at).await?,
                    KfkOffset::Explicit(n) => n
                };
                let key = key.clone();
                cli.fetch_records(offset_explicit, bytes, max_wait_ms).await
                    .map(|(recs_and_offsets, highwater_mark)| {
                        meters.throughput_ref(LBL_SERVER, LBL_FETCH, LBL_READ, LBL_KAFKA, Some(key))
                            .inc_by(records_and_offsets_length(&recs_and_offsets) as u64);
                        KfkRsp::fetch(recs_and_offsets, highwater_mark)
                    })
            }
            KfkReq::Offset { offset } => {
                match offset {
                    KfkOffset::Implicit(at) => {
                        let key = key.clone();
                        cli.get_offset(at).await
                            .map(|offset| {
                                meters.throughput_ref(LBL_SERVER, LBL_OFFSET, LBL_WRITE, LBL_KAFKA, Some(key))
                                    .inc_by(4);
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

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct KfkKey {
    pub topic: FastStr,
    pub partition: i32,
}

impl KfkKey {
    pub fn new(topic: FastStr, partition: i32) -> Self {
        KfkKey { topic, partition }
    }
}

impl Display for KfkKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.topic, self.partition)
    }
}

pub struct KfkClientCache<CTX> {
    client: Client,
    cache: Cache<KfkKey, KfkReqCtxSnd<CTX>>,
    meters: JrpkMeters,
}

impl <CTX: Debug + Send + 'static> KfkClientCache<CTX> {

    pub fn new(client: Client, capacity: u64, meters: JrpkMeters) -> Self {
        Self { client, cache: Cache::new(capacity), meters }
    }

    #[instrument(err, skip(self))]
    async fn init_kafka_loop(&self, key: KfkKey, capacity: usize) -> Result<KfkReqCtxSnd<CTX>, JrpkError> {
        info!("init: {}:{}, capacity: {}", key.topic, key.partition, capacity);
        let pc = self.client.partition_client( key.topic.as_str(), key.partition, UnknownTopicHandling::Error).await?;
        let (req_id_snd, req_id_rcv) = mpsc::channel(capacity);
        spawn(run_kafka_loop(pc, self.meters.clone(), req_id_rcv));
        Ok(req_id_snd)
    }

    #[instrument(level="debug", err, skip(self))]
    pub async fn lookup_kafka_sender(&self, key: KfkKey, capacity: usize) -> Result<KfkReqCtxSnd<CTX>, JrpkError> {
        trace!("lookup: {}:{}", key.topic, key.partition);
        let init = self.init_kafka_loop(key.clone(), capacity);
        let req_id_snd = self.cache.try_get_with(key, init).await?;
        //self.cache.run_pending_tasks().await;
        //trace!("cached: {}", self.cache.entry_count());
        Ok(req_id_snd)
    }
}
