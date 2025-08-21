use std::fmt::{Debug, Formatter};
use crate::util::{debug_vec_fn, handle_future, ReqId, ResId, debug_record_and_offset};
use moka::future::Cache;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::{Record, RecordAndOffset};
use std::ops::Range;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, warn};
use crate::errors::{JrpkError, JrpkResult};
use crate::jsonrpc::{JrpMethod, JrpReq};

pub enum KfkReq {
    Send {
        records: Vec<Record>
    },
    Fetch {
        offset: i64,
        bytes: Range<i32>,
        max_wait_ms: i32,
    },
    Offset {
        at: OffsetAt
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
            KfkReq::Offset { at } => {
                f.debug_struct("Offset")
                    .field("at", at)
                    .finish()
            }
        }
    }
}

impl KfkReq {
    pub fn send(records: Vec<Record>) -> Self {
        KfkReq::Send { records }
    }
    pub fn fetch(offset: i64, bytes: Range<i32>, max_wait_ms: i32) -> Self {
        KfkReq::Fetch { offset, bytes, max_wait_ms }
    }
    pub fn offset(at: OffsetAt) -> Self {
        KfkReq::Offset { at }
    }
}

impl <'a> TryFrom<JrpReq<'a>> for KfkReq {
    type Error = JrpkError;
    fn try_from(req: JrpReq<'a>) -> Result<Self, Self::Error> {
        match req.method {
            JrpMethod::Send => {
                let jrp_records = req.params.records
                    .ok_or(JrpkError::Syntax("records is missing"))?;
                let records: Vec<Record> = jrp_records.into_iter()
                    .map(|x| x.into())
                    .collect();
                Ok(KfkReq::send(records))
            }
            JrpMethod::Fetch => {
                let offset = req.params.offset.ok_or(JrpkError::Syntax("offset is missing"))?;
                let bytes = req.params.bytes.ok_or(JrpkError::Syntax("bytes is missing"))?;
                let max_wait_ms = req.params.max_wait_ms.ok_or(JrpkError::Syntax("max_wait_ms is missing"))?;
                Ok(KfkReq::fetch(offset, bytes, max_wait_ms))
            }
            JrpMethod::Offset => {
                let at = req.params.at.ok_or(JrpkError::Syntax("at is missing"))?;
                Ok(KfkReq::offset(at.into()))
            }
        }
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
                    debug_record_and_offset(f, &r.record, Some(r.offset)) }
                )?;
                write!(f, " }}")
            }
            KfkRsp::Offset(offset) => {
                f.debug_tuple("Offset").field(offset).finish()
            }
        }
    }
}

pub type KfkError = rskafka::client::error::Error;
pub type KfkReqId = ReqId<KfkReq, KfkRsp, KfkError>;
pub type KfkResId = ResId<KfkRsp, KfkError>;
pub type KfkResIdSnd = Sender<KfkResId>;
pub type KfkResIdRcv = Receiver<KfkResId>;
pub type KfkReqIdSnd = Sender<KfkReqId>;
pub type KfkReqIdRcv = Receiver<KfkReqId>;

async fn run_kafka_loop(cli: PartitionClient, mut req_id_rcv: KfkReqIdRcv) -> JrpkResult<()> {
    info!("kafka, client: {}/{}, start", cli.topic(), cli.partition());
    while let Some(req_id) = req_id_rcv.recv().await {
        debug!("kafka, client: {}/{}, request: {:?}", cli.topic(), cli.partition(), req_id);
        let id = req_id.id;
        let req = req_id.req;
        let res_id_snd = req_id.res_id_snd;
        let res_rsp = match req {
            KfkReq::Send { records } => {
                cli.produce(records, Compression::Snappy).await
                    .map(|offsets| KfkRsp::send(offsets))
            }
            KfkReq::Fetch { offset, bytes, max_wait_ms } => {
                cli.fetch_records(offset, bytes, max_wait_ms).await
                    .map(|(recs_and_offsets, highwater_mark)| KfkRsp::fetch(recs_and_offsets, highwater_mark))
            }
            KfkReq::Offset { at } => {
                cli.get_offset(at).await.map(|offset| KfkRsp::Offset(offset))
            }
        };
        let res_id = KfkResId::new(id, res_rsp);
        debug!("kafka, client: {}/{}, response: {:?}", cli.topic(), cli.partition(), res_id);
        res_id_snd.send(res_id).await?;
    }
    info!("kafka, client: {}/{}, end", cli.topic(), cli.partition());
    Ok(())
}

#[derive(Eq, Hash, PartialEq)]
struct KfkClientKey {
    pub topic: String,
    pub partition: i32,
}

impl KfkClientKey {
    fn new(topic: String, partition: i32) -> Self {
        KfkClientKey { topic, partition }
    }
}

impl Clone for KfkClientKey {
    fn clone(&self) -> Self {
        KfkClientKey::new(self.topic.clone(), self.partition)
    }
}

pub struct KfkClientCache {
    client: Client,
    cache: Cache<KfkClientKey, KfkReqIdSnd>,
}

impl KfkClientCache {

    pub fn new(client: Client, capacity: u64) -> Self {
        Self { client, cache: Cache::new(capacity) }
    }

    async fn init_kafka_loop(&self, key: &KfkClientKey, capacity: usize) -> JrpkResult<KfkReqIdSnd> {
        info!("kafka, init: {}:{}, capacity: {}", key.topic, key.partition, capacity);
        let pc = self.client.partition_client( key.topic.as_str(), key.partition, UnknownTopicHandling::Error).await?;
        let (req_id_snd, req_id_rcv) = mpsc::channel(capacity);
        tokio::spawn(handle_future("kafka", run_kafka_loop(pc, req_id_rcv)));
        Ok(req_id_snd)
    }

    pub async fn lookup_kafka_sender(&self, topic: String, partition: i32, capacity: usize) -> JrpkResult<KfkReqIdSnd> {
        debug!("kafka, lookup: {}:{}", topic, partition);
        let key = KfkClientKey::new(topic, partition);
        let init = self.init_kafka_loop(&key, capacity);
        let req_id_snd = self.cache.try_get_with_by_ref(&key, init).await?;
        Ok(req_id_snd)
    }
}
