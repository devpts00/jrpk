use std::fmt::{Debug, Formatter};
use crate::util::{debug_vec_fn, handle_future, ReqId, ResId, debug_record_and_offset};
use moka::future::Cache;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::{Record, RecordAndOffset};
use std::ops::Range;
use std::sync::Arc;
use base64::DecodeError;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tracing::{debug, info, trace, warn};
use crate::jsonrpc::{JrpError, JrpMethod, JrpReq};

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
    type Error = JrpError;
    fn try_from(req: JrpReq<'a>) -> Result<Self, Self::Error> {
        match req.method {
            JrpMethod::Send => {
                let jrp_records = req.params.records
                    .ok_or(JrpError::Syntax("records is missing"))?;
                let records: Result<Vec<Record>, DecodeError> = jrp_records.into_iter()
                    .map(|x| x.try_into())
                    .collect();
                Ok(KfkReq::send(records?))
            }
            JrpMethod::Fetch => {
                let offset = req.params.offset.ok_or(JrpError::Syntax("offset is missing"))?;
                let bytes = req.params.bytes.ok_or(JrpError::Syntax("bytes is missing"))?;
                let max_wait_ms = req.params.max_wait_ms.ok_or(JrpError::Syntax("max_wait_ms is missing"))?;
                Ok(KfkReq::fetch(offset, bytes, max_wait_ms))
            }
            JrpMethod::Offset => {
                let at = req.params.at.ok_or(JrpError::Syntax("at is missing"))?;
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

pub type RsKafkaError = rskafka::client::error::Error;
pub type KfkReqId = ReqId<KfkReq, KfkRsp, RsKafkaError>;
pub type KfkResId = ResId<KfkRsp, RsKafkaError>;
pub type KfkResIdSnd = Sender<KfkResId>;
pub type KfkResIdRcv = Receiver<KfkResId>;
pub type KfkReqIdSnd = Sender<KfkReqId>;
pub type KfkReqIdRcv = Receiver<KfkReqId>;

#[derive(Error, Debug)]
pub enum KfkError {
    #[error("rs kafka: {0}")]
    Rs(#[from] RsKafkaError),

    #[error("send: {0}")]
    Send(SendError<()>),

    #[error("{0}")]
    Wrapped(#[from] Arc<KfkError>),
}

/// deliberately drop payload
impl <T> From<tokio::sync::mpsc::error::SendError<T>> for KfkError {
    fn from(value: SendError<T>) -> Self {
        KfkError::Send(tokio::sync::mpsc::error::SendError(()))
    }
}

async fn run_kafka_loop(cli: PartitionClient, mut req_id_rcv: KfkReqIdRcv) -> Result<(), KfkError> {
    info!("kafka, client: {}/{} - START", cli.topic(), cli.partition());
    while let Some(req_id) = req_id_rcv.recv().await {
        trace!("kafka, client: {}/{}, request: {:?}", cli.topic(), cli.partition(), req_id);
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
        trace!("kafka, client: {}/{}, response: {:?}", cli.topic(), cli.partition(), res_id);
        res_id_snd.send(res_id).await?;
    }
    info!("kafka, client: {}/{} - END", cli.topic(), cli.partition());
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

    async fn init_kafka_loop(&self, key: &KfkClientKey, capacity: usize) -> Result<KfkReqIdSnd, KfkError> {
        info!("kafka, init: {}:{}, capacity: {}", key.topic, key.partition, capacity);
        let pc = self.client.partition_client( key.topic.as_str(), key.partition, UnknownTopicHandling::Error).await?;
        let (req_id_snd, req_id_rcv) = mpsc::channel(capacity);
        tokio::spawn(handle_future("kafka", run_kafka_loop(pc, req_id_rcv)));
        Ok(req_id_snd)
    }

    pub async fn lookup_kafka_sender(&self, topic: String, partition: i32, capacity: usize) -> Result<KfkReqIdSnd, KfkError> {
        trace!("kafka, lookup: {}:{}", topic, partition);
        let key = KfkClientKey::new(topic, partition);
        let init = self.init_kafka_loop(&key, capacity);
        let req_id_snd = self.cache.try_get_with_by_ref(&key, init).await?;
        Ok(req_id_snd)
    }
}
