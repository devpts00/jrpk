use std::fmt::{Display, Formatter};
use crate::util::{display_record, display_rec_and_offset, display_slice_bytes, display_vec, display_vec_fn, handle_future, unwrap_err, ReqId, ResId};
use moka::future::Cache;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use rskafka::record::{Record, RecordAndOffset};
use std::ops::Range;
use std::str::{from_utf8, Utf8Error};
use anyhow::anyhow;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, trace};
use crate::jsonrpc::{JrpMethod, JrpReq};


pub enum KfkReq {
    Send {
        records: Vec<Record>
    },
    Fetch {
        offset: i64,
        bytes: Range<i32>,
        max_wait_ms: i32,
    }
}

impl Display for KfkReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ ")?;
        match self {
            KfkReq::Send { records } => {
                write!(f, "send, records: [")?;
                let mut comma = false;
                for r in records.iter() {
                    if comma {
                        write!(f, ", ")?;
                    }
                    display_record(f, r, true)?;
                    comma = true;
                }
                write!(f, "]")?;
            },
            KfkReq::Fetch { offset, bytes, max_wait_ms } => {
                write!(f, "fetch, offset: {}, bytes: [{}..{}), max_wait_ms: {}", offset, bytes.start, bytes.end, max_wait_ms)?;
            }
        }
        write!(f, " }}")
    }
}

impl KfkReq {
    pub fn send(records: Vec<Record>) -> Self {
        KfkReq::Send { records }
    }
    pub fn fetch(offset: i64, bytes: Range<i32>, max_wait_ms: i32) -> Self {
        KfkReq::Fetch { offset, bytes, max_wait_ms }
    }
}

impl <'a> TryFrom<JrpReq<'a>> for KfkReq {
    type Error = anyhow::Error;
    fn try_from(req: JrpReq<'a>) -> Result<Self, Self::Error> {
        match req.method {
            JrpMethod::Send => {
                let jrp_records = req.params.records
                    .ok_or(anyhow!("records is missing"))?;
                let records: Vec<Record> = jrp_records.into_iter()
                    .map(|x| x.into())
                    .collect();
                Ok(KfkReq::send(records))
            }
            JrpMethod::Fetch => {
                let offset = req.params.offset.ok_or(anyhow!("offset is missing"))?;
                let bytes = req.params.bytes.ok_or(anyhow!("bytes is missing"))?;
                let max_wait_ms = req.params.max_wait_ms.ok_or(anyhow!("max_wait_ms is missing"))?;
                Ok(KfkReq::fetch(offset, bytes, max_wait_ms))
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
    }
}

impl KfkRsp {
    fn send(offsets: Vec<i64>) -> Self {
        KfkRsp::Send { offsets }
    }
    fn fetch(recs_and_offsets: Vec<RecordAndOffset>, high_watermark: i64) -> Self {
        KfkRsp::Fetch { recs_and_offsets, high_watermark }
    }
}

impl Display for KfkRsp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KfkRsp::Send { offsets } => {
                write!(f, "response: send, offsets: {:?}", offsets)
            }
            KfkRsp::Fetch { recs_and_offsets, high_watermark } => {
                write!(f, "response: fetch, records: ")?;
                display_vec_fn(f, recs_and_offsets, |f, ro| display_rec_and_offset(f, ro))?;
                write!(f, ", high_watermark: {}", high_watermark)
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

async fn run_kafka_loop(partition_client: PartitionClient, mut req_id_rcv: KfkReqIdRcv) -> anyhow::Result<()> {
    info!("kafka, start: {}:{}", partition_client.topic(), partition_client.partition());
    while let Some(req_id) = req_id_rcv.recv().await {
        debug!("kafka, request: {}", req_id);
        let id = req_id.id;
        let req = req_id.req;
        let res_id_snd = req_id.res_id_snd;
        let res_rsp = match req {
            KfkReq::Send { records } => {
                partition_client.produce(records, Compression::Gzip).await
                    .map(|offsets| KfkRsp::send(offsets))
            }
            KfkReq::Fetch { offset, bytes, max_wait_ms } => {
                partition_client.fetch_records(offset, bytes, max_wait_ms).await
                    .map(|(recs_and_offsets, highwater_mark)| KfkRsp::fetch(recs_and_offsets, highwater_mark))
            }
        };
        let res_id = KfkResId::new(id, res_rsp);
        debug!("kafka, response: {}", res_id);
        res_id_snd.send(res_id).await?;
    }
    info!("kafka, end: {}:{}", partition_client.topic(), partition_client.partition());
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

    async fn init_kafka_loop(&self, key: &KfkClientKey, capacity: usize) -> anyhow::Result<KfkReqIdSnd> {
        info!("kafka, init: {}:{}, capacity: {}", key.topic, key.partition, capacity);
        let pc = self.client.partition_client( key.topic.as_str(), key.partition, UnknownTopicHandling::Error).await?;
        let (snd, rcv) = mpsc::channel(capacity);
        tokio::spawn(
            handle_future(
                run_kafka_loop(pc, rcv)
            )
        );
        Ok(snd)
    }

    pub async fn lookup_kafka_sender(&self, topic: String, partition: i32, capacity: usize) -> anyhow::Result<KfkReqIdSnd> {
        debug!("kafka, lookup: {}:{}", topic, partition);
        let key = KfkClientKey::new(topic, partition);
        let init = self.init_kafka_loop(&key, capacity);
        self.cache.try_get_with_by_ref(&key, init).await
            .map_err(unwrap_err)
    }
}
