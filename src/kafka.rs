use std::future::Future;
use rskafka::record::{Record, RecordAndOffset};
use std::ops::Range;
use moka::future::Cache;
use rskafka::client::Client;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, trace};
use crate::jsonrpc::{JrpParams, JrpRequest};
use crate::util::{handle_future, unwrap_err, Request, Response};

pub enum KfkRequest {
    Send {
        records: Vec<Record>
    },
    Fetch {
        offset: i64,
        bytes: Range<i32>,
        max_wait_ms: i32,
    }
}

pub enum KfkResponse {
    Send {
        offsets: Vec<i64>,
    },
    Fetch {
        records_and_offsets: Vec<RecordAndOffset>,
        high_watermark: i64,
    }
}

pub type KfkError = rskafka::client::error::Error;

pub type KfkResponseSender = Sender<Response<KfkResponse, KfkError>>;
pub type KfkResponseReceiver = Receiver<Response<KfkResponse, KfkError>>;
pub type KfkRequestSender = Sender<Request<KfkRequest, KfkResponse, KfkError>>;
pub type KfkRequestReceiver = Receiver<Request<KfkRequest, KfkResponse, KfkError>>;

async fn run_kafka_loop(pc: PartitionClient, mut rcv: KfkRequestReceiver) -> anyhow::Result<()> {
    info!("loop, topic: {}, partition: {} - START", pc.topic(), pc.partition());
    while let Some((id, req, snd)) = rcv.recv().await {
        match req {
            KfkRequest::Send { records } => {
                match pc.produce(records, Compression::Gzip).await {
                    Ok(offsets) => {
                        let rsp = KfkResponse::Send { offsets };
                        snd.send((id, Ok(rsp))).await?;
                    }
                    Err(error) => {
                        snd.send((id, Err(error))).await?;
                    }
                }
            },
            KfkRequest::Fetch { offset, bytes, max_wait_ms } => {
                match pc.fetch_records(offset, bytes, max_wait_ms).await {
                    Ok((records_and_offsets, high_watermark)) => {
                        let rps = KfkResponse::Fetch { records_and_offsets, high_watermark };
                        snd.send((id, Ok(rps))).await?;
                    }
                    Err(error) => {
                        snd.send((id, Err(error))).await?;
                    }
                }
            }
        }
    }
    info!("loop, topic: {}, partition: {} - END", pc.topic(), pc.partition());
    Ok(())
}

pub struct KfkClientCache {
    client: Client,
    cache: Cache<(String, i32), Sender<Request<KfkRequest, KfkResponse, KfkError>>>,
}

impl KfkClientCache {
    pub fn new(client: Client, capacity: u64) -> Self {
        Self { client, cache: Cache::new(capacity) }
    }

    async fn init_kafka_loop(&self, topic: String, partition: i32, capacity: usize) -> anyhow::Result<KfkRequestSender> {
        info!("init, topic: {}, partition: {}, capacity: {}", topic, partition, capacity);
        let pc = self.client.partition_client(topic, partition, UnknownTopicHandling::Retry).await?;
        let (snd, rcv) = mpsc::channel(capacity);
        tokio::spawn(
            handle_future(
                run_kafka_loop(pc, rcv)
            )
        );
        Ok(snd)
    }

    pub async fn lookup_kafka_sender(&self, topic: String, partition: i32, capacity: usize) -> anyhow::Result<Sender<Request<KfkRequest, KfkResponse, KfkError>>> {
        trace!("lookup, topic: {}, partition: {}", topic, partition);
        let init = self.init_kafka_loop(topic.clone(), partition, capacity);
        self.cache.try_get_with((topic, partition), init).await
            .map_err(unwrap_err)
    }
}
