use crate::codec::{BytesFrameDecoderError, JsonCodec, JsonEncoderError};
use crate::jsonrpc::{JrpCtx, JrpDataCodecs, JrpData, JrpError, JrpErrorMsg, JrpExtra, JrpId, JrpMethod, JrpOffset, JrpParams, JrpRecFetch, JrpRecSend, JrpReq, JrpRsp, JrpRspData};
use crate::kafka::{KfkClientCache, KfkError, KfkOffset, KfkReq, KfkResCtx, KfkResCtxRcv, KfkResCtxSnd, KfkRsp, RsKafkaError};
use crate::util::{handle_future_result, set_buf_sizes, ReqCtx};
use base64::DecodeError;
use chrono::Utc;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt, Sink};
use rskafka::client::ClientBuilder;
use rskafka::record::{Record, RecordAndOffset};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::str::{from_utf8, Utf8Error};
use std::sync::Arc;
use bytesize::ByteSize;
use rskafka::client::partition::OffsetAt;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, trace, warn};

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("decoder: {0}")]
    Frame(#[from] BytesFrameDecoderError),
    #[error("encoder: {0}")]
    Encoder(#[from] JsonEncoderError),
    #[error("utf8: {0}")]
    Utf8(#[from] Utf8Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::error::Error),
    #[error("kafka: {0}")]
    Kafka(#[from] KfkError),
    #[error("jsonrpc: {0}")]
    Jrp(#[from] JrpError),
    #[error("send: {0}")]
    Send(SendError<()>),
    #[error("rs kafka: {0}")]
    Rs(#[from] RsKafkaError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("base64: {0}")]
    Base64(#[from] DecodeError),
    #[error("unexpected: {0}")]
    Unexpected(&'static str),
}

/// deliberately drop payload
impl <T> From<SendError<T>> for ServerError {
    fn from(_: SendError<T>) -> Self {
        ServerError::Send(SendError(()))
    }
}

fn j2k_rec_send(jrp_rec_send: JrpRecSend) -> Result<Record, DecodeError> {
    let key: Option<Vec<u8>> = jrp_rec_send.key.map(|k| k.into_bytes()).transpose()?;
    let value: Option<Vec<u8>> = jrp_rec_send.value.map(|v| v.into_bytes()).transpose()?;
    Ok(Record { key, value, headers: BTreeMap::new(), timestamp: Utc::now() })
}

fn k2j_rec_fetch(rec_and_offset: RecordAndOffset, codecs: &JrpDataCodecs) -> Result<JrpRecFetch<'static>, JrpError> {
    let offset = rec_and_offset.offset;
    let record = rec_and_offset.record;
    let key = record.key.map(|k|JrpData::from_bytes(k, codecs.key)).transpose()?;
    let value = record.value.map(|v|JrpData::from_bytes(v, codecs.value)).transpose()?;
    Ok(JrpRecFetch::new(offset, key, value))
}

fn k2j_rsp(rsp: KfkRsp, extra: JrpExtra) -> Result<JrpRspData<'static>, ServerError> {
    match (rsp, extra) {
        (KfkRsp::Send { offsets }, _) => {
            Ok(JrpRspData::send(offsets))
        }
        (KfkRsp::Fetch { recs_and_offsets, high_watermark }, JrpExtra::DataCodecs(codecs)) => {
            let res: Result<Vec<JrpRecFetch>, JrpError> = recs_and_offsets.into_iter()
                .map(|ro| { k2j_rec_fetch(ro, &codecs) }).collect();
            let records = res?;
            Ok(JrpRspData::fetch(records, high_watermark))
        }
        (KfkRsp::Offset(offset), _) => {
            Ok(JrpRspData::Offset(offset))
        }
        (k, e) => {
            error!("unexpected, response: {:?}, extra: {:?}", k, e);
            Err(ServerError::Unexpected("Unexpected state"))
        }
    }
}

/*#[inline]
fn x2j_err<E: Error>(err: E) -> JrpErrorMsg {
    JrpErrorMsg::new(err.to_string())
}
*/
fn j2k_offset(offset: JrpOffset) -> KfkOffset {
    match offset {
        JrpOffset::Earliest => KfkOffset::Implicit(OffsetAt::Earliest),
        JrpOffset::Latest => KfkOffset::Implicit(OffsetAt::Latest),
        JrpOffset::Timestamp(ts) => KfkOffset::Implicit(OffsetAt::Timestamp(ts)),
        JrpOffset::Offset(pos) => KfkOffset::Explicit(pos)
    }
}

fn j2k_req(jrp: JrpReq) -> Result<(usize, String, i32, KfkReq, JrpExtra), ServerError> {
    let id = jrp.id;
    let topic = jrp.params.topic.into_owned();
    let partition = jrp.params.partition;
    match jrp.method {
        JrpMethod::Send => {
            let jrp_records = jrp.params.records
                .ok_or(JrpError::Syntax("records is missing"))?;
            let records: Result<Vec<Record>, DecodeError> = jrp_records.into_iter()
                .map(|jrs| j2k_rec_send(jrs))
                .collect();
            Ok((id, topic, partition, KfkReq::send(records?), JrpExtra::None))
        }
        JrpMethod::Fetch => {
            let codecs = jrp.params.codecs.ok_or(JrpError::Syntax("codecs are missing"))?;
            let offset = jrp.params.offset.ok_or(JrpError::Syntax("offset is missing"))?;
            let bytes = jrp.params.bytes.ok_or(JrpError::Syntax("bytes is missing"))?;
            let max_wait_ms = jrp.params.max_wait_ms.ok_or(JrpError::Syntax("max_wait_ms is missing"))?;
            Ok((id, topic, partition, KfkReq::fetch(j2k_offset(offset), bytes, max_wait_ms), JrpExtra::DataCodecs(codecs)))
        }
        JrpMethod::Offset => {
            let offset = jrp.params.offset.ok_or(JrpError::Syntax("offset is missing"))?;
            Ok((id, topic, partition, KfkReq::offset(j2k_offset(offset)), JrpExtra::None))
        }
    }
}

async fn run_reader_loop(
    addr: SocketAddr,
    mut stream: SplitStream<Framed<TcpStream, JsonCodec>>,
    cache: Arc<KfkClientCache<JrpCtx>>,
    kfk_res_ctx_snd: KfkResCtxSnd<JrpCtx>,
    queue_size: usize,
) -> Result<(), ServerError> {

    while let Some(result) = stream.next().await {
        // if we cannot even decode frame - we disconnect
        let bytes = result?;
        debug!("reader, ctx: {}, json: {}", addr, from_utf8(bytes.as_ref())?);
        // we are optimistic and expect most requests to be well-formed
        match serde_json::from_slice::<JrpReq>(bytes.as_ref()) {
            // if request is well-formed, we proceed
            Ok(jrp_req) => {
                debug!("reader, ctx: {}, request: {:?}", addr, jrp_req);
                let (id, topic, partition, kfk_req, extra) = j2k_req(jrp_req)?;
                let jrp_ctx = JrpCtx::new(id, extra);
                let kfk_req_ctx = ReqCtx::new(jrp_ctx, kfk_req, kfk_res_ctx_snd.clone());
                let kfk_req_ctx_snd = cache.lookup_kafka_sender(topic, partition, queue_size).await?;
                kfk_req_ctx_snd.send(kfk_req_ctx).await?;
            }
            // if request is not well-formed we attempt to get at least and id to respond
            // and send decode error directly to the result channel, without round-trip to kafka client
            Err(err) => {
                warn!("jsonrpc decode error: {}", err);
                // if even an id is absent we give up and disconnect
                let jrp_id = serde_json::from_slice::<JrpId>(bytes.as_ref())?;
                let jrp_ctx = JrpCtx::new(jrp_id.id, JrpExtra::None);
                let kfk_err = KfkError::General(format!("jsonrpc decode error: {}", err));
                let kfk_res = KfkResCtx::err(jrp_ctx, kfk_err);
                kfk_res_ctx_snd.send(kfk_res).await?;
            }
        }
    }
    Ok(())

}

async fn run_writer_loop(
    addr: SocketAddr,
    mut sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpRsp<'static>>,
    mut kfk_res_ctx_rcv: KfkResCtxRcv<JrpCtx>
) -> Result<(), ServerError> {
    while let Some(kfk_res_ctx) = kfk_res_ctx_rcv.recv().await {
        info!("writer, ctx: {}, response: {:?}", addr, kfk_res_ctx);
        let ctx = kfk_res_ctx.ctx;
        let jrp_rsp = match kfk_res_ctx.res {
            Ok(kfk_rsp) => {
                match k2j_rsp(kfk_rsp, ctx.extra) {
                    Ok(jrp_rsp) => JrpRsp::ok(ctx.id, jrp_rsp),
                    Err(err) => JrpRsp::err(ctx.id, err.into())
                }
            }
            Err(err) => {
                JrpRsp::err(ctx.id, err.into())
            }
        };
        sink.send(jrp_rsp).await?;
    }
    sink.flush().await?;
    Ok(())
}

pub async fn listen(
    brokers: Vec<String>,
    bind: SocketAddr,
    max_frame_size: ByteSize,
    send_buf_size: ByteSize,
    recv_buf_size: ByteSize,
    queue_size: usize
) -> Result<(), ServerError> {

    info!("listen, kafka: {:?}", brokers);
    let client = ClientBuilder::new(brokers).build().await?;
    let cache: Arc<KfkClientCache<JrpCtx>> = Arc::new(KfkClientCache::new(client, 1024));

    info!("listen, bind: {:?}", bind);
    let listener = TcpListener::bind(bind).await?;

    loop {
        let (tcp, addr) = listener.accept().await?;
        info!("listen, accepted: {:?}", addr);
        set_buf_sizes(&tcp, recv_buf_size.as_u64() as usize, send_buf_size.as_u64() as usize)?;
        let codec = JsonCodec::new(max_frame_size.as_u64() as usize);
        let framed = Framed::new(tcp, codec);
        let (sink, stream) = framed.split();
        let (kfk_res_snd, kfk_res_rcv) = mpsc::channel::<KfkResCtx<JrpCtx>>(queue_size);
        tokio::spawn(
            handle_future_result(
                "reader",
                addr,
                run_reader_loop(addr, stream, cache.clone(), kfk_res_snd, queue_size)
            )
        );
        tokio::spawn(
            handle_future_result(
                "writer",
                addr,
                run_writer_loop(addr, sink, kfk_res_rcv)
            )
        );
    }
}
