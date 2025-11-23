use crate::codec::{JsonCodec, MeteredItem};
use crate::jsonrpc::{JrpCtx, JrpDataCodecs, JrpData, JrpExtra, JrpId, JrpMethod, JrpOffset, JrpRecFetch, JrpRecSend, JrpReq, JrpRsp, JrpRspData};
use crate::kafka::{KfkClientCache, KfkOffset, KfkReq, KfkResCtx, KfkResCtxRcv, KfkResCtxSnd, KfkRsp};
use crate::util::{set_buf_sizes, ReqCtx};
use base64::DecodeError;
use chrono::Utc;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rskafka::client::ClientBuilder;
use rskafka::record::{Record, RecordAndOffset};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use prometheus_client::registry::Registry;
use rskafka::client::partition::OffsetAt;
use tokio::net::{TcpListener, TcpStream};
use tokio::{spawn, try_join};
use tokio::sync::mpsc;
use tokio_util::codec::{Framed};
use tracing::{error, info, instrument, trace, warn};
use ustr::Ustr;
use crate::error::JrpkError;
use crate::metrics::{JrpkMeter, JrpkMeters, Meter, ERROR, FETCH, OFFSET, READ, SEND, SERVER, TCP, WRITE};

fn j2k_rec_send(jrp_rec_send: JrpRecSend) -> Result<Record, DecodeError> {
    let key: Option<Vec<u8>> = jrp_rec_send.key.map(|k| k.into_bytes()).transpose()?;
    let value: Option<Vec<u8>> = jrp_rec_send.value.map(|v| v.into_bytes()).transpose()?;
    Ok(Record { key, value, headers: BTreeMap::new(), timestamp: Utc::now() })
}

fn k2j_rec_fetch(rec_and_offset: RecordAndOffset, codecs: &JrpDataCodecs) -> Result<JrpRecFetch<'static>, JrpkError> {
    let offset = rec_and_offset.offset;
    let record = rec_and_offset.record;
    let timestamp = record.timestamp;
    let key = record.key.map(|k|JrpData::from_bytes(k, codecs.key)).transpose()?;
    let value = record.value.map(|v|JrpData::from_bytes(v, codecs.value)).transpose()?;
    Ok(JrpRecFetch::new(offset, timestamp, key, value))
}

fn k2j_rsp(rsp: KfkRsp, extra: JrpExtra) -> Result<JrpRspData<'static>, JrpkError> {
    match (rsp, extra) {
        (KfkRsp::Send { offsets }, _) => {
            Ok(JrpRspData::send(offsets))
        }
        (KfkRsp::Fetch { recs_and_offsets, high_watermark }, JrpExtra::DataCodecs(codecs)) => {
            let res: Result<Vec<JrpRecFetch>, JrpkError> = recs_and_offsets.into_iter()
                .map(|ro| { k2j_rec_fetch(ro, &codecs) }).collect();
            let records = res?;
            Ok(JrpRspData::fetch(records, high_watermark))
        }
        (KfkRsp::Offset(offset), _) => {
            Ok(JrpRspData::Offset(offset))
        }
        (k, e) => {
            error!("unexpected, response: {:?}, extra: {:?}", k, e);
            Err(JrpkError::Unexpected("Unexpected state"))
        }
    }
}

fn j2k_offset(offset: JrpOffset) -> KfkOffset {
    match offset {
        JrpOffset::Earliest => KfkOffset::Implicit(OffsetAt::Earliest),
        JrpOffset::Latest => KfkOffset::Implicit(OffsetAt::Latest),
        JrpOffset::Timestamp(ts) => KfkOffset::Implicit(OffsetAt::Timestamp(ts)),
        JrpOffset::Offset(pos) => KfkOffset::Explicit(pos)
    }
}

fn j2k_req(jrp: JrpReq) -> Result<(usize, Ustr, i32, KfkReq, JrpExtra), JrpkError> {
    let id = jrp.id;
    let topic = jrp.params.topic;
    let partition = jrp.params.partition;
    match jrp.method {
        JrpMethod::Send => {
            let jrp_records = jrp.params.records
                .ok_or(JrpkError::Syntax("records is missing"))?;
            let records: Result<Vec<Record>, DecodeError> = jrp_records.into_iter()
                .map(|jrs| j2k_rec_send(jrs))
                .collect();
            Ok((id, topic, partition, KfkReq::send(records?), JrpExtra::None))
        }
        JrpMethod::Fetch => {
            let codecs = jrp.params.codecs.ok_or(JrpkError::Syntax("codecs are missing"))?;
            let offset = jrp.params.offset.ok_or(JrpkError::Syntax("offset is missing"))?;
            let bytes = jrp.params.bytes.ok_or(JrpkError::Syntax("bytes is missing"))?;
            let max_wait_ms = jrp.params.max_wait_ms.ok_or(JrpkError::Syntax("max_wait_ms is missing"))?;
            Ok((id, topic, partition, KfkReq::fetch(j2k_offset(offset), bytes, max_wait_ms), JrpExtra::DataCodecs(codecs)))
        }
        JrpMethod::Offset => {
            let offset = jrp.params.offset.ok_or(JrpkError::Syntax("offset is missing"))?;
            Ok((id, topic, partition, KfkReq::offset(j2k_offset(offset)), JrpExtra::None))
        }
    }
}

#[instrument(ret, err, skip(tcp_stream, client_cache, kfk_res_ctx_snd))]
async fn server_req_reader(
    mut tcp_stream: SplitStream<Framed<TcpStream, JsonCodec>>,
    client_cache: Arc<KfkClientCache<JrpCtx>>,
    kfk_res_ctx_snd: KfkResCtxSnd<JrpCtx>,
    queue_size: usize,
    meters: JrpkMeters,
) -> Result<(), JrpkError> {
    let send_meter = meters.meter(SERVER, SEND, TCP, READ);
    let fetch_meter = meters.meter(SERVER, FETCH, TCP, READ);
    let offset_meter = meters.meter(SERVER, OFFSET, TCP, READ);
    let error_meter = meters.meter(SERVER, ERROR, TCP, READ);
    while let Some(result) = tcp_stream.next().await {
        // if we cannot even decode frame - we disconnect
        let bytes = result?;
        let length = bytes.len();
        trace!("json: {}", from_utf8(bytes.as_ref())?);
        // we are optimistic and expect most requests to be well-formed
        match serde_json::from_slice::<JrpReq>(bytes.as_ref()) {
            // if request is well-formed, we proceed
            Ok(jrp_req) => {
                trace!("request: {:?}", jrp_req);
                let (id, topic, partition, kfk_req, extra) = j2k_req(jrp_req)?;
                match kfk_req {
                    KfkReq::Send { .. } => send_meter.meter(length, None),
                    KfkReq::Fetch { .. } => fetch_meter.meter(length, None),
                    KfkReq::Offset { .. } => offset_meter.meter(length, None),
                }
                let jrp_ctx = JrpCtx::new(id, extra);
                let kfk_req_ctx = ReqCtx::new(jrp_ctx, kfk_req, kfk_res_ctx_snd.clone());
                let kfk_req_ctx_snd = client_cache.lookup_kafka_sender(topic, partition, queue_size).await?;
                kfk_req_ctx_snd.send(kfk_req_ctx).await?;
            }
            // if request is not well-formed we attempt to get at least and id to respond
            // and send decode error directly to the result channel, without round-trip to kafka client
            Err(err) => {
                warn!("jsonrpc decode error: {}", err);
                // if even an id is absent we give up and disconnect
                let jrp_id = serde_json::from_slice::<JrpId>(bytes.as_ref())?;
                let jrp_ctx = JrpCtx::new(jrp_id.id, JrpExtra::None);
                let kfk_err = JrpkError::Internal(format!("jsonrpc decode error: {}", err));
                let kfk_res = KfkResCtx::err(jrp_ctx, kfk_err);
                error_meter.meter(length, None);
                kfk_res_ctx_snd.send(kfk_res).await?;
            }
        }
    }
    Ok(())
}

type JrpkMeteredItem = MeteredItem<JrpRsp<'static>, JrpkMeter>;

#[instrument(ret, err, skip(tcp_sink, kfk_res_ctx_rcv))]
async fn server_rsp_writer(
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpkMeteredItem>,
    mut kfk_res_ctx_rcv: KfkResCtxRcv<JrpCtx>,
    meters: JrpkMeters,
) -> Result<(), JrpkError> {

    let send_meter = meters.meter(SERVER, SEND, TCP, WRITE);
    let fetch_meter = meters.meter(SERVER, FETCH, TCP, WRITE);
    let offset_meter = meters.meter(SERVER, OFFSET, TCP, WRITE);
    let error_meter = meters.meter(SERVER, ERROR, TCP, WRITE);

    while let Some(kfk_res_ctx) = kfk_res_ctx_rcv.recv().await {
        trace!("response: {:?}", kfk_res_ctx);
        let ctx = kfk_res_ctx.ctx;
        let (jrp_rsp, meter) = match kfk_res_ctx.res {
            Ok(kfk_rsp) => {
                match k2j_rsp(kfk_rsp, ctx.extra) {
                    Ok(jrp_rst_data) => {
                        let meter = match jrp_rst_data {
                            JrpRspData::Send { .. } => send_meter.clone(),
                            JrpRspData::Fetch { .. } => fetch_meter.clone(),
                            JrpRspData::Offset(_) => offset_meter.clone(),
                        };
                        (JrpRsp::result(ctx.id, jrp_rst_data), meter)
                    },
                    Err(err) => {
                        (JrpRsp::err(ctx.id, err.into()), error_meter.clone())
                    }
                }
            }
            Err(err) => {
                (JrpRsp::err(ctx.id, err.into()), error_meter.clone())
            }
        };
        let metered_item = JrpkMeteredItem::new(jrp_rsp, meter, Some(ctx.ts));
        tcp_sink.send(metered_item).await?;
    }
    tcp_sink.flush().await?;
    Ok(())
}

#[instrument(ret, err, skip(client_cache, tcp_stream))]
async fn serve_jsonrpc(
    tcp_stream: TcpStream,
    client_cache: Arc<KfkClientCache<JrpCtx>>,
    max_frame_size: usize,
    send_buf_size: usize,
    recv_buf_size: usize,
    queue_size: usize,
    meters: JrpkMeters,
) -> Result<(), JrpkError> {
    set_buf_sizes(&tcp_stream, recv_buf_size, send_buf_size)?;
    let codec = JsonCodec::new(max_frame_size);
    let framed = Framed::new(tcp_stream, codec);
    let (tcp_sink, tcp_stream) = framed.split();
    let (kfk_res_snd, kfk_res_rcv) = mpsc::channel::<KfkResCtx<JrpCtx>>(queue_size);
    let rh = spawn(
        server_req_reader(
            tcp_stream,
            client_cache,
            kfk_res_snd,
            queue_size,
            meters.clone(),
        )
    );
    let wh = spawn(
        server_rsp_writer(
            tcp_sink,
            kfk_res_rcv,
            meters.clone(),
        )
    );
    let _ = try_join!(rh, wh)?;
    Ok(())
}

#[instrument(ret, err)]
pub async fn listen_jsonrpc(
    brokers: Vec<String>,
    bind: SocketAddr,
    max_frame_size: usize,
    send_buf_size: usize,
    recv_buf_size: usize,
    queue_size: usize,
    registry: Arc<Mutex<Registry>>,
) -> Result<(), JrpkError> {

    let meters = {
        let mut rg = registry.lock().unwrap();
        JrpkMeters::new(&mut rg)
    };

    info!("connect: {}", brokers.join(","));
    let client = ClientBuilder::new(brokers).build().await?;
    let client_cache: Arc<KfkClientCache<JrpCtx>> = Arc::new(KfkClientCache::new(client, 1024, meters.clone()));
    info!("bind: {:?}", bind);
    let listener = TcpListener::bind(bind).await?;
    loop {
        let (tcp_stream, addr) = listener.accept().await?;
        info!("accepted: {:?}", addr);
        spawn(
            serve_jsonrpc(
                tcp_stream,
                client_cache.clone(),
                max_frame_size,
                send_buf_size,
                recv_buf_size,
                queue_size,
                meters.clone(),
            )
        );
    }
}
