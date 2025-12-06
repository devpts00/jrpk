use crate::codec::{JsonCodec, MeteredItem};
use crate::model::{JrpCodecs, JrpData, JrpId, JrpMethod, JrpOffset, JrpRecFetch, JrpRecSend, JrpReq, JrpRsp, JrpRspData};
use crate::kafka::{KfkClientCache, KfkOffset, KfkReq, KfkResCtx, KfkResCtxRcv, KfkResCtxSnd, KfkRsp};
use crate::util::{set_buf_sizes, ReqCtx, Tap};
use base64::DecodeError;
use chrono::Utc;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rskafka::record::{Record, RecordAndOffset};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::ops::Range;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use prometheus_client::registry::Registry;
use rskafka::client::partition::OffsetAt;
use tokio::net::{TcpListener, TcpStream};
use tokio::{select, spawn, try_join};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::{Framed};
use tracing::{info, instrument, trace, warn};
use crate::error::JrpkError;
use crate::metrics::{JrpkMetrics, Labels, LblTier, LblTraffic};

#[derive(Debug)]
pub struct SrvCtx {
    pub id: usize,
    pub method: JrpMethod,
    pub tap: Tap,
    pub ts: Instant,
}

impl SrvCtx {
    pub fn new(id: usize, method: JrpMethod, tap: Tap) -> Self {
        SrvCtx { id, method, tap, ts: Instant::now() }
    }
}

#[inline]
fn j2k_rec_send(jrp_rec_send: JrpRecSend) -> Result<Record, DecodeError> {
    let key: Option<Vec<u8>> = jrp_rec_send.key.map(|k| k.into_bytes()).transpose()?;
    let value: Option<Vec<u8>> = jrp_rec_send.value.map(|v| v.into_bytes()).transpose()?;
    Ok(Record { key, value, headers: BTreeMap::new(), timestamp: Utc::now() })
}

#[inline]
fn k2j_rec_fetch(rec_and_offset: RecordAndOffset, codecs: &JrpCodecs) -> Result<JrpRecFetch<'static>, JrpkError> {
    let offset = rec_and_offset.offset;
    let record = rec_and_offset.record;
    let timestamp = record.timestamp;
    let key = record.key.map(|k|JrpData::from_bytes(k, codecs.key)).transpose()?;
    let value = record.value.map(|v|JrpData::from_bytes(v, codecs.value)).transpose()?;
    Ok(JrpRecFetch::new(offset, timestamp, key, value))
}

#[inline]
fn k2j_rsp(rsp: KfkRsp<JrpCodecs>) -> Result<JrpRspData<'static>, JrpkError> {
    match rsp {
        KfkRsp::Send { offsets } => {
            Ok(JrpRspData::send(offsets))
        }
        KfkRsp::Fetch { recs_and_offsets, high_watermark, codecs } => {
            let res: Result<Vec<JrpRecFetch>, JrpkError> = recs_and_offsets.into_iter()
                .map(|ro| { k2j_rec_fetch(ro, &codecs) }).collect();
            let records = res?;
            Ok(JrpRspData::fetch(records, high_watermark))
        }
        KfkRsp::Offset(offset) => {
            Ok(JrpRspData::Offset(offset))
        }
    }
}

#[inline]
fn j2k_offset(offset: JrpOffset) -> KfkOffset {
    match offset {
        JrpOffset::Earliest => KfkOffset::Implicit(OffsetAt::Earliest),
        JrpOffset::Latest => KfkOffset::Implicit(OffsetAt::Latest),
        JrpOffset::Timestamp(ts) => KfkOffset::Implicit(OffsetAt::Timestamp(ts)),
        JrpOffset::Offset(pos) => KfkOffset::Explicit(pos)
    }
}

#[inline]
fn j2k_req(
    method: JrpMethod,
    offset: Option<JrpOffset>,
    codecs: Option<JrpCodecs>,
    records: Option<Vec<JrpRecSend>>,
    bytes: Option<Range<i32>>,
    max_wait_ms: Option<i32>,
) -> Result<KfkReq<JrpCodecs>, JrpkError> {
    match method {
        JrpMethod::Send => {
            let jrp_records = records
                .ok_or(JrpkError::Syntax("records is missing"))?;
            let records: Result<Vec<Record>, DecodeError> = jrp_records.into_iter()
                .map(|jrs| j2k_rec_send(jrs))
                .collect();
            let kfk_req = KfkReq::send(records?);
            Ok(kfk_req)
        }
        JrpMethod::Fetch => {
            let offset = offset.ok_or(JrpkError::Syntax("offset is missing"))?;
            let bytes = bytes.ok_or(JrpkError::Syntax("bytes is missing"))?;
            let max_wait_ms = max_wait_ms.ok_or(JrpkError::Syntax("max_wait_ms is missing"))?;
            let codecs = codecs.ok_or(JrpkError::Syntax("codecs are missing"))?;
            let kfk_req = KfkReq::fetch(j2k_offset(offset), bytes, max_wait_ms, codecs);
            Ok(kfk_req)
        }
        JrpMethod::Offset => {
            let offset = offset.ok_or(JrpkError::Syntax("offset is missing"))?;
            let kfk_req = KfkReq::offset(j2k_offset(offset));
            Ok(kfk_req)
        }
    }
}

#[instrument(ret, err, skip(tcp_stream, client_cache, kfk_res_ctx_snd, jrp_err_snd, metrics))]
async fn jsonrpc_req_reader(
    mut tcp_stream: SplitStream<Framed<TcpStream, JsonCodec>>,
    client_cache: Arc<KfkClientCache<JrpCodecs, SrvCtx>>,
    kfk_res_ctx_snd: KfkResCtxSnd<JrpCodecs, SrvCtx>,
    jrp_err_snd: JrpErrSnd,
    metrics: JrpkMetrics,
) -> Result<(), JrpkError> {
    while let Some(result) = tcp_stream.next().await {
        // if we cannot even decode frame - we disconnect
        let mut labels = Labels::new(LblTier::Server).traffic(LblTraffic::In).build();
        let bytes = result?;
        let length = bytes.len() as u64;
        trace!("json: {}", from_utf8(bytes.as_ref())?);
        // we are optimistic and expect most requests to be well-formed
        match serde_json::from_slice::<JrpReq>(bytes.as_ref()) {
            // if request is syntactically correct, we proceed
            Ok(jrp_req) => {
                trace!("request: {:?}", jrp_req);
                let params = jrp_req.params;
                let tap = Tap::new(params.topic, params.partition);
                let srv_ctx = SrvCtx::new(jrp_req.id, jrp_req.method, tap.clone());
                metrics.throughputs
                    .get_or_create(labels.method(jrp_req.method).tap(tap))
                    .inc_by(length);
                let kfk_req_res = j2k_req(jrp_req.method, params.offset, params.codecs, params.records, params.bytes, params.max_wait_ms);
                match kfk_req_res {
                    //if request is semantically correct, send it to kafka pipeline
                    Ok(kfk_req) => {
                        let tap = srv_ctx.tap.clone();
                        let kfk_req_ctx = ReqCtx::new(kfk_req, srv_ctx, kfk_res_ctx_snd.clone());
                        let kfk_req_ctx_snd = client_cache.lookup_kafka_sender(tap).await?;
                        kfk_req_ctx_snd.send(kfk_req_ctx).await?;
                    }
                    // if request is NOT semantically correct, send error directly to output pipeline
                    Err(err) => {
                        let kfk_res_ctx = KfkResCtx::err(err, srv_ctx);
                        kfk_res_ctx_snd.send(kfk_res_ctx).await?;
                    }
                }
            }
            // if request is not well-formed we attempt to get at least and id to respond
            // and send decode error directly to the result channel, without round-trip to kafka client
            Err(err) => {
                warn!("jsonrpc decode error: {}", err);
                // if even an id is absent we give up and disconnect
                metrics.throughputs
                    .get_or_create(&labels)
                    .inc_by(length);
                let jrp_id = serde_json::from_slice::<JrpId>(bytes.as_ref())?;
                let id = jrp_id.id;
                let jrp_err = JrpkError::Internal(format!("jsonrpc decode error: {}", err));
                jrp_err_snd.send((id, jrp_err)).await?;
            }
        }
    }
    Ok(())
}

type JrpRspMeteredItem = MeteredItem<JrpRsp<'static>>;

#[instrument(ret, err, skip(tcp_sink, kfk_res_ctx_rcv, jrp_err_rcv, metrics))]
async fn jsonrpc_rsp_writer(
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpRspMeteredItem>,
    mut kfk_res_ctx_rcv: KfkResCtxRcv<JrpCodecs, SrvCtx>,
    mut jrp_err_rcv: JrpErrRcv,
    metrics: JrpkMetrics,
) -> Result<(), JrpkError> {
    loop {
        select! {
            Some(kfk_res_ctx) = kfk_res_ctx_rcv.recv() => {
                trace!("response: {:?}", kfk_res_ctx);
                let KfkResCtx { res: kfk_res_rsp, ctx: srv_ctx } = kfk_res_ctx;
                let ts = srv_ctx.ts;
                let tap = srv_ctx.tap;
                let mut labels = Labels::new(LblTier::Server).method(srv_ctx.method).tap(tap).build();
                metrics.latencies
                    .get_or_create(&labels)
                    .observe(ts.elapsed().as_secs_f64());
                let throughput = metrics.throughputs
                    .get_or_create_owned(labels.traffic(LblTraffic::Out));
                let jrp_res_rsp_data = kfk_res_rsp
                    .and_then(|kfk_rsp| k2j_rsp(kfk_rsp));
                let jrp_rsp = JrpRsp::res(srv_ctx.id, jrp_res_rsp_data);
                let item = MeteredItem::new(jrp_rsp, throughput);
                tcp_sink.send(item).await?;
            }
            Some((id, jrp_err)) = jrp_err_rcv.recv() => {
                let jrp_rsp = JrpRsp::err(id, jrp_err.into());
                let labels = Labels::new(LblTier::Server).traffic(LblTraffic::Out).build();
                let throughput = metrics.throughputs
                    .get_or_create_owned(&labels);
                let item = MeteredItem::new(jrp_rsp, throughput);
                tcp_sink.send(item).await?;
            }
            else => {
                info!("done");
                break;
            }
        }
    }
    tcp_sink.flush().await?;
    Ok(())
}

type JrpErrSnd = Sender<(usize, JrpkError)>;
type JrpErrRcv = Receiver<(usize, JrpkError)>;

#[instrument(ret, err, skip(client_cache, tcp_stream, metrics))]
async fn serve_jsonrpc(
    tcp_stream: TcpStream,
    client_cache: Arc<KfkClientCache<JrpCodecs, SrvCtx>>,
    max_frame_size: usize,
    send_buf_size: usize,
    recv_buf_size: usize,
    queue_size: usize,
    metrics: JrpkMetrics,
) -> Result<(), JrpkError> {
    set_buf_sizes(&tcp_stream, recv_buf_size, send_buf_size)?;
    let codec = JsonCodec::new(max_frame_size);
    let framed = Framed::new(tcp_stream, codec);
    let (tcp_sink, tcp_stream) = framed.split();
    let (kfk_res_snd, kfk_res_rcv) = mpsc::channel::<KfkResCtx<JrpCodecs, SrvCtx>>(queue_size);
    let (jrp_err_snd, jrp_err_rcv) = mpsc::channel::<(usize, JrpkError)>(queue_size);

    let rh = spawn(
        jsonrpc_req_reader(
            tcp_stream,
            client_cache,
            kfk_res_snd,
            jrp_err_snd,
            metrics.clone(),
        )
    );
    let wh = spawn(
        jsonrpc_rsp_writer(
            tcp_sink,
            kfk_res_rcv,
            jrp_err_rcv,
            metrics.clone(),
        )
    );
    let _ = try_join!(rh, wh)?;
    Ok(())
}

#[instrument(ret, err, skip(kafka_clients, prometheus_registry))]
pub async fn listen_jsonrpc(
    bind: SocketAddr,
    max_frame_size: usize,
    send_buf_size: usize,
    recv_buf_size: usize,
    queue_size: usize,
    kafka_clients: Arc<KfkClientCache<JrpCodecs, SrvCtx>>,
    prometheus_registry: Arc<Mutex<Registry>>,
) -> Result<(), JrpkError> {

    let metrics = JrpkMetrics::new(prometheus_registry);

    info!("bind: {:?}", bind);
    let listener = TcpListener::bind(bind).await?;
    loop {
        let (tcp_stream, addr) = listener.accept().await?;
        info!("accepted: {:?}", addr);
        spawn(
            serve_jsonrpc(
                tcp_stream,
                kafka_clients.clone(),
                max_frame_size,
                send_buf_size,
                recv_buf_size,
                queue_size,
                metrics.clone(),
            )
        );
    }
}
