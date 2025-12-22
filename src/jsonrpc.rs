use crate::codec::{JsonCodec, MeteredItem};
use crate::error::JrpkError;
use crate::kafka::{KfkClientCache, KfkError, KfkOffset, KfkReq, KfkRsp, KfkTypes};
use crate::metrics::{JrpkMetrics, JrpkLabels, LblMethod, LblTier, LblTraffic};
use crate::model::{JrpCodecs, JrpData, JrpId, JrpMethod, JrpOffset, JrpRecFetch, JrpRecSend, JrpReq, JrpRsp, JrpRspData};
use crate::util::{set_buf_sizes, Ctx, Request, Tap};
use chrono::Utc;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use prometheus_client::registry::Registry;
use rskafka::client::partition::OffsetAt;
use rskafka::record::{Record, RecordAndOffset};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::{select, spawn, try_join};
use tokio_util::codec::Framed;
use tracing::{info, instrument, trace, warn};

#[derive(Debug, Clone)]
pub struct JrpCtx<E: Clone> {
    id: usize,
    ts: Instant,
    tap: Tap,
    extra: E
}

impl <E: Clone> JrpCtx<E> {
    fn new(id: usize, tap: Tap, extra: E) -> Self {
        JrpCtx { id, ts: Instant::now(), tap, extra }
    }
}

pub struct JrpCtxTypes;

impl JrpCtxTypes {
    pub fn send(id: usize, tap: Tap) -> JrpCtx<()> {
        JrpCtx::new(id, tap, ())
    }
    pub fn fetch(id: usize, tap: Tap, codecs: JrpCodecs) -> JrpCtx<JrpCodecs> {
        JrpCtx::new(id, tap, codecs)
    }
    pub fn offset(id: usize, tap: Tap) -> JrpCtx<()> {
        JrpCtx::new(id, tap, ())
    }
}

impl KfkTypes for JrpCtxTypes {
    type S = JrpCtx<()>;
    type F = JrpCtx<JrpCodecs>;
    type O = JrpCtx<()>;
}

impl <'a> TryFrom<JrpRecSend<'a>> for Record {
    type Error = JrpkError;
    fn try_from(value: JrpRecSend) -> Result<Self, Self::Error> {
        let key: Option<Vec<u8>> = value.key.map(|k| k.into_bytes()).transpose()?;
        let value: Option<Vec<u8>> = value.value.map(|v| v.into_bytes()).transpose()?;
        Ok(Record { key, value, headers: BTreeMap::new(), timestamp: Utc::now() })
    }
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
fn k2j_rsp_send(send: Result<Vec<i64>, KfkError>) -> Result<JrpRspData<'static>, JrpkError> {
    let offsets = send?;
    Ok(JrpRspData::send(offsets))
}

#[inline]
fn k2j_rsp_fetch(fetch: Result<(Vec<RecordAndOffset>, i64), KfkError>, codecs: &JrpCodecs) -> Result<JrpRspData<'static>, JrpkError> {
    let (records_and_offsets, high_watermark) = fetch?;
    let records = records_and_offsets.into_iter()
        .map(|ro| k2j_rec_fetch(ro, &codecs))
        .collect::<Result<Vec<JrpRecFetch<'static>>, JrpkError>>()?;
    Ok(JrpRspData::fetch(records, high_watermark))
}

#[inline]
fn k2j_rsp_offset(offsets: Result<i64, KfkError>) -> Result<JrpRspData<'static>, JrpkError> {
    let offsets = offsets?;
    Ok(JrpRspData::Offset(offsets))
}

impl Into<KfkOffset> for JrpOffset {
    fn into(self) -> KfkOffset {
        match self {
            JrpOffset::Earliest => KfkOffset::At(OffsetAt::Earliest),
            JrpOffset::Latest => KfkOffset::At(OffsetAt::Latest),
            JrpOffset::Timestamp(ts) => KfkOffset::At(OffsetAt::Timestamp(ts)),
            JrpOffset::Offset(pos) => KfkOffset::Pos(pos)
        }
    }
}

async fn j2k_req<'a>(
    jrp_req: JrpReq<'a>,
    kfk_rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
    metrics: &JrpkMetrics,
    labels: &mut JrpkLabels,
    length: u64,
) -> Result<(Tap, KfkReq<JrpCtxTypes>), JrpkError> {
    let id = jrp_req.id;
    let params = jrp_req.params;
    let tap = Tap::new(params.topic, params.partition);
    let labels = labels.tap(tap.clone());
    match jrp_req.method {
        JrpMethod::Send => {
            let labels = labels.method(LblMethod::Send);
            metrics.throughputs.get_or_create(&labels).inc_by(length);
            let records = params.records
                .ok_or(JrpkError::Syntax("records is missing"))?
                .into_iter()
                .map(|jrs| Record::try_from(jrs))
                .collect::<Result<Vec<Record>, JrpkError>>()?;
            let ctx = JrpCtxTypes::send(id, tap.clone());
            Ok((tap, KfkReq::Send(Request(Ctx(ctx, records), kfk_rsp_snd))))
        }
        JrpMethod::Fetch => {
            let labels = labels.method(LblMethod::Fetch);
            metrics.throughputs.get_or_create(&labels).inc_by(length);
            let offset: KfkOffset = params.offset.ok_or(JrpkError::Syntax("offset is missing")).map(|o| o.into())?;
            let bytes = params.bytes.ok_or(JrpkError::Syntax("bytes is missing"))?;
            let max_wait_ms = params.max_wait_ms.ok_or(JrpkError::Syntax("max_wait_ms is missing"))?;
            let codecs = params.codecs.ok_or(JrpkError::Syntax("codecs are missing"))?;
            let ctx = JrpCtxTypes::fetch(id, tap.clone(), codecs);
            Ok((tap, KfkReq::Fetch(Request(Ctx(ctx, (offset, bytes, max_wait_ms)), kfk_rsp_snd))))
        }
        JrpMethod::Offset => {
            let labels = labels.method(LblMethod::Offset);
            metrics.throughputs.get_or_create(&labels).inc_by(length);
            let offset = params.offset.ok_or(JrpkError::Syntax("offset is missing"))
                .map(|o| o.into())?;
            let ctx = JrpCtxTypes::offset(id, tap.clone());
            Ok((tap, KfkReq::Offset(Request(Ctx(ctx, offset), kfk_rsp_snd))))
        }
    }
}

#[instrument(ret, err, skip(tcp_stream, cli_cache, kfk_rsp_snd, jrp_err_snd, metrics))]
async fn jsonrpc_req_reader(
    mut tcp_stream: SplitStream<Framed<TcpStream, JsonCodec>>,
    cli_cache: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
    kfk_rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
    jrp_err_snd: JrpErrSnd,
    metrics: JrpkMetrics,
) -> Result<(), JrpkError> {
    while let Some(result) = tcp_stream.next().await {
        // if we cannot even decode frame - we disconnect
        let bytes = result?;
        let length = bytes.len() as u64;
        trace!("json: {}", from_utf8(bytes.as_ref())?);
        let mut labels = JrpkLabels::new(LblTier::Jsonrpc).traffic(LblTraffic::In).build();
        // we are optimistic and expect most requests to be well-formed
        match serde_json::from_slice::<JrpReq>(bytes.as_ref()) {
            // if request is syntactically correct, we proceed
            Ok(jrp_req) => {
                trace!("request: {:?}", jrp_req);
                let id = jrp_req.id;
                match j2k_req(jrp_req, kfk_rsp_snd.clone(), &metrics, &mut labels, length).await {
                    Ok((tap, kfk_req)) => {
                        let kfk_req_snd = cli_cache.lookup_sender(tap).await?;
                        kfk_req_snd.send(kfk_req).await?;
                    }
                    Err(error) => {
                        jrp_err_snd.send((id, error)).await?;
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

#[instrument(ret, err, skip(tcp_sink, kfk_rsp_rcv, jrp_err_rcv, metrics))]
async fn jsonrpc_rsp_writer(
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpRspMeteredItem>,
    mut kfk_rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
    mut jrp_err_rcv: JrpErrRcv,
    metrics: JrpkMetrics,
) -> Result<(), JrpkError> {
    loop {
        let mut labels = JrpkLabels::new(LblTier::Jsonrpc).traffic(LblTraffic::Out).build();
        select! {
            biased;
            Some(kfk_rsp) = kfk_rsp_rcv.recv() => {
                let jrp_rsp = match kfk_rsp {
                    KfkRsp::Send(Ctx(ctx, offsets)) => {
                        metrics.latency(&labels.method(LblMethod::Send).tap(ctx.tap), ctx.ts);
                         JrpRsp::res(ctx.id, k2j_rsp_send(offsets))
                    }
                    KfkRsp::Fetch(Ctx(ctx, records_and_offsets)) => {
                        metrics.latency(&labels.method(LblMethod::Fetch).tap(ctx.tap), ctx.ts);
                        JrpRsp::res(ctx.id, k2j_rsp_fetch(records_and_offsets, &ctx.extra))
                    }
                    KfkRsp::Offset(Ctx(ctx, offset)) => {
                        metrics.latency(&labels.method(LblMethod::Offset).tap(ctx.tap), ctx.ts);
                        JrpRsp::res(ctx.id, k2j_rsp_offset(offset))
                    }
                };
                let throughput = metrics.throughputs.get_or_create_owned(&labels);
                let item = MeteredItem::new(jrp_rsp, throughput);
                tcp_sink.send(item).await?;
            }
            Some((id, jrp_err)) = jrp_err_rcv.recv() => {
                let jrp_rsp = JrpRsp::err(id, jrp_err.into());
                let labels = JrpkLabels::new(LblTier::Jsonrpc).traffic(LblTraffic::Out).build();
                let throughput = metrics.throughputs.get_or_create_owned(&labels);
                let item = MeteredItem::new(jrp_rsp, throughput);
                tcp_sink.send(item).await?;
            }
            else => {
                break;
            }
        }
    }
    tcp_sink.flush().await?;
    Ok(())
}

type JrpErrSnd = Sender<(usize, JrpkError)>;
type JrpErrRcv = Receiver<(usize, JrpkError)>;

#[instrument(ret, err, skip(cli_cache, tcp_stream, metrics))]
async fn serve_jsonrpc(
    tcp_stream: TcpStream,
    cli_cache: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
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
    let (kfk_rsp_snd, kfk_rsp_rcv) = mpsc::channel::<KfkRsp<JrpCtxTypes>>(queue_size);
    let (jrp_err_snd, jrp_err_rcv) = mpsc::channel::<(usize, JrpkError)>(queue_size);

    let rh = spawn(
        jsonrpc_req_reader(
            tcp_stream,
            cli_cache,
            kfk_rsp_snd,
            jrp_err_snd,
            metrics.clone(),
        )
    );
    let wh = spawn(
        jsonrpc_rsp_writer(
            tcp_sink,
            kfk_rsp_rcv,
            jrp_err_rcv,
            metrics.clone(),
        )
    );
    let _ = try_join!(rh, wh)?;
    Ok(())
}

#[instrument(ret, err, skip(cli_cache, prometheus_registry))]
pub async fn listen_jsonrpc(
    bind: SocketAddr,
    max_frame_size: usize,
    send_buf_size: usize,
    recv_buf_size: usize,
    queue_size: usize,
    cli_cache: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
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
                cli_cache.clone(),
                max_frame_size,
                send_buf_size,
                recv_buf_size,
                queue_size,
                metrics.clone(),
            )
        );
    }
}
