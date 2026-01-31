use crate::codec::LinesCodec;
use crate::error::JrpkError;
use crate::kafka::{KfkClientCache, KfkError, KfkOffset, KfkReq, KfkRsp, KfkTypes, KfkTap};
use crate::metrics::{JrpkMetrics, JrpkLabels, LblMethod, LblTier, LblTraffic, MeteredItem};
use crate::model::{JrpCodec, JrpData, JrpId, JrpMethod, JrpOffset, JrpRecFetch, JrpRecSend, JrpReq, JrpRsp, JrpRspData, JrpSelector};
use crate::util::{set_buf_sizes, Ctx, Req};
use chrono::Utc;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rskafka::client::partition::OffsetAt;
use rskafka::record::{Record, RecordAndOffset};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Instant;
use base64::DecodeError;
use faststr::FastStr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::{select, spawn, try_join};
use tokio_util::codec::Framed;
use tracing::{info, instrument, trace, warn};

#[derive(Debug, Clone)]
pub struct JrpCtx<E: Clone> {
    pub id: usize,
    pub ts: Instant,
    pub tap: KfkTap,
    pub extra: E
}

impl <E: Clone> JrpCtx<E> {
    pub fn new(id: usize, ts: Instant, tap: KfkTap, extra: E) -> Self {
        JrpCtx { id, ts, tap, extra }
    }
}

#[derive(Debug)]
pub struct JrpCtxTypes;

impl JrpCtxTypes {
    pub fn send(id: usize, ts: Instant, tap: KfkTap) -> JrpCtx<()> {
        JrpCtx::new(id, ts, tap, ())
    }
    pub fn fetch(id: usize, ts: Instant, tap: KfkTap, selector: JrpSelector) -> JrpCtx<JrpSelector> {
        JrpCtx::new(id, ts, tap, selector)
    }
    pub fn offset(id: usize, ts: Instant, tap: KfkTap) -> JrpCtx<()> {
        JrpCtx::new(id, ts, tap, ())
    }
}

impl KfkTypes for JrpCtxTypes {
    type S = JrpCtx<()>;
    type F = JrpCtx<JrpSelector>;
    type O = JrpCtx<()>;
}

impl <'a> TryFrom<JrpRecSend<'a>> for Record {
    type Error = JrpkError;
    fn try_from(jrp_rec_snd: JrpRecSend) -> Result<Self, Self::Error> {
        let headers = jrp_rec_snd.headers.into_iter()
            .map(|(k, v)|
                v.into_bytes().map(|b| (k, b))
            )
            .collect::<Result<BTreeMap<String, Vec<u8>>, DecodeError>>();
        let headers = headers?;
        let key: Option<Vec<u8>> = jrp_rec_snd.key
            .map(|k| k.into_bytes())
            .transpose()?;
        let value: Option<Vec<u8>> = jrp_rec_snd.value
            .map(|v| v.into_bytes())
            .transpose()?;
        Ok(Record { key, value, headers, timestamp: Utc::now() })
    }
}

#[inline]
fn k2j_data(bytes: Option<Vec<u8>>, select: Option<JrpCodec>) -> Result<Option<JrpData<'static>>, JrpkError> {
    match (bytes, select) {
        (Some(bytes), Some(codec)) => Ok(Some(JrpData::from_bytes(bytes, &codec)?)),
        _ => Ok(None)
    }
}

#[inline]
fn k2j_rec_headers(headers: BTreeMap<String, Vec<u8>>, codecs: &Vec<(FastStr, JrpCodec)>, default: Option<JrpCodec>) -> Result<Vec<(FastStr, JrpData<'static>)>, JrpkError> {
    headers.into_iter()
        .filter_map(|(header, bytes)| {
            let header = FastStr::new(header);
            codecs.binary_search_by(|(name, _)| name.as_str().cmp(header.as_str()))
                .ok()
                .and_then(|idx| codecs.get(idx))
                .map(|(_, codec)| codec)
                .or(default.as_ref())
                .map(|codec| JrpData::from_bytes(bytes, codec).map(|data| (header, data)))
    }).collect()
}

#[inline]
pub fn k2j_rec_fetch(rec_and_offset: RecordAndOffset, selector: &JrpSelector) -> Result<JrpRecFetch<'static>, JrpkError> {
    let offset = rec_and_offset.offset;
    let record = rec_and_offset.record;
    let timestamp = record.timestamp;
    let key = k2j_data(record.key, selector.key)?;
    let value =   k2j_data(record.value, Some(selector.value))?;
    let headers = k2j_rec_headers(record.headers, &selector.headers, selector.header_default)?;
    Ok(JrpRecFetch::new(offset, timestamp, key, value, headers))
}

#[inline]
fn k2j_rsp_send(send: Result<Vec<i64>, KfkError>) -> Result<JrpRspData<'static>, JrpkError> {
    let offsets = send?;
    Ok(JrpRspData::send(offsets))
}

#[inline]
fn k2j_rsp_fetch(fetch: Result<(Vec<RecordAndOffset>, i64), KfkError>, selector: &JrpSelector) -> Result<JrpRspData<'static>, JrpkError> {
    let (records_and_offsets, high_watermark) = fetch?;
    let records = records_and_offsets.into_iter()
        .map(|ro| k2j_rec_fetch(ro, selector))
        .collect::<Result<Vec<JrpRecFetch<'static>>, JrpkError>>()?;
    Ok(JrpRspData::fetch(records, high_watermark))
}

#[inline]
fn k2j_rsp_offset(offsets: Result<i64, KfkError>) -> Result<JrpRspData<'static>, JrpkError> {
    let offsets = offsets?;
    Ok(JrpRspData::Offset(offsets))
}

impl From<KfkOffset> for JrpOffset {
    fn from(value: KfkOffset) -> Self {
        match value {
            KfkOffset::At(OffsetAt::Earliest) => JrpOffset::Earliest,
            KfkOffset::At(OffsetAt::Latest) => JrpOffset::Latest,
            KfkOffset::At(OffsetAt::Timestamp(timestamp)) => JrpOffset::Timestamp(timestamp),
            KfkOffset::Pos(offset) => JrpOffset::Offset(offset),
        }
    }
}

// TODO: split into three separate functions for focused re-use
pub fn j2k_req<'a>(
    jrp_req: JrpReq<'a>,
    kfk_rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
    metrics: &JrpkMetrics,
    labels: &mut JrpkLabels,
    length: usize,
) -> Result<(KfkTap, KfkReq<JrpCtxTypes>), JrpkError> {
    let id = jrp_req.id;
    let params = jrp_req.params;
    let tap = KfkTap::new(params.topic, params.partition);
    let labels = labels.tap(tap.clone());
    match jrp_req.method {
        JrpMethod::Send => {
            let labels = labels.method(LblMethod::Send);
            metrics.size_by_value(&labels, length);
            let records = params.records
                .ok_or(JrpkError::Syntax("records is missing"))?
                .into_iter()
                .map(|jrs| Record::try_from(jrs))
                .collect::<Result<Vec<Record>, JrpkError>>()?;
            let ctx = JrpCtxTypes::send(id, Instant::now(), tap.clone());
            Ok((tap, KfkReq::Send(Req(Ctx(ctx, records), kfk_rsp_snd))))
        }
        JrpMethod::Fetch => {
            let labels = labels.method(LblMethod::Fetch);
            metrics.size_by_value(&labels, length);
            let offset: KfkOffset = params.offset.ok_or(JrpkError::Syntax("offset is missing")).map(|o| o.into())?;
            let bytes = params.bytes.ok_or(JrpkError::Syntax("bytes is missing"))?;
            let max_wait_ms = params.max_wait_ms.ok_or(JrpkError::Syntax("max_wait_ms is missing"))?;
            let selector = params.selector.ok_or(JrpkError::Syntax("selector is missing"))?;
            let ctx = JrpCtxTypes::fetch(id, Instant::now(), tap.clone(), selector);
            Ok((tap, KfkReq::Fetch(Req(Ctx(ctx, (offset, bytes, max_wait_ms)), kfk_rsp_snd))))
        }
        JrpMethod::Offset => {
            let labels = labels.method(LblMethod::Offset);
            metrics.size_by_value(&labels, length);
            let offset = params.offset.ok_or(JrpkError::Syntax("offset is missing"))
                .map(|o| o.into())?;
            let ctx = JrpCtxTypes::offset(id, Instant::now(), tap.clone());
            Ok((tap, KfkReq::Offset(Req(Ctx(ctx, offset), kfk_rsp_snd))))
        }
    }
}

#[instrument(level="info", ret, err, skip(tcp_stream, kfk_clients, kfk_rsp_snd, jrp_err_snd, metrics))]
async fn jsonrpc_req_reader(
    mut tcp_stream: SplitStream<Framed<TcpStream, LinesCodec>>,
    kfk_clients: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
    kfk_rsp_snd: Sender<KfkRsp<JrpCtxTypes>>,
    jrp_err_snd: JrpErrSnd,
    metrics: Arc<JrpkMetrics>,
) -> Result<(), JrpkError> {
    while let Some(result) = tcp_stream.next().await {
        // if we cannot even decode frame - we disconnect
        let line = result?;
        let length = line.len();
        trace!("json: {}", from_utf8(line.as_ref())?);
        let mut labels = JrpkLabels::new(LblTier::Jsonrpc).traffic(LblTraffic::In).build();
        // we are optimistic and expect most requests to be well-formed
        match serde_json::from_slice::<JrpReq>(line.as_ref()) {
            // if request is syntactically correct, we proceed
            Ok(jrp_req) => {
                trace!("request: {:?}", jrp_req);
                let id = jrp_req.id;
                match j2k_req(jrp_req, kfk_rsp_snd.clone(), &metrics, &mut labels, length) {
                    Ok((tap, kfk_req)) => {
                        let kfk_req_snd = kfk_clients.lookup_sender(tap).await?;
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
                metrics.size_by_value(&labels, length);
                let jrp_id = serde_json::from_slice::<JrpId>(line.as_ref())?;
                let id = jrp_id.id;
                let jrp_err = JrpkError::Internal(format!("jsonrpc decode error: {}", err));
                jrp_err_snd.send((id, jrp_err)).await?;
            }
        }
    }
    Ok(())
}

type JrpRspMeteredItem = MeteredItem<JrpRsp<'static>>;

#[instrument(level="info", ret, err, skip(tcp_sink, kfk_rsp_rcv, jrp_err_rcv, metrics))]
async fn jsonrpc_rsp_writer(
    mut tcp_sink: SplitSink<Framed<TcpStream, LinesCodec>, JrpRspMeteredItem>,
    mut kfk_rsp_rcv: Receiver<KfkRsp<JrpCtxTypes>>,
    mut jrp_err_rcv: JrpErrRcv,
    metrics: Arc<JrpkMetrics>,
) -> Result<(), JrpkError> {
    loop {
        let mut labels = JrpkLabels::new(LblTier::Jsonrpc).traffic(LblTraffic::Out).build();
        select! {
            biased;
            Some(kfk_rsp) = kfk_rsp_rcv.recv() => {
                let jrp_rsp = match kfk_rsp {
                    KfkRsp::Send(Ctx(ctx, offsets)) => {
                        metrics.time(&labels.method(LblMethod::Send).tap(ctx.tap), ctx.ts);
                         JrpRsp::res(ctx.id, k2j_rsp_send(offsets))
                    }
                    KfkRsp::Fetch(Ctx(ctx, records_and_offsets)) => {
                        metrics.time(&labels.method(LblMethod::Fetch).tap(ctx.tap), ctx.ts);
                        JrpRsp::res(ctx.id, k2j_rsp_fetch(records_and_offsets, &ctx.extra))
                    }
                    KfkRsp::Offset(Ctx(ctx, offset)) => {
                        metrics.time(&labels.method(LblMethod::Offset).tap(ctx.tap), ctx.ts);
                        JrpRsp::res(ctx.id, k2j_rsp_offset(offset))
                    }
                };
                let item = MeteredItem::new(jrp_rsp, metrics.clone(), labels.clone());
                tcp_sink.send(item).await?;
            }
            Some((id, jrp_err)) = jrp_err_rcv.recv() => {
                let jrp_rsp = JrpRsp::err(id, jrp_err.into());
                let labels = JrpkLabels::new(LblTier::Jsonrpc).traffic(LblTraffic::Out).build();
                let item = MeteredItem::new(jrp_rsp, metrics.clone(), labels);
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

#[instrument(level="info", ret, err, skip(kfk_cache, tcp_stream, metrics))]
async fn serve_jsonrpc(
    jrp_max_frame_size: usize,
    jrp_queue_size: usize,
    kfk_cache: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
    tcp_stream: TcpStream,
    tcp_send_buf_size: usize,
    tcp_recv_buf_size: usize,
    metrics: Arc<JrpkMetrics>,
) -> Result<(), JrpkError> {

    set_buf_sizes(&tcp_stream, tcp_recv_buf_size, tcp_send_buf_size)?;

    let codec = LinesCodec::new_with_max_length(jrp_max_frame_size);
    let framed = Framed::new(tcp_stream, codec);
    let (tcp_sink, tcp_stream) = framed.split();
    let (kfk_rsp_snd, kfk_rsp_rcv) = mpsc::channel::<KfkRsp<JrpCtxTypes>>(jrp_queue_size);
    let (jrp_err_snd, jrp_err_rcv) = mpsc::channel::<(usize, JrpkError)>(jrp_queue_size);

    let rh = spawn(
        jsonrpc_req_reader(
            tcp_stream,
            kfk_cache,
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

#[instrument(level="info", ret, err, skip(kfk_cache, metrics))]
pub async fn listen_jsonrpc(
    jrp_bind: SocketAddr,
    jrp_max_frame_size: usize,
    jrp_queue_size: usize,
    kfk_cache: Arc<KfkClientCache<KfkReq<JrpCtxTypes>>>,
    tcp_send_buf_size: usize,
    tcp_recv_buf_size: usize,
    metrics: Arc<JrpkMetrics>,
) -> Result<(), JrpkError> {

    info!("bind: {:?}", jrp_bind);
    let listener = TcpListener::bind(jrp_bind).await?;
    loop {
        let (tcp_stream, addr) = listener.accept().await?;
        info!("accepted: {:?}", addr);
        spawn(
            serve_jsonrpc(
                jrp_max_frame_size,
                jrp_queue_size,
                kfk_cache.clone(),
                tcp_stream,
                tcp_send_buf_size,
                tcp_recv_buf_size,
                metrics.clone(),
            )
        );
    }
}
