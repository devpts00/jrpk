use crate::codec::{JsonCodec, MeteredItem};
use crate::model::{JrpCodecs, JrpData, JrpId, JrpMethod, JrpOffset, JrpParams, JrpRecFetch, JrpRecSend, JrpReq, JrpRsp, JrpRspData};
use crate::kafka::{KfkClientCache, KfkOffset, KfkOut, KfkReq};
use crate::util::{set_buf_sizes, Id, Request, Tap};
use base64::DecodeError;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rskafka::record::{Record, RecordAndOffset};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::ops::Range;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use chrono::Utc;
use moka::future::Cache;
use prometheus_client::registry::Registry;
use rskafka::client::partition::OffsetAt;
use tokio::net::{TcpListener, TcpStream};
use tokio::{select, spawn, try_join};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::{Framed};
use tracing::{info, instrument, trace, warn};
use crate::error::JrpkError;
use crate::metrics::{JrpkMetrics, Labels, LblMethod, LblTier, LblTraffic};

enum JrpCtx

#[derive(Debug, Clone)]
struct JrpCtx<E> {
    id: usize,
    ts: Instant,
    tap: Tap,
    extra: E
}

impl JrpCtxBasic {
    fn new(tap: Tap) -> Self { Self { ts: Instant::now(), tap } }
}

type JrpCtxSend = JrpCtxBasic;
type JrpCtxOffset = JrpCtxBasic;

#[derive(Debug, Clone)]
struct JrpCtxFetch {
    ts: Instant,
    tap: Tap,
    codecs: JrpCodecs,
}

impl JrpCtxFetch {
    fn new(tap: Tap, codecs: JrpCodecs) -> Self { Self { ts: Instant::now(), tap, codecs } }
}

struct JrpCtxCache {
    send: Cache<usize, JrpCtxSend>,
    offset: Cache<usize, JrpCtxOffset>,
    fetch: Cache<usize, JrpCtxFetch>
}

impl JrpCtxCache {
    fn new(queue_size: usize) -> Self {
        Self {
            send: Cache::new(2 * queue_size as u64),
            offset: Cache::new(2 * queue_size as u64),
            fetch: Cache::new(2 * queue_size as u64)
        }
    }
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
fn k2j_rsp(rsp: KfkOut, codecs: Option<JrpCodecs>) -> Result<JrpRspData<'static>, JrpkError> {
    match rsp {
        KfkOut::Send(offsets) => {
            Ok(JrpRspData::send(offsets))
        }
        KfkOut::Fetch(recs_and_offsets, high_watermark) => {
            // TODO: take care of codecs
            let codecs = codecs.unwrap_or_default();
            let res: Result<Vec<JrpRecFetch>, JrpkError> = recs_and_offsets.into_iter()
                .map(|ro| { k2j_rec_fetch(ro, &codecs) }).collect();
            let records = res?;
            Ok(JrpRspData::fetch(records, high_watermark))
        }
        KfkOut::Offset(offset) => {
            Ok(JrpRspData::Offset(offset))
        }
    }
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
    ctx_cache: Arc<JrpCtxCache>,
    kfk_rsp_snd: Sender<Id<Result<KfkOut, JrpkError>>>,
    metrics: &JrpkMetrics,
    labels: &mut Labels,
    length: u64,
) -> Result<(Tap, KfkReq), JrpkError> {
    let id = jrp_req.id;
    let params = jrp_req.params;
    let tap = Tap::new(params.topic, params.partition);
    let labels = labels.tap(tap.clone());
    let kfk_req = match jrp_req.method {
        JrpMethod::Send => {
            let labels = labels.method(LblMethod::Send);
            metrics.throughputs.get_or_create(&labels).inc_by(length);
            let records = params.records
                .ok_or(JrpkError::Syntax("records is missing"))?
                .into_iter()
                .map(|jrs| Record::try_from(jrs))
                .collect::<Result<Vec<Record>, JrpkError>>()?;
            let ctx = JrpCtxSend::new(tap.clone());
            ctx_cache.send.insert(id, ctx).await;
            KfkReq::Send(Request::new(id, records, kfk_rsp_snd))
        }
        JrpMethod::Fetch => {
            let labels = labels.method(LblMethod::Fetch);
            metrics.throughputs.get_or_create(&labels).inc_by(length);
            let offset: KfkOffset = params.offset.ok_or(JrpkError::Syntax("offset is missing")).map(|o| o.into())?;
            let bytes = params.bytes.ok_or(JrpkError::Syntax("bytes is missing"))?;
            let max_wait_ms = params.max_wait_ms.ok_or(JrpkError::Syntax("max_wait_ms is missing"))?;
            let codecs = params.codecs.ok_or(JrpkError::Syntax("codecs are missing"))?;
            let ctx = JrpCtxFetch::new(tap.clone(), codecs);
            ctx_cache.fetch.insert(id, ctx).await;
            KfkReq::Fetch(Request::new(id, (offset, bytes, max_wait_ms), kfk_rsp_snd))
        }
        JrpMethod::Offset => {
            let labels = labels.method(LblMethod::Offset);
            metrics.throughputs.get_or_create(&labels).inc_by(length);
            let offset = params.offset.ok_or(JrpkError::Syntax("offset is missing"))
                .map(|o| o.into())?;
            let ctx = JrpCtxOffset::new(tap.clone());
            ctx_cache.offset.insert(id, ctx).await;
            KfkReq::Offset(Request::new(id, offset, kfk_rsp_snd))
        }
    };
    Ok((tap, kfk_req))
}

#[instrument(ret, err, skip(tcp_stream, cli_cache, ctx_cache, kfk_rsp_snd, jrp_err_snd, metrics))]
async fn jsonrpc_req_reader(
    mut tcp_stream: SplitStream<Framed<TcpStream, JsonCodec>>,
    cli_cache: Arc<KfkClientCache<KfkReq>>,
    ctx_cache: Arc<JrpCtxCache>,
    kfk_rsp_snd: Sender<Id<Result<KfkOut, JrpkError>>>,
    jrp_err_snd: JrpErrSnd,
    metrics: JrpkMetrics,
) -> Result<(), JrpkError> {
    while let Some(result) = tcp_stream.next().await {
        // if we cannot even decode frame - we disconnect
        let bytes = result?;
        let length = bytes.len() as u64;
        trace!("json: {}", from_utf8(bytes.as_ref())?);
        let mut labels = Labels::new(LblTier::Server).traffic(LblTraffic::In).build();
        // we are optimistic and expect most requests to be well-formed
        match serde_json::from_slice::<JrpReq>(bytes.as_ref()) {
            // if request is syntactically correct, we proceed
            Ok(jrp_req) => {
                trace!("request: {:?}", jrp_req);
                let id = jrp_req.id;
                match j2k_req(jrp_req, ctx_cache.clone(), kfk_rsp_snd.clone(), &metrics, &mut labels, length).await {
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

#[instrument(ret, err, skip(tcp_sink, kfk_rsp_rcv, jrp_err_rcv, ctx_cache, metrics))]
async fn jsonrpc_rsp_writer(
    mut tcp_sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpRspMeteredItem>,
    mut kfk_rsp_rcv: Receiver<Id<Result<KfkOut, JrpkError>>>,
    mut jrp_err_rcv: JrpErrRcv,
    ctx_cache: Arc<JrpCtxCache>,
    metrics: JrpkMetrics,
) -> Result<(), JrpkError> {
    loop {
        select! {
            biased;
            Some(Id(id, res)) = kfk_rsp_rcv.recv() => {
                trace!("response, id: {}, res: {:?}", id, res);
                // panic if lookup fails - means something is logically broken

                let x = res;


                let JrpCtx { ts, codecs } = ctx_cache.remove(&id).await.unwrap();
                let codecs = codecs.unwrap_or_default();
                let mut labels = Labels::new(LblTier::Server).method(method).tap(tap).build();
                metrics.latencies.get_or_create(&labels)
                    .observe(ts.elapsed().as_secs_f64());
                let throughput = metrics.throughputs
                    .get_or_create_owned(labels.traffic(LblTraffic::Out));
                let jrp_res_rsp_data = res.and_then(|kfk_rsp| k2j_rsp(kfk_rsp, codecs));
                let jrp_rsp = JrpRsp::res(id, jrp_res_rsp_data);
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
    cli_cache: Arc<KfkClientCache>,
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
    let (kfk_res_id_snd, kfk_res_id_rcv) = mpsc::channel::<ResId<KfkOut, JrpkError>>(queue_size);
    let (jrp_err_snd, jrp_err_rcv) = mpsc::channel::<(usize, JrpkError)>(queue_size);
    let ctx_cache = Arc::new(Cache::new(queue_size as u64));

    let rh = spawn(
        jsonrpc_req_reader(
            tcp_stream,
            cli_cache,
            ctx_cache.clone(),
            kfk_res_id_snd,
            jrp_err_snd,
            metrics.clone(),
        )
    );
    let wh = spawn(
        jsonrpc_rsp_writer(
            tcp_sink,
            kfk_res_id_rcv,
            jrp_err_rcv,
            ctx_cache,
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
    cli_cache: Arc<KfkClientCache>,
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
