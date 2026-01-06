use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use faststr::FastStr;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use log::warn;
use moka::future::Cache;
use reqwest::Url;
use tokio::net::TcpStream;
use tokio::{spawn, try_join};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::block_in_place;
use tokio_util::codec::Framed;
use tracing::{error, info, instrument, trace};
use crate::args::Offset;
use crate::codec::LinesCodec;
use crate::error::JrpkError;
use crate::model::{JrpCodec, JrpCodecs, JrpOffset, JrpRecFetch, JrpReq, JrpRsp, JrpRspData};
use crate::metrics::{spawn_push_prometheus, JrpkMetrics, JrpkLabels, LblMethod, LblTier, LblTraffic, MeteredItem};
use crate::util::{url_append_tap, Tap};

type JrpkMeteredConsReq<'a> = MeteredItem<JrpReq<'a>>;

fn a2j_offset(ao: Offset) -> JrpOffset {
    match ao {
        Offset::Earliest => JrpOffset::Earliest,
        Offset::Latest => JrpOffset::Latest,
        Offset::Timestamp(ts) => JrpOffset::Timestamp(ts),
        Offset::Offset(pos) => JrpOffset::Offset(pos),
    }
}

#[inline]
fn less_record_offset(record: &JrpRecFetch, offset: Offset) -> bool {
    match offset {
        Offset::Earliest => false,
        Offset::Latest => true,
        Offset::Timestamp(timestamp) => record.timestamp < timestamp,
        Offset::Offset(pos) => record.offset < pos,
    }
}

#[allow(clippy::needless_lifetimes)]
#[instrument(ret, err, level="trace", skip(writer, records))]
async fn write_records<'a>(
    id: usize,
    records: Vec<JrpRecFetch<'a>>,
    until: Offset,
    writer: &mut BufWriter<File>,
) -> Result<(), JrpkError> {
    block_in_place(|| {
        for record in records {
            if less_record_offset(&record, until) {
                match record.value {
                    Some(data) => {
                        trace!("record, id: {}, timestamp: {}, offset: {}", id, 0, record.offset);
                        // TODO: differentiate between binary and text data
                        let buf = data.as_text()?;
                        writer.write_all(buf.as_bytes())?;
                        writer.write_all(b"\n")?;
                    }
                    None => {
                        warn!("record, id: {}, offset: {} - EMPTY", id, record.offset);
                    }
                }
            }
        }
        Ok(())
    })
}

#[instrument(ret, err, skip(metrics, times, offset_rcv, tcp_sink))]
async fn consumer_req_writer<'a>(
    tap: Tap,
    max_batch_size: i32,
    max_wait_ms: i32,
    metrics: Arc<JrpkMetrics>,
    times: Arc<Cache<usize, Instant>>,
    mut offset_rcv: Receiver<Offset>,
    mut tcp_sink: SplitSink<Framed<TcpStream, LinesCodec>, JrpkMeteredConsReq<'a>>,
) -> Result<(), JrpkError> {
    let labels = JrpkLabels::new(LblTier::Client)
        .method(LblMethod::Fetch)
        .traffic(LblTraffic::Out)
        .tap(tap.clone())
        .build();
    let mut id = 0;
    while let Some(offset) = offset_rcv.recv().await {
        let tap = tap.clone();
        // TODO: support all codecs
        let codecs = JrpCodecs::default();
        let codecs = JrpCodecs::new(JrpCodec::Base64, JrpCodec::Base64, Vec::new());
        let bytes = 1..max_batch_size;
        let jrp_req_fetch = JrpReq::fetch(id, tap.topic, tap.partition, a2j_offset(offset), bytes, max_wait_ms, codecs);
        let metered_item = JrpkMeteredConsReq::new(jrp_req_fetch, metrics.clone(), labels.clone());
        times.insert(id, Instant::now()).await;
        tcp_sink.send(metered_item).await?;
        id = id + 1;
    }
    tcp_sink.flush().await?;
    Ok(())
}


#[instrument(ret, err, skip(metrics, times, offset_snd, tcp_stream))]
async fn consumer_rsp_reader(
    tap: Tap,
    path: FastStr,
    from: Offset,
    until: Offset,
    metrics: Arc<JrpkMetrics>,
    times: Arc<Cache<usize, Instant>>,
    offset_snd: Sender<Offset>,
    mut tcp_stream: SplitStream<Framed<TcpStream, LinesCodec>>,
) -> Result<(), JrpkError> {
    let labels = JrpkLabels::new(LblTier::Client)
        .method(LblMethod::Fetch)
        .traffic(LblTraffic::In)
        .tap(tap)
        .build();
    let file = File::create(path.as_str())?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    offset_snd.send(from).await?;
    while let Some(result) = tcp_stream.next().await {
        let frame = result?;
        let jrp_rsp = serde_json::from_slice::<JrpRsp>(frame.as_ref())?;
        let id = jrp_rsp.id;
        match jrp_rsp.take_result() {
            Ok(jrp_rsp_data) => {
                match jrp_rsp_data {
                    JrpRspData::Fetch { high_watermark, mut records } => {
                        metrics.size(&labels, &frame);
                        if let Some(ts) = times.remove(&id).await {
                            metrics.time(&labels, ts);
                        }
                        records.sort_by_key(|r| r.offset);
                        let mut done = true;
                        // if more data is available
                        if let Some(last) = records.last() {
                            // if the last item is not out of bounds
                            if less_record_offset(last, until) && last.offset < high_watermark {
                                done = false;
                                offset_snd.send(Offset::Offset(last.offset + 1)).await?;
                            }
                        }
                        write_records(id, records, until, &mut writer).await?;
                        if done {
                            break;
                        }
                    }
                    JrpRspData::Send { .. } => {
                        error!("error, id: {}, response: send", id);
                        return Err(JrpkError::Unexpected("send"));
                    }
                    JrpRspData::Offset(_) => {
                        error!("error, id: {}, response: offset", id);
                        return Err(JrpkError::Unexpected("offset"));
                    }
                }
            }
            Err(err) => {
                error!("error, id: {}, message: {}", id, err.message);
                return Err(JrpkError::Internal(err.message))
            }
        }
    }
    writer.flush()?;
    Ok(())
}

#[instrument(ret, err, skip(metrics, metrics_url))]
pub async fn consume(
    address: FastStr,
    tap: Tap,
    path: FastStr,
    from: Offset,
    until: Offset,
    max_batch_size: i32,
    max_wait_ms: i32,
    max_frame_size: usize,
    metrics: Arc<JrpkMetrics>,
    mut metrics_url: Url,
    metrics_period: Duration,

) -> Result<(), JrpkError> {

    url_append_tap(&mut metrics_url, &tap)?;
    let ph = spawn_push_prometheus(
        metrics_url,
        metrics_period,
        metrics.clone(),
    );

    let stream = TcpStream::connect(address.as_str()).await?;
    let codec = LinesCodec::new_with_max_length(max_frame_size);
    let framed = Framed::new(stream, codec);
    let (tcp_sink, tcp_stream) = framed.split();
    let (offset_snd, offset_rcv) = mpsc::channel::<Offset>(2);
    let times = Arc::new(Cache::builder().time_to_live(Duration::from_mins(1)).build());

    let wh = spawn(
        consumer_req_writer(
            tap.clone(),
            max_batch_size,
            max_wait_ms,
            metrics.clone(),
            times.clone(),
            offset_rcv,
            tcp_sink
        )
    );

    let rh = spawn(
        consumer_rsp_reader(
            tap,
            path,
            from,
            until,
            metrics,
            times,
            offset_snd,
            tcp_stream,
        )
    );

    let _ = try_join!(wh, rh);
    let _ = ph.cancel().await?;
    Ok(())
}
