use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use faststr::FastStr;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use moka::future::Cache;
use reqwest::Url;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::{spawn, try_join};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::block_in_place;
use tokio_util::codec::Framed;
use tracing::{debug, error, instrument, trace};
use crate::args::FileFormat;
use crate::codec::LinesCodec;
use crate::error::JrpkError;
use crate::model::{write_records, JrpCodec, JrpOffset, JrpRecFetch, JrpReq, JrpRsp, JrpRspData, JrpSelector, Progress};
use crate::metrics::{spawn_push_prometheus, JrpkMetrics, JrpkLabels, LblMethod, LblTier, LblTraffic, MeteredItem};
use crate::util::{url_append_tap, Budget, Tap, VecBufWriter};

type JrpkMeteredConsReq<'a> = MeteredItem<JrpReq<'a>>;

#[inline]
fn less_record_offset(record: &JrpRecFetch, offset: JrpOffset) -> bool {
    match offset {
        JrpOffset::Earliest => false,
        JrpOffset::Latest => true,
        JrpOffset::Timestamp(timestamp) => record.timestamp < timestamp,
        JrpOffset::Offset(pos) => record.offset < pos,
    }
}

fn test<W: Write, S: Serialize>(writer: &mut W, data: S) {
    let w: &mut W = writer;
    serde_json::to_writer(w, &data).unwrap();
    writer.write_all(b"\n").unwrap();
}

/*
#[instrument(ret, err, level="trace", skip(records, buf, writer))]
async fn write_records<'a, W: Write>(
    format: Format,
    records: Vec<JrpRecFetch<'a>>,
    until: Offset,
    budget: &mut Budget,
    buf: &mut Vec<u8>,
    writer: &mut W,
) -> Result<(), JrpkError> {
    block_in_place(|| {
        let records = records.into_iter()
            .take_while(|r| less_record_offset(r, until));
        match format {
            Format::Value => {
                for record in records {
                    if let Some(value) = record.value {
                        if !budget.slice_to_writer(buf, writer, value.as_text().as_bytes())? {
                            break
                        }
                    }
                }
            }
            Format::Record => {
                for record in records {
                    if !budget.json_to_writer(buf, writer, &record)? {
                        break
                    }
                }
            }
        }
        writer.flush()?;
        Ok(())
    })
}
 */

#[instrument(ret, err, skip(metrics, times, offset_rcv, tcp_sink))]
async fn consumer_req_writer<'a>(
    jrp_selector: JrpSelector,
    kfk_tap: Tap,
    kfk_fetch_min_size: i32,
    kfk_fetch_max_size: i32,
    kfk_fetch_max_wait_ms: i32,
    metrics: Arc<JrpkMetrics>,
    times: Arc<Cache<usize, Instant>>,
    mut offset_rcv: Receiver<JrpOffset>,
    mut tcp_sink: SplitSink<Framed<TcpStream, LinesCodec>, JrpkMeteredConsReq<'a>>,
) -> Result<(), JrpkError> {
    let labels = JrpkLabels::new(LblTier::Client)
        .method(LblMethod::Fetch)
        .traffic(LblTraffic::Out)
        .tap(kfk_tap.clone())
        .build();
    let mut id = 0;
    while let Some(jrp_offset) = offset_rcv.recv().await {
        let tap = kfk_tap.clone();
        let bytes = kfk_fetch_min_size..kfk_fetch_max_size;
        let jrp_req_fetch = JrpReq::fetch(id, tap.topic, tap.partition, jrp_offset, bytes, kfk_fetch_max_wait_ms, jrp_selector.clone());
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
    kfk_tap: Tap,
    kfk_from: JrpOffset,
    kfk_until: JrpOffset,
    file_path: FastStr,
    file_format: FileFormat,
    file_save_max_rec_count: usize,
    file_save_max_size: usize,
    metrics: Arc<JrpkMetrics>,
    times: Arc<Cache<usize, Instant>>,
    offset_snd: Sender<JrpOffset>,
    mut tcp_stream: SplitStream<Framed<TcpStream, LinesCodec>>,
) -> Result<(), JrpkError> {
    let labels = JrpkLabels::new(LblTier::Client)
        .method(LblMethod::Fetch)
        .traffic(LblTraffic::In)
        .tap(kfk_tap)
        .build();
    let file = File::create(file_path.as_str())?;
    let mut writer = VecBufWriter::with_capacity(64 * 1024, file);
    let mut budget = Budget::new(file_save_max_size, file_save_max_rec_count);
    offset_snd.send(kfk_from).await?;
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
                        // if more data is available
                        if let Some(last) = records.last() {
                            // if the last item is not out of bounds
                            if less_record_offset(last, kfk_until) && last.offset < high_watermark {
                                offset_snd.send(JrpOffset::Offset(last.offset + 1)).await?;
                            }
                        }
                        let records = records.into_iter().map(|r| Ok(r));
                        let progress = block_in_place(|| {
                            write_records(file_format, records, kfk_until.into(), &mut budget, &mut writer)
                        })?;

                        match progress {
                            Progress::Continue(offset) => {
                                trace!("continue, offset: {}", offset)
                            }
                            Progress::Done => {
                                debug!("done");
                                break;
                            }
                            Progress::Overdraft => {
                                debug!("break: budget overdraft");
                                break;
                            }
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
    Ok(())
}

#[instrument(ret, err, skip(prom_push_url))]
pub async fn consume(
    jrp_address: FastStr,
    jrp_frame_max_size: usize,
    jrp_offset_from: JrpOffset,
    jrp_offset_until: JrpOffset,
    jrp_key_codec: Option<JrpCodec>,
    jrp_value_codec: JrpCodec,
    jrp_header_codecs: Vec<(FastStr, JrpCodec)>,
    jrp_header_codec_default: Option<JrpCodec>,
    kfk_topic: FastStr,
    kfk_partition: i32,
    kfk_fetch_min_size: i32,
    kfk_fetch_max_size: i32,
    kfk_fetch_max_wait_time_ms: i32,
    file_path: FastStr,
    file_format: FileFormat,
    file_save_max_rec_count: usize,
    file_save_max_size: usize,
    mut prom_push_url: Url,
    prom_push_period: Duration,
) -> Result<(), JrpkError> {

    let jrp_selector = JrpSelector::new(jrp_key_codec, jrp_value_codec, jrp_header_codecs, jrp_header_codec_default);
    let kfk_tap = Tap::new(kfk_topic, kfk_partition);
    let metrics = Arc::new(JrpkMetrics::new());

    url_append_tap(&mut prom_push_url, &kfk_tap)?;
    let ph = spawn_push_prometheus(
        prom_push_url,
        prom_push_period,
        metrics.clone(),
    );

    let stream = TcpStream::connect(jrp_address.as_str()).await?;
    let codec = LinesCodec::new_with_max_length(jrp_frame_max_size);
    let framed = Framed::new(stream, codec);
    let (tcp_sink, tcp_stream) = framed.split();
    let (offset_snd, offset_rcv) = mpsc::channel::<JrpOffset>(2);
    let times = Arc::new(Cache::builder().time_to_live(Duration::from_mins(1)).build());

    let wh = spawn(
        consumer_req_writer(
            jrp_selector,
            kfk_tap.clone(),
            kfk_fetch_min_size,
            kfk_fetch_max_size,
            kfk_fetch_max_wait_time_ms,
            metrics.clone(),
            times.clone(),
            offset_rcv,
            tcp_sink
        )
    );

    let rh = spawn(
        consumer_rsp_reader(
            kfk_tap,
            jrp_offset_from,
            jrp_offset_until,
            file_path,
            file_format,
            file_save_max_rec_count,
            file_save_max_size,
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
