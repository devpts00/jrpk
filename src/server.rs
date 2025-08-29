use crate::codec::{BytesFrameDecoderError, JsonCodec, JsonEncoderError};
use crate::jsonrpc::{JrpError, JrpRecFetchCodecs, JrpReq, JrpRsp};
use crate::kafka::{KfkClientCache, KfkError, KfkReq, KfkReqId, KfkReqIdRcv, KfkReqIdSnd, KfkResId, KfkResIdRcv, KfkResIdSnd, KfkRsp, RsKafkaError};
use crate::util::{handle_future, set_buf_sizes, ReqId, ResId};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rskafka::client::ClientBuilder;
use std::net::SocketAddr;
use std::str::from_utf8_unchecked;
use std::sync::Arc;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::Framed;
use tracing::{error, info, trace};
use crate::args::Args;

type JrpKfkReq = KfkReq<JrpRecFetchCodecs>;
type JrpKfkClientCache = KfkClientCache<JrpRecFetchCodecs>;
type JrpKfkReqId = KfkReqId<JrpRecFetchCodecs>;
type JrpKfkResId = KfkResId<JrpRecFetchCodecs>;
type JrpKfkResIdSnd = KfkResIdSnd<JrpRecFetchCodecs>;
type JrpKfkResIdRcv = KfkResIdRcv<JrpRecFetchCodecs>;
type JrpKfkReqIdSnd = KfkReqIdSnd<JrpRecFetchCodecs>;
type JrpKfkReqIdRcv = KfkReqIdRcv<JrpRecFetchCodecs>;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("decoder: {0}")]
    Decoder(#[from] BytesFrameDecoderError),
    #[error("encoder: {0}")]
    Encoder(#[from] JsonEncoderError),
    #[error("json: {0}")]
    Json(#[from] serde_json::error::Error),
    #[error("kafka: {0}")]
    Kafka(#[from] KfkError),
    #[error("jsonrpc: {0}")]
    Jsonrpc(#[from] JrpError),
    #[error("send: {0}")]
    Send(SendError<()>),
    #[error("rs kafka: {0}")]
    Rs(#[from] RsKafkaError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// deliberately drop payload
impl <T> From<SendError<T>> for ServerError {
    fn from(value: SendError<T>) -> Self {
        ServerError::Send(SendError(()))
    }
}

async fn run_input_loop(
    addr: SocketAddr,
    mut stream: SplitStream<Framed<TcpStream, JsonCodec>>,
    ctx: Arc<JrpKfkClientCache>,
    kfk_res_id_snd: JrpKfkResIdSnd,
    queue_size: usize,
) -> Result<(), ServerError> {
    info!("input, addr: {} - START", addr);
    while let Some(result) = stream.next().await {
        // if we cannot even decode frame - we disconnect
        let bytes = result?;
        let raw_str = unsafe { from_utf8_unchecked(bytes.as_ref()) };
        trace!("input, addr: {}, json: {}", addr, raw_str);
        match serde_json::from_slice::<JrpReq>(bytes.as_ref()) {
            Ok(jrp_req) => {
                trace!("input, addr: {}, request: {:?}", addr, jrp_req);
                let id = jrp_req.id;
                let topic = jrp_req.params.topic.to_owned();
                let partition = jrp_req.params.partition;
                let kfk_req: JrpKfkReq = jrp_req.try_into()?;
                let kfk_req_id = ReqId::new(id, kfk_req, kfk_res_id_snd.clone());
                let kfk_req_id_snd = ctx.lookup_kafka_sender(topic, partition, queue_size).await?;
                kfk_req_id_snd.send(kfk_req_id).await?;
            }
            Err(err) => {
                error!("input, error: {}", err);
            }
        }
    }
    info!("input, addr: {} - END", addr);
    Ok(())
}

async fn run_output_loop(
    addr: SocketAddr,
    mut sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpRsp>,
    mut kfk_res_id_rcv: Receiver<JrpKfkResId>
) -> Result<(), ServerError> {
    info!("output, addr: {} - START", addr);
    while let Some(kfk_res_id) = kfk_res_id_rcv.recv().await {
        let rsp_jrp: JrpRsp = kfk_res_id.into();
        trace!("output, addr: {}, response: {:?}", addr, rsp_jrp);
        sink.feed(rsp_jrp).await?;
        sink.flush().await?;
    }
    sink.flush().await?;
    info!("output, addr: {} - END", addr);
    Ok(())
}

pub async fn listen(args: Args) -> Result<(), ServerError> {

    info!("server, kafka: {:?}", args.brokers);
    let client = ClientBuilder::new(args.brokers.0).build().await?;
    let ctx = Arc::new(KfkClientCache::new(client, 1024));

    info!("server, listen: {:?}", args.bind);
    let listener = TcpListener::bind(args.bind).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("server, accepted: {:?}", addr);
        set_buf_sizes(
            &stream,
            args.recv_buffer_size.as_u64() as usize,
            args.send_buffer_size.as_u64() as usize
        )?;
        let codec = JsonCodec::new(args.max_frame_size.as_u64() as usize);
        let framed = Framed::new(stream, codec);
        let (sink, stream) = framed.split();
        let (kfk_res_id_snd, kfk_res_id_rcv) = mpsc::channel::<JrpKfkResId>(args.queue_size);
        tokio::spawn(handle_future("input", run_input_loop(addr, stream, ctx.clone(), kfk_res_id_snd, args.queue_size)));
        tokio::spawn(handle_future("output", run_output_loop(addr, sink, kfk_res_id_rcv)));
    }
}
