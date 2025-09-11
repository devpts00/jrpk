use crate::codec::{BytesFrameDecoderError, JsonCodec, JsonEncoderError};
use crate::jsonrpc::{JrpError, JrpErrorMsg, JrpId, JrpRecFetchCodecs, JrpReq, JrpRsp};
use crate::kafka::{KfkClientCache, KfkError, KfkReq, KfkReqId, KfkReqIdRcv, KfkReqIdSnd, KfkResId, KfkResIdRcv, KfkResIdSnd, KfkRsp, RsKafkaError};
use crate::util::{handle_future_result, set_buf_sizes, ReqId, ResId};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rskafka::client::ClientBuilder;
use std::net::SocketAddr;
use std::str::{from_utf8, from_utf8_unchecked, Utf8Error};
use std::sync::Arc;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, trace, warn};
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
    #[error("utf8: {0}")]
    Utf8(#[from] Utf8Error),
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

async fn run_reader_loop(
    addr: SocketAddr,
    mut stream: SplitStream<Framed<TcpStream, JsonCodec>>,
    cache: Arc<JrpKfkClientCache>,
    jrp_rsp_snd: Sender<JrpRsp>,
    queue_size: usize,
) -> Result<(), ServerError> {

    while let Some(result) = stream.next().await {
        // if we cannot even decode frame - we disconnect
        let bytes = result?;
        trace!("reader, ctx: {}, json: {}", addr, from_utf8(bytes.as_ref())?);
        // we are optimistic and expect most requests to be well-formed
        match serde_json::from_slice::<JrpReq>(bytes.as_ref()) {
            // if request is well-formed, we proceed
            Ok(jrp_req) => {
                debug!("reader, ctx: {}, request: {:?}", addr, jrp_req);
                let id = jrp_req.id;
                let topic = jrp_req.params.topic.to_owned();
                let partition = jrp_req.params.partition;
                let kfk_req: JrpKfkReq = jrp_req.try_into()?;
                let kfk_req_id = ReqId::new(id, kfk_req, jrp_rsp_snd.clone());
                let kfk_req_id_snd = cache.lookup_kafka_sender(topic, partition, queue_size).await?;
                kfk_req_id_snd.send(kfk_req_id).await?;
            }
            // if request is not well-formed we attempt to get at least and id to respond
            Err(err) => {
                warn!("jsonrpc decode error: {}", err);
                // if even an id is absent we give up and disconnect
                let jrp_id = serde_json::from_slice::<JrpId>(bytes.as_ref())?;
                let err_msg = format!("jsonrpc decode error: {}", err);
                let jrp_err_msg = JrpErrorMsg::new(err_msg);
                let jrp_rsp = JrpRsp::err(jrp_id.id, jrp_err_msg);
                jrp_rsp_snd.send(jrp_rsp).await?;
            }
        }
    }
    Ok(())

}

async fn run_writer_loop(
    addr: SocketAddr,
    mut sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpRsp>,
    mut kfk_res_rcv: Receiver<JrpRsp>
) -> Result<(), ServerError> {
    while let Some(jrp_rsp) = kfk_res_rcv.recv().await {
        info!("writer, ctx: {}, response: {:?}", addr, jrp_rsp);
        sink.send(jrp_rsp).await?;
    }
    sink.flush().await?;
    Ok(())
}

pub async fn listen(args: Args) -> Result<(), ServerError> {

    info!("listen, kafka: {:?}", args.brokers);
    let client = ClientBuilder::new(args.brokers.0).build().await?;
    let cache = Arc::new(KfkClientCache::new(client, 1024));

    info!("listen, bind: {:?}", args.bind);
    let listener = TcpListener::bind(args.bind).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("listen, accepted: {:?}", addr);
        set_buf_sizes(
            &stream,
            args.recv_buffer_size.as_u64() as usize,
            args.send_buffer_size.as_u64() as usize
        )?;
        let codec = JsonCodec::new(args.max_frame_size.as_u64() as usize);
        let framed = Framed::new(stream, codec);
        let (sink, stream) = framed.split();
        let (kfk_res_snd, kfk_res_rcv) = mpsc::channel::<JrpRsp>(args.queue_size);
        tokio::spawn(
            handle_future_result(
                "reader",
                addr,
                run_reader_loop(addr, stream, cache.clone(), kfk_res_snd, args.queue_size)
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
