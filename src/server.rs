use crate::codec::{BytesFrameDecoderError, JsonCodec, JsonEncoderError};
use crate::jsonrpc::{JrpError, JrpRecFetchCodecs, JrpReq, JrpRsp};
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
use tracing::{debug, error, info, trace};
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
    kfk_res_id_snd: JrpKfkResIdSnd,
    queue_size: usize,
) -> Result<(), ServerError> {

    while let Some(result) = stream.next().await {
        // if we cannot even decode frame - we disconnect
        let bytes = result?;
        trace!("reader, ctx: {}, json: {}", addr, from_utf8(bytes.as_ref())?);
        let jrp_req = serde_json::from_slice::<JrpReq>(bytes.as_ref())?;
        debug!("reader, ctx: {}, request: {:?}", addr, jrp_req);
        let id = jrp_req.id;
        let topic = jrp_req.params.topic.to_owned();
        let partition = jrp_req.params.partition;
        let kfk_req: JrpKfkReq = jrp_req.try_into()?;
        let kfk_req_id = ReqId::new(id, kfk_req, kfk_res_id_snd.clone());
        let kfk_req_id_snd = cache.lookup_kafka_sender(topic, partition, queue_size).await?;
        kfk_req_id_snd.send(kfk_req_id).await?;
    }
    Ok(())

}

async fn run_writer_loop(
    addr: SocketAddr,
    mut sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpRsp>,
    mut kfk_res_rcv: Receiver<JrpKfkResId>
) -> Result<(), ServerError> {
    while let Some(kfk_res_id) = kfk_res_rcv.recv().await {
        let rsp_jrp: JrpRsp = kfk_res_id.into();
        info!("writer, ctx: {}, response: {:?}", addr, rsp_jrp);
        sink.send(rsp_jrp).await?;
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
        let (kfk_res_snd, kfk_res_rcv) = mpsc::channel::<JrpKfkResId>(args.queue_size);
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
