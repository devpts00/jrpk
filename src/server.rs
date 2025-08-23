use crate::args::HostPort;
use crate::jsonrpc::{JrpReq, JrpRsp};
use crate::kafka::{KfkClientCache, KfkReq, KfkResIdSnd, KfkRsp, KfkResId};
use crate::util::{handle_future, ReqId};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rskafka::client::ClientBuilder;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd};
use std::str::from_utf8_unchecked;
use std::sync::Arc;
use socket2::SockRef;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, trace};
use crate::codec::JsonCodec;
use crate::errors::JrpkResult;
use crate::QUEUE_SIZE;

async fn run_input_loop(addr: SocketAddr, mut stream: SplitStream<Framed<TcpStream, JsonCodec>>, ctx: Arc<KfkClientCache>, kfk_res_id_snd: KfkResIdSnd) -> JrpkResult<()> {
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
                let kfk_req: KfkReq = jrp_req.try_into()?;
                let kfk_req_id = ReqId::new(id, kfk_req, kfk_res_id_snd.clone());
                let kfk_req_id_snd = ctx.lookup_kafka_sender(topic, partition, QUEUE_SIZE).await?;
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

async fn run_output_loop(addr: SocketAddr, mut sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpRsp>, mut kfk_res_id_rcv: Receiver<KfkResId>) -> JrpkResult<()> {
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

pub async fn listen(bind: SocketAddr, brokers: Vec<HostPort>) -> JrpkResult<()> {

    info!("server, kafka: {:?}", brokers);
    let bs: Vec<String> = brokers.iter().map(|hp| { hp.to_string() }).collect();
    let client = ClientBuilder::new(bs).build().await?;
    let ctx = Arc::new(KfkClientCache::new(client, 1024));

    info!("server, listen: {:?}", bind);
    let listener = TcpListener::bind(bind).await?;

    loop {
        let (stream, addr) = listener.accept().await?;

        let socket = SockRef::from(&stream);
        info!("recv: {}, send: {}", socket.recv_buffer_size()?, socket.send_buffer_size()?);
        socket.set_recv_buffer_size(1024)?;
        socket.set_send_buffer_size(1024)?;
        info!("recv: {}, send: {}", socket.recv_buffer_size()?, socket.send_buffer_size()?);
        drop(socket);

        info!("server, accepted: {:?}", addr);

        let codec = JsonCodec::new();
        let framed = Framed::new(stream, codec);
        let (sink, stream) = framed.split();
        let (kfk_res_id_snd, kfk_res_id_rcv) = mpsc::channel::<KfkResId>(QUEUE_SIZE);
        tokio::spawn(handle_future("input", run_input_loop(addr, stream, ctx.clone(), kfk_res_id_snd)));
        tokio::spawn(handle_future("output", run_output_loop(addr, sink, kfk_res_id_rcv)));
    }
}
