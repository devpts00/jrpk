use crate::args::HostPort;
use crate::jsonrpc::{JrpReq, JrpRsp};
use crate::kafka::{KfkClientCache, KfkReq, KfkResIdSnd, KfkRsp, KfkResId};
use crate::util::{handle_future, ReqId};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rskafka::client::ClientBuilder;
use std::net::SocketAddr;
use std::str::from_utf8_unchecked;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, trace};
use crate::codec::JsonCodec;
use crate::errors::JrpkResult;

async fn run_input_loop(addr: SocketAddr, mut stream: SplitStream<Framed<TcpStream, JsonCodec>>, ctx: Arc<KfkClientCache>, kfk_res_id_snd: KfkResIdSnd) -> JrpkResult<()> {
    info!("input, start: {}", addr);
    while let Some(result) = stream.next().await {
        // if we cannot even decode frame - we disconnect
        let bytes = result?;
        let raw_str = unsafe { from_utf8_unchecked(bytes.as_ref()) };
        debug!("input, json: {}", raw_str);
        match serde_json::from_slice::<JrpReq>(bytes.as_ref()) {
            Ok(jrp_req) => {
                debug!("input, request: {:?}", jrp_req);
                let id = jrp_req.id;
                let topic = jrp_req.params.topic.to_owned();
                let partition = jrp_req.params.partition;
                let kfk_req: KfkReq = jrp_req.try_into()?;
                let kfk_req_id = ReqId::new(id, kfk_req, kfk_res_id_snd.clone());
                let kfk_req_id_snd = ctx.lookup_kafka_sender(topic, partition, 64).await?;
                kfk_req_id_snd.send(kfk_req_id).await?;
            }
            Err(err) => {
                error!("input, error: {}", err);
            }
        }
    }
    info!("input, end: {}", addr);
    Ok(())
}

async fn run_output_loop(addr: SocketAddr, mut sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpRsp>, mut kfk_res_id_rcv: Receiver<KfkResId>) -> JrpkResult<()> {
    info!("output, start: {}", addr);
    while let Some(kfk_res_id) = kfk_res_id_rcv.recv().await {
        let rsp_jrp: JrpRsp = kfk_res_id.into();
        debug!("output, response: {:?}", rsp_jrp);
        sink.send(rsp_jrp).await?;
    }
    info!("output, end: {}", addr);
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
        info!("server, accepted: {:?}", addr);
        let codec = JsonCodec::new();
        let framed = Framed::new(stream, codec);
        let (sink, stream) = framed.split();
        let (kfk_res_id_snd, kfk_res_id_rcv) = mpsc::channel::<KfkResId>(1024);
        tokio::spawn(handle_future("input", run_input_loop(addr, stream, ctx.clone(), kfk_res_id_snd)));
        tokio::spawn(handle_future("output", run_output_loop(addr, sink, kfk_res_id_rcv)));
    }
}
