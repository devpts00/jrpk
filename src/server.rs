use crate::args::HostPort;
use crate::jsonrpc::{JrpReq, JrpRsp};
use crate::kafka::{KfkClientCache, KfkReq, KfkResIdSnd, KfkRsp, KfkResId};
use crate::util::{handle_future, ReqId};
use anyhow::{Result};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rskafka::client::ClientBuilder;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::Framed;
use tracing::{error, info, trace, warn};
use crate::codec::JsonCodec;

async fn run_send_requests_loop(addr: SocketAddr, mut stream: SplitStream<Framed<TcpStream, JsonCodec>>, ctx: Arc<KfkClientCache>, kfk_res_id_snd: KfkResIdSnd) -> Result<()> {
    info!("input, addr: {} - START", addr);
    while let Some(result) = stream.next().await {
        // if we cannot even decode frame - we disconnect
        let bytes = result?;
        match serde_json::from_slice::<JrpReq>(bytes.as_ref()) {
            Ok(jrp_req) => {
                trace!("input, json: {}", jrp_req);
                let id = jrp_req.id;
                let topic = jrp_req.params.topic.to_owned();
                let partition = jrp_req.params.partition;
                let kfk_req: KfkReq = jrp_req.try_into()?;
                let kfk_req_id = ReqId::new(id, kfk_req, kfk_res_id_snd.clone());
                let kfk_req_id_snd = ctx.lookup_kafka_sender(topic, partition, 64).await?;
                kfk_req_id_snd.send(kfk_req_id).await?;
            }
            Err(err) => {
                error!("error: {}", err);
            }
        }
    }
    info!("input, addr: {} - END", addr);
    Ok(())
}

async fn run_receive_responses_loop(addr: SocketAddr, mut sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpRsp>, mut kfk_res_id_rcv: Receiver<KfkResId>) -> Result<()> {
    info!("output, addr: {} - START", addr);
    while let Some(kfk_res_id) = kfk_res_id_rcv.recv().await {
        let rsp_jrp: JrpRsp = kfk_res_id.into();
        trace!("output, response: {}", rsp_jrp);
        sink.send(rsp_jrp).await?;
    }
    info!("output, addr: {} - END", addr);
    Ok(())
}

pub async fn listen(bind: SocketAddr, brokers: Vec<HostPort>) -> Result<()> {

    info!("server, kafka: {:?}", brokers);
    let bs: Vec<String> = brokers.iter().map(|hp| { hp.to_string() }).collect();
    let client = ClientBuilder::new(bs).build().await?;
    let ctx = Arc::new(KfkClientCache::new(client, 1024));

    info!("server, bind: {:?}", bind);
    let listener = TcpListener::bind(bind).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("accepted: {:?}", addr);
        let codec = JsonCodec::new();
        let framed = Framed::new(stream, codec);
        let (sink, stream) = framed.split();
        let (kfk_res_id_snd, kfk_res_id_rcv) = mpsc::channel::<KfkResId>(1024);
        tokio::spawn(
            handle_future(
                run_send_requests_loop(addr, stream, ctx.clone(), kfk_res_id_snd),
            )
        );
        tokio::spawn(
            handle_future(
                run_receive_responses_loop(addr, sink, kfk_res_id_rcv)
            )
        );
    }
}
