use std::net::SocketAddr;
use std::str::from_utf8;
use anyhow::{anyhow, Error};
use anyhow::Result;
use log::{error, trace, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::info;
use crate::args::HostPort;
use crate::jsonrpc::Codec;
use crate::util::handle;

pub async fn listen(bind: SocketAddr, brokers: Vec<HostPort>) -> Result<()> {
    info!("server, bind: {:?}, brokers: {:?}", bind, brokers);
    let listener = TcpListener::bind(bind).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        info!("accepted: {:?}", addr);
        tokio::spawn(async move {
            handle(process(stream).await)
        });
    }
}

pub async fn process(stream: TcpStream) -> Result<()> {
    let mut framed: Framed<TcpStream, Codec> = Framed::with_capacity(stream, Codec::new(), 8192);
    while let Some(item) = framed.next().await {
        let json = item?;
        info!("json: {:?}", json);
    }
    Ok(())
}
