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

struct JsonRpcCodec {
    level: u8,
    start: usize,
    position: usize,
    count: usize,
}

impl JsonRpcCodec {
    fn new() -> Self {
        JsonRpcCodec {
            level: 0,
            start: 0,
            position: 0,
            count: 0
        }
    }
}

impl Decoder for JsonRpcCodec {

    type Item = serde_json::Value;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {

        while self.position < src.len() {
            match src[self.position] {
                b'{'=> {
                    if self.level == 0 {
                        self.start = self.position;
                    }
                    self.level += 1;
                    self.position += 1;
                }
                b'}' => {
                    self.level -= 1;
                    self.position += 1;
                    if self.level == 0 {
                        let ready = src.split_to(self.position);
                        let buf = ready.get(self.start..self.position)
                            .ok_or(anyhow!("bad slice indices"))?;
                        let json = serde_json::from_slice(buf)?;
                        self.position = 0;
                        self.start = 0;
                        self.count += 1;
                        return Ok(Some(json))
                    }
                }
                _ => {
                    self.position += 1;
                }
            }
        }

        //info!("level: {}, count: {}, range: {} - {}", self.level, self.count, self.start, self.position);

        Ok(None)
    }
}

impl Encoder<serde_json::Value> for JsonRpcCodec {
    type Error = anyhow::Error;
    fn encode(&mut self, item: serde_json::Value, dst: &mut BytesMut) -> std::result::Result<(), Self::Error> {
        Ok(serde_json::to_writer(dst.writer(), &item)?)
    }
}

pub async fn process(stream: TcpStream) -> Result<()> {
    let mut framed: Framed<TcpStream, JsonRpcCodec> = Framed::with_capacity(stream, JsonRpcCodec::new(), 8192);
    while let Some(item) = framed.next().await {
        let json = item?;
        //info!("json: {:?}", json);
    }
    Ok(())
}
