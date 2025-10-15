use std::path::PathBuf;
use bytesize::ByteSize;
use thiserror::Error;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt, TryFutureExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::info;
use crate::args::Offset;
use crate::codec::JsonCodec;
use crate::jsonrpc::{JrpMethod, JrpParams, JrpRecSend, JrpReq, JrpRsp};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("io: {0}")]
    IO(#[from] std::io::Error),
}

async fn consumer_writer<'a>(mut sink: SplitSink<Framed<TcpStream, JsonCodec>, JrpReq<'a>>) -> Result<(), ClientError> {
    
    
    
    Ok(())
}

async fn consumer_reader(mut stream: SplitStream<Framed<TcpStream, JsonCodec>>) -> Result<(), ClientError> {
    while let Some(result) = stream.next().await {
        
    }
    Ok(())
}

pub async fn consume(
    address: String,
    topic: String,
    partition: i32,
    from: Offset,
    until: Offset,
    file: PathBuf,
    max_frame_size: ByteSize,
) -> Result<(), ClientError> {

    info!("consume, address: {}, topic: {}, partition: {}, from: {:?}, until: {:?}, file: {:?}, max_frame_size: {}",
        address, topic, partition, from, until, file, max_frame_size
    );
    let stream = TcpStream::connect(address).await?;
    info!("connected: {}", stream.peer_addr()?);
    let codec = JsonCodec::new(max_frame_size.as_u64() as usize);
    let framed = Framed::new(stream, codec);
    let (sink, stream) = framed.split();

    consumer_writer(sink).await?;
    consumer_reader(stream).await?;

    Ok(())
}

pub async fn producer_writer() -> Result<(), ClientError> {
    Ok(())
}

pub async fn producer_reader() -> Result<(), ClientError> {
    Ok(())
}

pub async fn produce(address: String, topic: String, partition: i32, file: PathBuf, max_frame_size: ByteSize) -> Result<(), ClientError> {
    Ok(())
}
