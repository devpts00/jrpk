use crate::args::HostPort;
use crate::codec::JsonCodec;
use crate::errors::{JrpkError, JrpkResult};
use crate::util::handle_future;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::join;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, info};

pub async fn connect(target: HostPort, path: PathBuf) -> JrpkResult<()> {
    let stream = TcpStream::connect(target.to_string()).await?;
    let codec = JsonCodec::new();
    let framed = Framed::new(stream, codec);
    let (sink, stream) = framed.split();
    let jhi = tokio::spawn(handle_future("input", run_input_loop(target.clone(), stream)));
    let jho = tokio::spawn(handle_future("output", run_output_loop(target.clone(), path, sink)));
    let (ri, ro) = join!(jhi, jho);
    ri.or(ro)?;
    Ok(())
}

async fn run_input_loop(peer: HostPort, mut stream: SplitStream<Framed<TcpStream, JsonCodec>>) -> Result<(), JrpkError> {
    info!("input, start: {}", peer);
    loop {
        if let Some(result) = stream.next().await {
            let x = result;
            //let frame = result?;
            // let text = from_utf8(&frame)?;
            // debug!("input, response: {}", text);
        }
    }
}

async fn run_output_loop(peer: HostPort, path: PathBuf, mut sink: SplitSink<Framed<TcpStream, JsonCodec>, &[u8]>) -> JrpkResult<()> {
    info!("output, peer: {}, file: {:?}", peer, path);
    let file = File::open(&path).await?;
    let mut reader = BufReader::new(file);
    let mut data: Vec<u8> = Vec::with_capacity(32 * 1024);
    reader.read_to_end(&mut data).await?;
    reader.shutdown().await?;
    loop {
        let bytes = &data[..data.len()];
        debug!("output, size: {}", bytes.len());
        sink.feed(bytes).await?;
    }
}
