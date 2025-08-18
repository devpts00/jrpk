use crate::args::HostPort;
use std::io;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tracing::info;

pub async fn send_file(path: PathBuf, target: HostPort) -> io::Result<()> {
    info!("client, file: {:?}, target: {:?}", path, target);
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);
    let stream = TcpStream::connect(target.to_string()).await?;
    let mut writer = BufWriter::new(stream);
    tokio::io::copy(&mut reader, &mut writer).await?;
    writer.flush().await?;
    writer.shutdown().await?;
    reader.shutdown().await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    Ok(())
}
