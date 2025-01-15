use crate::args::HostPort;
use std::io;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadBuf};
use tokio::net::TcpStream;
use tracing::info;

pub async fn connect(file: PathBuf, target: HostPort) -> io::Result<()> {
    info!("client, file: {:?}, target: {:?}", file, target);
    let mut file: File = File::open(&file).await?;
    let mut reader = BufReader::new(file);
    let mut stream = TcpStream::connect(target.to_string()).await?;
    let mut writer = BufWriter::new(stream);
    tokio::io::copy(&mut reader, &mut writer).await?;
    writer.flush().await?;
    writer.shutdown().await?;
    reader.shutdown().await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    Ok(())
}
