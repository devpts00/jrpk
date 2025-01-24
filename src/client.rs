use crate::args::HostPort;
use std::io;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tracing::info;

pub async fn send_file(file: PathBuf, target: HostPort) -> io::Result<()> {
    info!("client, file: {:?}, target: {:?}", file, target);
    let file: File = File::open(&file).await?;
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

pub async fn send_text(target: HostPort) -> io::Result<()> {
    info!("client, target: {:?}", target);
    let text = r#"
        { "jsonrpc": "2.0", "id": 1, "method": "send", "params": { "topic": "posts", "key": "john", "payload": { "first": "john", "last": "doe", "age": 35 } } }
        { "jsonrpc": "2.0", "id": 1, "method": "send", "params": { "topic": "posts", "key": "john", "payload": { "first": "john", "last": "doe", "age": 35 } } }
        { "jsonrpc": "2.0", "id": 1, "method": "send", "params": { "topic": "posts", "partition": 1, "payload": { "first": "john", "last": "doe", "age": 35 } } }
        { "jsonrpc": "2.0", "id": 1, "method": "send", "params": { "topic": "posts", "partition": 2, "payload": { "first": "john", "last": "doe", "age": 35 } } }
        { "jsonrpc": "2.0", "id": 1, "method": "send", "params": { "topic": "posts", "partition": 3, "payload": { "first": "{john}", "last": "\"doe 15\u00f8C", "age": 35 } } }
    "#;
    let stream = TcpStream::connect(target.to_string()).await?;
    let mut writer = BufWriter::new(stream);
    let mut reader = BufReader::new(text.as_bytes());
    tokio::io::copy(&mut reader, &mut writer).await?;
    writer.flush().await?;
    writer.shutdown().await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    Ok(())
}
