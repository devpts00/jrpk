use std::path::PathBuf;
use thiserror::Error;
use crate::args::Offset;

#[derive(Error, Debug)]
pub enum ClientError {

}

pub async fn consume(address: String, topic: String, partition: i32, from: Offset, until: Offset, file: PathBuf) -> Result<(), ClientError> {
    Ok(())
}

pub async fn producer_writer() -> Result<(), ClientError> {
    Ok(())
}

pub async fn producer_reader() -> Result<(), ClientError> {
    Ok(())
}

pub async fn produce(address: String, topic: String, partition: i32, file: PathBuf) -> Result<(), ClientError> {
    Ok(())
}
