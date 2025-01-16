use anyhow::Result;
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use tracing::info;
use crate::server::RequestRecord;

pub fn producer(brokers: &str) -> Result<BaseProducer> {
    let p: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;
    Ok(p)
}

pub fn send(rec: &RequestRecord) -> Result<()> {
    info!("record: {:?}", rec);
    Ok(())
}

pub async fn poll(p: &BaseProducer) {
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    p.poll(None);
}

