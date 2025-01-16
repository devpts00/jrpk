use crate::args::HostPort;
use crate::serde::{Codec, Params, Request};
use crate::util::handle;
use anyhow::Result;
use rdkafka::producer::BaseRecord;
use std::net::SocketAddr;
use std::os::raw::c_void;
use rdkafka::IntoOpaque;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tracing::info;
use crate::kafka::{send};

struct OpaqueId(usize);

impl IntoOpaque for OpaqueId {
    fn into_ptr(self) -> *mut c_void {
        self.0 as *mut c_void
    }
    unsafe fn from_ptr(p: *mut c_void) -> Self {
        OpaqueId(p as usize)
    }
}

pub type RequestRecord<'a> = BaseRecord<'a, String, String, usize>;

fn record<'a>(id: usize, topic: &'a String, key: Option<&'a String>, partition: Option<i32>, payload: &'a String) -> RequestRecord<'a> {
    RequestRecord {
        topic,
        partition,
        payload: Some(payload),
        key,
        timestamp: None,
        headers: None,
        delivery_opaque: id,
    }
}

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

async fn process(stream: TcpStream) -> Result<()> {
    let mut framed: Framed<TcpStream, Codec> = Framed::with_capacity(stream, Codec::new(), 8192);
    while let Some(item) = framed.next().await {
        let request = item?;
        info!("request: {:?}", request);
        match request.params {
            Params::Send { topic, key, partition, payload } => {
                let text = payload.to_string();
                let record = record(request.id, &topic, key.as_ref(), partition, &text);
                send(&record).expect("TODO: panic message");
            }
            Params::Poll { .. } => {

            }
        }

    }
    Ok(())
}
