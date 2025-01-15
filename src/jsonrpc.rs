use serde::{Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
enum Partition {
    #[serde(rename = "num")]
    Num(u16),
    #[serde(rename = "key")]
    Key(String),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "method", content = "params")]
enum Params {
    #[serde(rename = "send")]
    Send {
        topic: String,
        partition: Option<Partition>,
        body: serde_json::Value
    },
    #[serde(rename = "poll")]
    Poll {
        topic: String,
        partition: Option<Partition>,
        count: u16,
    }
}

#[derive(Debug, Deserialize)]
struct Request {
    jsonrpc: String,
    id: u64,
    #[serde(flatten)]
    params: Params,
}

#[cfg(test)]
mod tests {
    use tracing::info;
    use crate::jsonrpc::Request;

    #[test]
    fn test_deserialize() {
        let send: Request = serde_json::from_str(
            r#"{ "jsonrpc": "2.0", "id": 1, "method": "send", "params": { "topic": "posts", "partition": { "key": "john" }, "body": 1 } }"#
        ).unwrap();
        println!("send: {:?}", send);
        let poll: Request = serde_json::from_str(
            r#"{ "jsonrpc": "2.0", "id": 1, "method": "poll", "params": { "topic": "posts", "partition": { "num": 7 }, "count": 1024 } }"#
        ).unwrap();
        println!("poll: {:?}", poll);

    }
}