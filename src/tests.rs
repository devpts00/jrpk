#[cfg(test)]

use crate::init_tracing;
use futures::stream::{SplitSink, SplitStream};
use rand::Rng;
use random_data::{DataGenerator, DataType};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::ops::Add;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use tracing::{debug, error, info};


#[test]
fn test_produce() -> () {
    init_tracing();

    info!("test, start");

    let addr = "jrpk:1133";

    let rs = TcpStream::connect(addr).unwrap();
    let ws = rs.try_clone().unwrap();

    let rh = thread::spawn(move || {
        info!("reader, start");
        rs.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let mut reader = BufReader::with_capacity(4 * 1024, rs);
        let mut line = String::with_capacity(4 * 1024);
        while let Ok(n) = reader.read_line(&mut line) {
            if n > 0 {
                line.pop();
                debug!("response: {}", line);
                line.clear()
            } else {
                break;
            }
        }
        info!("reader, end");
    });

    let wh = thread::spawn(move || {
        info!("writer, start");
        let mut writer = BufWriter::with_capacity(4 * 1024, ws);
        let mut rng = rand::rng();
        let mut gen = DataGenerator::default();
        let total: usize = rng.random_range(1000000..2000000);
        for n in 0..total {
            let size: usize = rng.random_range(500..1000);
            let mut comma = false;
            let id: u32 = rng.random();
            write!(&mut writer, r#"{{ "jsonrpc": "2.0", "id": {}, "method": "send", "params": {{ "topic": "posts", "partition": 0, "records": "#, id).unwrap();
            writer.write("[".as_bytes()).unwrap();
            for n in 0..size {
                if comma {
                    writer.write(",".as_bytes()).unwrap();
                } else {
                    comma = true;
                }
                let key: String = DataType::Country.random(&mut gen);
                let first: String = DataType::FirstName.random(&mut gen);
                let last: String = DataType::LastName.random(&mut gen);
                let age: u8 = rng.random_range(10..80);
                write!(&mut writer, r#"{{"key": "{}", "value": {{ "first": "{}", "last": "{}", "age": {} }}}}"#, key, first, last, age).unwrap();
            }
            writer.write("] }}".as_bytes()).unwrap();
            writer.flush().unwrap();
        }
        info!("writer, end");
    });

    match wh.join() {
        Ok(_) => info!("writer, success"),
        Err(e) => error!("writer, error: {:?}", e),
    }

    match rh.join() {
        Ok(_) => info!("reader, success"),
        Err(e) => error!("reader, error: {:?}", e),
    }

    info!("test, end");

}
