use crate::errors::JrpkError;
use clap::Parser;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::ops::Deref;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl Display for HostPort {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for HostPort {
    type Err = JrpkError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (host, port) = s.split_once(':')
            .ok_or(JrpkError::General("endpoint must be host:port"))?;
        Ok(HostPort {
            host: String::from(host),
            port: u16::from_str_radix(port, 10)?
        })
    }
}

#[derive(Debug, Clone)]
pub struct Ctr<C>(pub C);

impl<C> Deref for Ctr<C> {
    type Target = C;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: FromStr> FromStr for Ctr<Vec<T>> {
    type Err = T::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let vec: Result<Vec<T>, T::Err> = s.split(',')
            .map(T::from_str)
            .collect();
        Ok(Ctr(vec?))
    }
}

impl<T: Display> Display for Ctr<Vec<T>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for x in self.0.iter() {
            write!(f, "{}", x)?
        }
        Ok(())
    }
}

impl<C> From<C> for Ctr<C> {
    fn from(value: C) -> Self {
        Ctr(value)
    }
}

#[derive(Debug, Clone, Parser)]
pub struct Args {
    #[arg(long)]
    pub brokers: Ctr<Vec<HostPort>>,
    #[arg(long)]
    pub bind: SocketAddr,
}
