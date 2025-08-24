use clap::Parser;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::ops::Deref;
use std::str::FromStr;
use bytesize::ByteSize;

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
    pub brokers: Ctr<Vec<String>>,
    #[arg(long)]
    pub bind: SocketAddr,
    #[arg(long)]
    pub max_frame_size: ByteSize,
}
