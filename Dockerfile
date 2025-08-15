FROM rust:latest

RUN rustup default stable
RUN cargo install tokio-console