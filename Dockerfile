FROM rust:latest

RUN rustup default stable
RUN cargo install tokio-console
RUN apt update && apt install -y \
    libjemalloc2 \
    graphviz golang-go google-perftools libgoogle-perftools-dev \
    libmimalloc-dev \
    bc heaptrack
RUN go install github.com/google/pprof@latest
ENV PATH=$PATH:~/go/bin
