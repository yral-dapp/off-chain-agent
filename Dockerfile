FROM ubuntu:latest

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    ffmpeg \
    golang-go \
    libc6 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 50051

COPY ./target/x86_64-unknown-linux-gnu/release/icp-off-chain-agent .
CMD ["./icp-off-chain-agent"]
