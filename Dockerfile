FROM debian:bookworm-20240211

WORKDIR /app

COPY ./target/x86_64-unknown-linux-musl/release/icp-off-chain-agent .

RUN apt-get update && apt-get install -y ca-certificates

EXPOSE 50051

CMD ["./icp-off-chain-agent"]
