set dotenv-required

default:
    just --list

build tag:
    docker build -f Dockerfile-local \
    --build-arg GOOGLE_SA_KEY=$GOOGLE_SA_KEY \
    --build-arg YRAL_METADATA_TOKEN=$YRAL_METADATA_TOKEN \
    --build-arg QSTASH_CURRENT_SIGNING_KEY=$QSTASH_CURRENT_SIGNING_KEY \
    --build-arg QSTASH_AUTH_TOKEN=$QSTASH_AUTH_TOKEN \
    --build-arg STORJ_INTERFACE_TOKEN=$STORJ_INTERFACE_TOKEN \
    -t ghcr.io/yral-dapp/off-chain-agent:{{tag}} .
