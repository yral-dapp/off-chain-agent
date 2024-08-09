#!/bin/bash

# export $(grep -v '^#' .env.example | xargs)
cargo run --features local-bin
