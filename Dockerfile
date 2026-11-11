# syntax=docker/dockerfile:1
# Multi-stage build: Rust builder -> distroless runtime (glibc, CA certs, non-root).
# librdkafka (cmake-build) and OpenSSL (vendored) are linked statically.

FROM rust:1.95-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake perl make \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src

# Dependency layer: build with a stub main so source edits don't rebuild deps.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src target/release/deps/smb_kafka_watcher* target/release/smb-kafka-watcher*

COPY src ./src
RUN cargo build --release --locked && strip target/release/smb-kafka-watcher

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/smb-kafka-watcher /usr/local/bin/smb-kafka-watcher
ENV LOG_FORMAT=json HTTP_PORT=8080
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/usr/local/bin/smb-kafka-watcher"]
