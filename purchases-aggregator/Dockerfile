FROM rust:1.67 as builder
RUN USER=root cargo new --bin myapp
WORKDIR myapp
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
RUN cargo build --release
RUN rm src/*.rs

COPY ./src ./src
RUN touch ./src/main.rs
RUN cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && apt-get install && rm -rf /var/lib/apt/lists/*
COPY --from=builder /myapp/target/release/purchases-aggregator /usr/local/bin/purchases-aggregator
ENV RUST_BACKTRACE=1
ENV SCHEMA_REGISTRY_ADDRESS=localhost:8082
ENV KAFKA_BROKER_ADDRESS=localhost:9092
CMD ["purchases-aggregator"]
