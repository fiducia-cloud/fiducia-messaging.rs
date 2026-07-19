# syntax=docker/dockerfile:1
FROM rust:1.97.0-slim-bookworm@sha256:cfbb0e0ef7a73e736386bfa346f1cb0503c6d162969dc9426fb37834f3f64c25 AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && apt-get clean
WORKDIR /build
COPY . .
RUN cargo build --locked --release --bin fiducia-relay --features postgres,nats,telemetry \
    && strip target/release/fiducia-relay

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:ce0d66bc0f64aae46e6a03add867b07f42cc7b8799c949c2e898057b7f75a151
COPY --from=build --chown=65532:65532 /build/target/release/fiducia-relay /usr/local/bin/fiducia-relay
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-relay"]
