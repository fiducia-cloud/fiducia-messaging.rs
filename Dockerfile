# syntax=docker/dockerfile:1
FROM rust:1-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && apt-get clean
WORKDIR /build
COPY . .
RUN cargo build --locked --release --bin fiducia-relay --features postgres,nats \
    && strip target/release/fiducia-relay

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build --chown=65532:65532 /build/target/release/fiducia-relay /usr/local/bin/fiducia-relay
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-relay"]
