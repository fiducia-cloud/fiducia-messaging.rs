# syntax=docker/dockerfile:1
FROM rust:1-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && apt-get clean
WORKDIR /build
COPY . .
RUN cargo build --locked --release --bins --features compat-service \
    && strip target/release/fiducia-relay \
    && strip target/release/fiducia-messaging-compat

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build --chown=65532:65532 /build/target/release/fiducia-relay /usr/local/bin/fiducia-relay
COPY --from=build --chown=65532:65532 /build/target/release/fiducia-messaging-compat /usr/local/bin/fiducia-messaging-compat
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-relay"]
