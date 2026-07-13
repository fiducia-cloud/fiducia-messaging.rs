# syntax=docker/dockerfile:1
FROM rust:1-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && apt-get clean
WORKDIR /build
COPY . .
RUN cargo build --locked --release --bin fiducia-messaging \
    && strip target/release/fiducia-messaging

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build --chown=65532:65532 /build/target/release/fiducia-messaging /usr/local/bin/fiducia-messaging
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-messaging"]
