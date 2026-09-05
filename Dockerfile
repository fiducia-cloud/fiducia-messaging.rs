# syntax=docker/dockerfile:1
FROM rust:1.98.0-slim-bookworm@sha256:1469a27c125cb5a3aebfa4f4e4665d935b02fb72cc093b2c974b3d740e43f157 AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && apt-get clean
WORKDIR /build
COPY . .
RUN cargo build --locked --release --bin fiducia-relay --features postgres,nats,telemetry \
    && strip target/release/fiducia-relay

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=build --chown=65532:65532 /build/target/release/fiducia-relay /usr/local/bin/fiducia-relay
USER 65532:65532
# --- sops: this final stage has no shell (distroless/scratch), so runtime
# decryption cannot run inside the container. Inject secrets HOST-SIDE at
# `docker run` instead — never at build, never as --build-arg:
#     just env-docker-run prod <image>        # decrypts env/enc/prod.env.enc
#                                             # and passes --env-file, no plaintext on disk
# or render a platform secret from the same ciphertext. See env/README.md.
ENTRYPOINT ["/usr/local/bin/fiducia-relay"]
