# syntax=docker/dockerfile:1
FROM rust:1.97.1-slim-bookworm@sha256:158b745f1b82dbeec7ea06e6b1617d6b005723bb66e6141cd2ddfee40d079ec3 AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && apt-get clean
WORKDIR /build
COPY . .
RUN cargo build --locked --release --bin fiducia-relay --features postgres,nats,telemetry \
    && strip target/release/fiducia-relay

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:adcd20c7b4c988b73cbfbddb26d2eee574571e6d7c9ffea29b3821e0690efb77
COPY --from=build --chown=65532:65532 /build/target/release/fiducia-relay /usr/local/bin/fiducia-relay
USER 65532:65532
# --- sops: this final stage has no shell (distroless/scratch), so runtime
# decryption cannot run inside the container. Inject secrets HOST-SIDE at
# `docker run` instead — never at build, never as --build-arg:
#     just env-docker-run prod <image>        # decrypts env/enc/prod.env.enc
#                                             # and passes --env-file, no plaintext on disk
# or render a platform secret from the same ciphertext. See env/README.md.
ENTRYPOINT ["/usr/local/bin/fiducia-relay"]
