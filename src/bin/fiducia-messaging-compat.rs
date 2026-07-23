use fiducia_messaging::transactional::OutboxPublisher;
use sea_orm::{ConnectOptions, Database};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Same telemetry contract as the integrated relay: with `--features
    // telemetry` the shared fleet crate installs JSON logs plus OTLP traces and
    // metrics; the guard stays bound for the whole of `main` because dropping it
    // shuts the exporters down.
    #[cfg(feature = "telemetry")]
    let _telemetry = fiducia_telemetry::init("fiducia-messaging-compat");

    // Fallback for a build without the feature — JSON logs only, so batch
    // failures still reach the log pipeline instead of raw stderr.
    #[cfg(not(feature = "telemetry"))]
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be configured")?;
    let nats_url = std::env::var("NATS_URL").map_err(|_| "NATS_URL must be configured")?;
    let mut options = ConnectOptions::new(database_url);
    options.max_connections(10).sqlx_logging(false);
    let pool = Database::connect(options).await?;
    // Schema is applied declaratively out-of-band (`migrations/` is the source
    // of truth) — no boot-time migrator here.
    // Same TLS/credentials policy as the integrated relay: non-loopback
    // endpoints require TLS unless explicitly opted out, NATS_CREDS_FILE keeps
    // credentials off the URL, and the URL is never logged.
    let nats = fiducia_messaging::connect::connect(&nats_url).await?;
    // The compat publisher now awaits JetStream acks (see `transactional`), so
    // canonical `fiducia.*` subjects need the stream in place with a dedup
    // window that satisfies the crate invariant — same fail-closed check as the
    // relay. Legacy non-`fiducia.*` subjects are outside this stream; their
    // publishes surface as per-row errors with backoff metadata instead of
    // being silently fire-and-forget.
    let js = async_nats::jetstream::new(nats.clone());
    let stream_config =
        fiducia_messaging::stream::config_from_env(fiducia_messaging::DEFAULT_CLAIM_TTL)?;
    fiducia_messaging::stream::ensure_stream(
        &js,
        stream_config,
        fiducia_messaging::DEFAULT_CLAIM_TTL,
    )
    .await?;
    tracing::info!("fiducia compatibility outbox publisher started");
    OutboxPublisher::new(pool, nats)
        .run(Duration::from_millis(250))
        .await?;
    Ok(())
}
