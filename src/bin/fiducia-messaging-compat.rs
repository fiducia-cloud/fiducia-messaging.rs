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
    let nats = async_nats::connect(nats_url).await?;
    tracing::info!("fiducia compatibility outbox publisher started");
    OutboxPublisher::new(pool, nats)
        .run(Duration::from_millis(250))
        .await?;
    Ok(())
}
