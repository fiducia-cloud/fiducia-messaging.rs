use fiducia_messaging::transactional::OutboxPublisher;
use sea_orm::{ConnectOptions, Database};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Same JSON log contract as the fleet (and the integrated relay), so batch
    // failures reach the log pipeline instead of raw stderr.
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
