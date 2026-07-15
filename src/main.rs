//! `fiducia-relay` — a thin drain loop that moves the transactional outbox to
//! NATS JetStream. It is intentionally minimal; the **library** is the product.
//!
//! Built only with `--features postgres,nats` (it needs both a DB to read and a
//! bus to write). Without them, `main` prints a usage note so the crate still
//! builds a binary in the default, dependency-free configuration.

#[cfg(all(feature = "postgres", feature = "nats"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Duration;

    use fiducia_messaging::publisher::NatsPublisher;
    use fiducia_messaging::OutboxPublisher;

    // JSON structured logs, level from RUST_LOG (default info) — the same log
    // contract as the rest of the fleet, so a parked (dead-lettered) outbox row
    // or a batch failure is visible to the log pipeline, not lost on raw stderr.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL must be set (e.g. postgres://user:pass@host/db)")?;
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let batch_size: i64 = std::env::var("RELAY_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pool = sqlx::PgPool::connect(&db_url).await?;
    // Adopt the migrations dir: apply every tracked migration in `migrations/`.
    sqlx::migrate!("./migrations").run(&pool).await?;

    let client = async_nats::connect(&nats_url).await?;
    let js = async_nats::jetstream::new(client);
    let publisher = NatsPublisher::new(js);

    // The DB-coupled drainer: durable expiring claim leases, exponential
    // backoff, retry metadata, and JetStream-ack-before-owner-conditioned-mark
    // (via `NatsPublisher`). The pure `outbox::Relay` remains available for
    // callers that own the DB dance.
    let outbox = OutboxPublisher::new(&pool, &publisher).with_batch_size(batch_size);

    tracing::info!("fiducia-relay: draining message_outbox to the configured NATS endpoint");
    outbox.run(Duration::from_millis(500)).await?;
    Ok(())
}

#[cfg(not(all(feature = "postgres", feature = "nats")))]
fn main() {
    eprintln!("fiducia-relay is a thin outbox->JetStream drain loop.");
    eprintln!("Rebuild with:  cargo run --bin fiducia-relay --features postgres,nats");
    eprintln!("The library (envelope, outbox/inbox, subjects) is the product.");
}
