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

    use chrono::Utc;
    use fiducia_messaging::db;
    use fiducia_messaging::outbox::Relay;
    use fiducia_messaging::publisher::NatsPublisher;

    let db_url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL must be set (e.g. postgres://user:pass@host/db)")?;
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let batch_size: i64 = std::env::var("RELAY_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pool = sqlx::PgPool::connect(&db_url).await?;
    db::apply_schema(&pool).await?;

    let client = async_nats::connect(&nats_url).await?;
    let js = async_nats::jetstream::new(client);
    let publisher = NatsPublisher::new(js);
    let relay = Relay::new(&publisher);

    eprintln!("fiducia-relay: draining message_outbox -> {nats_url}");
    loop {
        let batch = db::claim_pending_outbox(&pool, batch_size).await?;
        if batch.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        let outcome = relay.drain(&batch).await;
        for id in outcome.published {
            db::mark_published(&pool, id, Utc::now()).await?;
        }
        for (id, err) in outcome.failed {
            eprintln!("fiducia-relay: publish {id} failed: {err}");
            // Leave it pending to retry; a real deployment would cap attempts
            // here and call db::mark_failed once exhausted.
        }
    }
}

#[cfg(not(all(feature = "postgres", feature = "nats")))]
fn main() {
    eprintln!("fiducia-relay is a thin outbox->JetStream drain loop.");
    eprintln!("Rebuild with:  cargo run --bin fiducia-relay --features postgres,nats");
    eprintln!("The library (envelope, outbox/inbox, subjects) is the product.");
}
