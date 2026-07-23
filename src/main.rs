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

    // Telemetry. With `--features telemetry` the shared fleet crate owns the
    // subscriber: JSON logs *plus* OTLP traces and metrics when
    // OTEL_EXPORTER_OTLP_ENDPOINT is set (stdout-only when it is not). The relay
    // is a long-running drain loop, so outbox lag and dead-letter counts belong
    // on the same collector path as the rest of the fleet rather than only in
    // log lines. The guard must stay bound for the whole of `main` — dropping it
    // flushes and shuts the exporters down, so `let _ =` here would export
    // nothing.
    #[cfg(feature = "telemetry")]
    let _telemetry = fiducia_telemetry::init("fiducia-relay");

    // Without the feature, keep the previous local JSON logs so the binary still
    // builds and behaves with just `postgres,nats`. Same log contract either way:
    // a parked (dead-lettered) outbox row or a batch failure reaches the log
    // pipeline instead of raw stderr.
    #[cfg(not(feature = "telemetry"))]
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL must be set (e.g. postgres://user:pass@host/db)")?;
    // No default: a relay silently falling back to anonymous plaintext
    // localhost is a fail-open misconfiguration, not a convenience.
    let nats_url = std::env::var("NATS_URL")
        .map_err(|_| "NATS_URL must be set; the relay does not default to localhost")?;
    let batch_size: i64 = std::env::var("RELAY_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let mut options = sea_orm::ConnectOptions::new(db_url);
    options.sqlx_logging(false);
    let pool = sea_orm::Database::connect(options).await?;
    // Schema is applied declaratively out-of-band (the tracked files in
    // `migrations/` are the source of truth) — no boot-time migrator here.
    // A caller that owns its own database can still run
    // `fiducia_messaging::db::apply_schema` explicitly.

    // TLS is policy, not luck: non-loopback endpoints require TLS unless
    // explicitly opted out, and NATS_CREDS_FILE keeps credentials off the URL
    // (see `connect`). The URL itself is never logged.
    let client = fiducia_messaging::connect::connect(&nats_url).await?;
    let js = async_nats::jetstream::new(client);
    // Fail closed before the first publish: ensure the `fiducia.*` stream
    // exists with an explicit config and that its duplicate_window covers
    // `min_duplicate_window(claim_ttl)` — JetStream's 2-minute default window
    // silently double-delivers a crash-window re-publish otherwise.
    let stream_config =
        fiducia_messaging::stream::config_from_env(fiducia_messaging::DEFAULT_CLAIM_TTL)?;
    fiducia_messaging::stream::ensure_stream(
        &js,
        stream_config,
        fiducia_messaging::DEFAULT_CLAIM_TTL,
    )
    .await?;
    let publisher = NatsPublisher::new(js);

    // The DB-coupled drainer: durable expiring claim leases, exponential
    // backoff, retry metadata, and JetStream-ack-before-owner-conditioned-mark
    // (via `NatsPublisher`). The pure `outbox::Relay` remains available for
    // callers that own the DB dance.
    let outbox = OutboxPublisher::new(&pool, &publisher).with_batch_size(batch_size);

    // Opt-in retention: RELAY_RETENTION_HOURS purges published outbox rows and
    // processed inbox claims older than that age, hourly. Off by default —
    // deleting a processed inbox claim gives up dedup for a very-late replay of
    // that message, so the operator picks the horizon.
    // `hours * 3600` must not wrap: a wrapped product becomes a TINY retention
    // age, and the hourly purge would then delete recently-published outbox
    // rows and freshly-processed inbox claims — giving up dedup for redeliveries
    // still in flight. A value that cannot be a duration is a misconfiguration.
    if let Some(retention_hours) = std::env::var("RELAY_RETENTION_HOURS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|hours| *hours > 0 && hours.checked_mul(3600).is_some())
    {
        fn log_purge(table: &str, result: Result<u64, fiducia_messaging::MessagingError>) {
            match result {
                Ok(0) => {}
                Ok(rows) => tracing::info!(table, rows, "retention: purged terminal rows"),
                Err(error) => tracing::warn!(table, %error, "retention: purge failed"),
            }
        }
        let retention = Duration::from_secs(
            retention_hours
                .checked_mul(3600)
                .expect("filtered above: retention_hours * 3600 fits u64"),
        );
        let purge_pool = pool.clone();
        tracing::info!(
            retention_hours,
            "fiducia-relay: hourly retention purge enabled"
        );
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(3600));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                timer.tick().await;
                use fiducia_messaging::db;
                log_purge(
                    "message_outbox",
                    db::purge_published_outbox(&purge_pool, retention).await,
                );
                log_purge(
                    "message_inbox",
                    db::purge_processed_inbox(&purge_pool, retention).await,
                );
                log_purge(
                    "message_inbox_consumer",
                    db::purge_processed_consumer_inbox(&purge_pool, retention).await,
                );
            }
        });
    }

    // Drain until SIGTERM/SIGINT, finishing the in-flight batch so its claimed
    // rows are marked or released — otherwise every rolling restart strands a
    // batch until the claim TTL expires.
    let shutdown = async {
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };

    tracing::info!("fiducia-relay: draining message_outbox to the configured NATS endpoint");
    outbox
        .run_until(Duration::from_millis(500), shutdown)
        .await?;
    tracing::info!("fiducia-relay: stopped cleanly");
    Ok(())
}

#[cfg(not(all(feature = "postgres", feature = "nats")))]
fn main() {
    eprintln!("fiducia-relay is a thin outbox->JetStream drain loop.");
    eprintln!("Rebuild with:  cargo run --bin fiducia-relay --features postgres,nats");
    eprintln!("The library (envelope, outbox/inbox, subjects) is the product.");
}
