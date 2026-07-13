//! Postgres-backed outbox/inbox repository — the `postgres` feature.
//!
//! Runtime-checked queries only (`sqlx::query`, not the `query!` macros), so the
//! crate builds with no `DATABASE_URL` and no live database. The schema lives in
//! `migrations/0001_fiducia_messaging.sql` and is embedded via [`SCHEMA_SQL`];
//! apply it on boot with [`apply_schema`] (or run the whole `migrations/` dir
//! with `sqlx::migrate!`). These functions are the durable counterparts to the
//! pure logic in [`crate::outbox`].

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::error::MessagingError;
use crate::outbox::{OutboxRecord, OutboxStatus};
use crate::publisher::Publisher;

/// The messaging schema DDL, embedded from the first migration. Idempotent, so
/// [`apply_schema`] can run it directly and `sqlx::migrate!` can run the same
/// file as a tracked migration.
pub const SCHEMA_SQL: &str = include_str!("../migrations/0001_fiducia_messaging.sql");

/// Apply the messaging schema. Idempotent (`CREATE TABLE IF NOT EXISTS`), so it
/// is safe to call on every process start.
pub async fn apply_schema(pool: &PgPool) -> Result<(), MessagingError> {
    sqlx::raw_sql(SCHEMA_SQL)
        .execute(pool)
        .await
        .map_err(MessagingError::database)?;
    Ok(())
}

/// Insert a staged outbox row using a pool connection. Convenience for callers
/// with no open transaction; a repeated `dedup_id` is ignored (`ON CONFLICT`),
/// so enqueue is itself idempotent.
///
// RECONCILE: this pool-based enqueue runs in its OWN connection, so it is *not*
// atomic with the caller's domain change — which defeats the whole point of a
// transactional outbox. Codex's `Outbox::enqueue` took a `&mut Transaction`;
// that behaviour is preserved as [`enqueue_outbox_tx`] below, which is the
// correct entry point for the outbox pattern. This pool variant is kept for
// backwards compatibility and one-off/manual enqueues.
pub async fn enqueue_outbox(pool: &PgPool, rec: &OutboxRecord) -> Result<(), MessagingError> {
    sqlx::query(OUTBOX_INSERT_SQL)
        .bind(rec.id)
        .bind(&rec.subject)
        .bind(&rec.dedup_id)
        .bind(&rec.payload)
        .bind(rec.status.as_str())
        .bind(rec.attempts as i32)
        .bind(rec.created_at)
        .execute(pool)
        .await
        .map_err(MessagingError::database)?;
    Ok(())
}

/// Insert a staged outbox row **inside the caller's transaction** — the correct
/// transactional-outbox usage (folded from codex's `Outbox::enqueue`): the row
/// commits atomically with the domain change it accompanies, so a message is
/// never lost nor sent for a rolled-back change. A repeated `dedup_id` is
/// ignored (`ON CONFLICT`), keeping enqueue idempotent.
pub async fn enqueue_outbox_tx(
    tx: &mut Transaction<'_, Postgres>,
    rec: &OutboxRecord,
) -> Result<(), MessagingError> {
    // Preserve codex's non-empty-subject guard.
    if rec.subject.trim().is_empty() {
        return Err(MessagingError::database("outbox subject must be non-empty"));
    }
    sqlx::query(OUTBOX_INSERT_SQL)
        .bind(rec.id)
        .bind(&rec.subject)
        .bind(&rec.dedup_id)
        .bind(&rec.payload)
        .bind(rec.status.as_str())
        .bind(rec.attempts as i32)
        .bind(rec.created_at)
        .execute(&mut **tx)
        .await
        .map_err(MessagingError::database)?;
    Ok(())
}

const OUTBOX_INSERT_SQL: &str = "INSERT INTO message_outbox
        (id, subject, dedup_id, payload, status, attempts, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (dedup_id) DO NOTHING";

/// Claim up to `limit` pending rows (oldest first), bumping their attempt count.
///
/// Uses `FOR UPDATE SKIP LOCKED` so several relay workers can drain the outbox
/// concurrently without handing the same row to two of them.
pub async fn claim_pending_outbox(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<OutboxRecord>, MessagingError> {
    let rows = sqlx::query(
        "UPDATE message_outbox
            SET attempts = attempts + 1
          WHERE id IN (
              SELECT id FROM message_outbox
               WHERE status = 'pending'
                 AND available_at <= now()
               ORDER BY created_at
               LIMIT $1
               FOR UPDATE SKIP LOCKED
          )
        RETURNING id, subject, dedup_id, payload, status, attempts, created_at",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(MessagingError::database)?;

    rows.iter().map(row_to_outbox).collect()
}

/// Mark a row published and stamp `published_at`.
pub async fn mark_published(
    pool: &PgPool,
    id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), MessagingError> {
    sqlx::query("UPDATE message_outbox SET status = 'published', published_at = $2 WHERE id = $1")
        .bind(id)
        .bind(at)
        .execute(pool)
        .await
        .map_err(MessagingError::database)?;
    Ok(())
}

/// Mark a row failed (retries exhausted); leaves it for operator attention.
pub async fn mark_failed(pool: &PgPool, id: Uuid) -> Result<(), MessagingError> {
    sqlx::query("UPDATE message_outbox SET status = 'failed' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .map_err(MessagingError::database)?;
    Ok(())
}

/// Try to record an incoming message for consumer dedup. Returns `true` the
/// first time this `message_id` is seen and `false` for a duplicate delivery, so
/// the caller runs the external effect at most once.
pub async fn inbox_try_insert(
    pool: &PgPool,
    message_id: Uuid,
    idempotency_key: &str,
    received_at: DateTime<Utc>,
) -> Result<bool, MessagingError> {
    let result = sqlx::query(
        "INSERT INTO message_inbox (message_id, idempotency_key, received_at)
         VALUES ($1, $2, $3)
         ON CONFLICT DO NOTHING",
    )
    .bind(message_id)
    .bind(idempotency_key)
    .bind(received_at)
    .execute(pool)
    .await
    .map_err(MessagingError::database)?;
    Ok(result.rows_affected() == 1)
}

/// Mark an inbox row processed once its effect completes.
pub async fn inbox_mark_processed(
    pool: &PgPool,
    message_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), MessagingError> {
    sqlx::query("UPDATE message_inbox SET processed_at = $2 WHERE message_id = $1")
        .bind(message_id)
        .bind(at)
        .execute(pool)
        .await
        .map_err(MessagingError::database)?;
    Ok(())
}

/// A DB-coupled outbox drainer: claims a bounded batch of *due* pending rows
/// with `FOR UPDATE SKIP LOCKED`, publishes each through a [`Publisher`], and
/// records the outcome with retry metadata + exponential backoff — all in one
/// transaction.
///
// RECONCILE: this is the merge of the two publisher designs.
//   * MINE — `outbox::Relay`: pure and transport-agnostic (batch in, outcome
//     out, no DB), kept as-is for callers that want to own the DB dance.
//   * CODEX — `outbox::OutboxPublisher`: DB-coupled, SKIP LOCKED + SQL
//     exponential backoff + `last_error` + flush-before-mark. That design is
//     preserved here, but adapted to publish through the crate's `Publisher`
//     trait (so it dedups via `NatsPublisher` / is testable via
//     `RecordingPublisher`) and to drive MY richer `message_outbox` schema
//     (jsonb `payload` + `dedup_id` + `status`) instead of codex's bytea one.
// NATS-flush-before-mark: `NatsPublisher::publish` awaits the JetStream publish
// ack (durability) before returning, so a row is marked `published` only after
// the broker has durably stored it — the same guarantee codex got from
// `nats.flush()`.
pub struct OutboxPublisher<'a> {
    pool: &'a PgPool,
    publisher: &'a dyn Publisher,
    batch_size: i64,
    max_attempts: i32,
}

impl<'a> OutboxPublisher<'a> {
    /// Build a publisher over a pool and a [`Publisher`]. Defaults: batch of
    /// 100, up to 8 attempts before a row is parked as `failed`.
    pub fn new(pool: &'a PgPool, publisher: &'a dyn Publisher) -> Self {
        OutboxPublisher {
            pool,
            publisher,
            batch_size: 100,
            max_attempts: 8,
        }
    }

    /// Override the max rows claimed per batch.
    pub fn with_batch_size(mut self, batch_size: i64) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Override how many failed attempts a row tolerates before it is marked
    /// `failed` (parked for operator attention).
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    /// Claim and publish one batch. Returns how many rows were published.
    ///
    /// Preserves codex's semantics: on the first publish failure it records the
    /// error + backoff and stops the batch (avoids hammering a down broker),
    /// committing the progress made so far.
    pub async fn publish_batch(&self) -> Result<u64, MessagingError> {
        let mut tx = self.pool.begin().await.map_err(MessagingError::database)?;

        let rows: Vec<(Uuid, String, String, serde_json::Value, i32)> = sqlx::query_as(
            "SELECT id, subject, dedup_id, payload, attempts
               FROM message_outbox
              WHERE status = 'pending'
                AND available_at <= now()
              ORDER BY created_at
              FOR UPDATE SKIP LOCKED
              LIMIT $1",
        )
        .bind(self.batch_size)
        .fetch_all(&mut *tx)
        .await
        .map_err(MessagingError::database)?;

        let mut published: u64 = 0;
        for (id, subject, dedup_id, payload, attempts) in rows {
            let bytes = serde_json::to_vec(&payload)?;
            match self.publisher.publish(&subject, &dedup_id, &bytes).await {
                Ok(()) => {
                    sqlx::query(
                        "UPDATE message_outbox
                            SET status = 'published',
                                published_at = now(),
                                attempts = attempts + 1,
                                last_error = NULL
                          WHERE id = $1",
                    )
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(MessagingError::database)?;
                    published += 1;
                }
                Err(error) => {
                    if attempts + 1 >= self.max_attempts {
                        // Retries exhausted -> park as failed for an operator.
                        sqlx::query(
                            "UPDATE message_outbox
                                SET status = 'failed',
                                    attempts = attempts + 1,
                                    last_error = $2
                              WHERE id = $1",
                        )
                        .bind(id)
                        .bind(error.to_string())
                        .execute(&mut *tx)
                        .await
                        .map_err(MessagingError::database)?;
                    } else {
                        // Exponential backoff, capped at 5 minutes (codex's SQL).
                        sqlx::query(
                            "UPDATE message_outbox
                                SET attempts = attempts + 1,
                                    last_error = $2,
                                    available_at = now()
                                        + least(interval '5 minutes',
                                                interval '1 second'
                                                    * power(2, least(attempts, 8)))
                              WHERE id = $1",
                        )
                        .bind(id)
                        .bind(error.to_string())
                        .execute(&mut *tx)
                        .await
                        .map_err(MessagingError::database)?;
                    }
                    break;
                }
            }
        }

        tx.commit().await.map_err(MessagingError::database)?;
        Ok(published)
    }

    /// Drain forever on a fixed interval. A batch failure is logged and the loop
    /// continues (folded from codex's `run`, using `eprintln!` to keep the crate
    /// free of a tracing dependency).
    pub async fn run(self, interval: Duration) -> Result<(), MessagingError> {
        let mut timer = tokio::time::interval(interval);
        loop {
            timer.tick().await;
            if let Err(error) = self.publish_batch().await {
                eprintln!("fiducia outbox publish batch failed: {error}");
            }
        }
    }
}

fn row_to_outbox(row: &sqlx::postgres::PgRow) -> Result<OutboxRecord, MessagingError> {
    let status_str: String = row.try_get("status").map_err(MessagingError::database)?;
    let attempts: i32 = row.try_get("attempts").map_err(MessagingError::database)?;
    Ok(OutboxRecord {
        id: row.try_get("id").map_err(MessagingError::database)?,
        subject: row.try_get("subject").map_err(MessagingError::database)?,
        dedup_id: row.try_get("dedup_id").map_err(MessagingError::database)?,
        payload: row.try_get("payload").map_err(MessagingError::database)?,
        created_at: row.try_get("created_at").map_err(MessagingError::database)?,
        status: OutboxStatus::from_str(&status_str).unwrap_or(OutboxStatus::Pending),
        attempts: attempts.max(0) as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // DB-free sanity check: the embedded schema matches what the repo queries
    // expect. A live-Postgres integration test is out of scope for `cargo test`.
    #[test]
    fn embedded_schema_defines_both_tables() {
        assert!(SCHEMA_SQL.contains("CREATE TABLE IF NOT EXISTS message_outbox"));
        assert!(SCHEMA_SQL.contains("CREATE TABLE IF NOT EXISTS message_inbox"));
        assert!(SCHEMA_SQL.contains("dedup_id"));
        assert!(SCHEMA_SQL.contains("idempotency_key"));
        // Merged-in: codex's backoff columns + per-consumer inbox.
        assert!(SCHEMA_SQL.contains("available_at"));
        assert!(SCHEMA_SQL.contains("last_error"));
        assert!(SCHEMA_SQL.contains("CREATE TABLE IF NOT EXISTS message_inbox_consumer"));
    }
}
