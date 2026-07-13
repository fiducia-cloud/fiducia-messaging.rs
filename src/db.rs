//! Postgres-backed outbox/inbox repository — the `postgres` feature.
//!
//! Runtime-checked queries only (`sqlx::query`, not the `query!` macros), so the
//! crate builds with no `DATABASE_URL` and no live database. The schema lives in
//! `sql/messaging.sql` and is embedded via [`SCHEMA_SQL`]; apply it on boot with
//! [`apply_schema`]. These functions are the durable counterparts to the pure
//! logic in [`crate::outbox`].

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::MessagingError;
use crate::outbox::{OutboxRecord, OutboxStatus};

/// The messaging schema DDL, embedded from `sql/messaging.sql`.
pub const SCHEMA_SQL: &str = include_str!("../sql/messaging.sql");

/// Apply the messaging schema. Idempotent (`CREATE TABLE IF NOT EXISTS`), so it
/// is safe to call on every process start.
pub async fn apply_schema(pool: &PgPool) -> Result<(), MessagingError> {
    sqlx::raw_sql(SCHEMA_SQL)
        .execute(pool)
        .await
        .map_err(MessagingError::database)?;
    Ok(())
}

/// Insert a staged outbox row. Call inside the *same* transaction as the domain
/// change it accompanies. A repeated `dedup_id` is ignored (`ON CONFLICT`), so
/// enqueue is itself idempotent.
pub async fn enqueue_outbox(pool: &PgPool, rec: &OutboxRecord) -> Result<(), MessagingError> {
    sqlx::query(
        "INSERT INTO message_outbox
            (id, subject, dedup_id, payload, status, attempts, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (dedup_id) DO NOTHING",
    )
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
    }
}
