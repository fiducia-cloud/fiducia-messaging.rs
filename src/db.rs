//! Postgres-backed outbox/inbox repository — the `postgres` feature.
//!
//! Runtime-checked queries only (`sqlx::query`, not the `query!` macros), so the
//! crate builds with no `DATABASE_URL` and no live database. The schema lives in
//! `migrations/` and is embedded via [`SCHEMA_SQL`] / [`HARDENING_SCHEMA_SQL`];
//! apply it on boot with [`apply_schema`] (or run the whole migrations directory
//! with `sqlx::migrate!`). These functions are the durable counterparts to the
//! pure logic in [`crate::outbox`].

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::error::MessagingError;
use crate::outbox::{validate_for_publish, OutboxRecord, OutboxStatus};
use crate::publisher::Publisher;

/// Validate a record before it is staged: canonical subject (no wildcard /
/// injected tokens) and serialized payload within
/// [`MAX_MESSAGE_BYTES`](crate::outbox::MAX_MESSAGE_BYTES).
fn validate_outbox_record(rec: &OutboxRecord) -> Result<(), MessagingError> {
    let payload_len = serde_json::to_vec(&rec.payload)?.len();
    validate_for_publish(&rec.subject, payload_len)
}

/// The messaging schema DDL, embedded from the first migration. Idempotent, so
/// [`apply_schema`] can run it directly and `sqlx::migrate!` can run the same
/// file as a tracked migration.
pub const SCHEMA_SQL: &str = include_str!("../migrations/0001_fiducia_messaging.sql");
/// Forward migration adding tenant-scoped idempotency and durable claim leases.
pub const HARDENING_SCHEMA_SQL: &str =
    include_str!("../migrations/0002_tenant_dedup_and_claim_leases.sql");

/// Apply the messaging schema. Idempotent (`CREATE TABLE IF NOT EXISTS`), so it
/// is safe to call on every process start.
pub async fn apply_schema(pool: &PgPool) -> Result<(), MessagingError> {
    sqlx::raw_sql(SCHEMA_SQL)
        .execute(pool)
        .await
        .map_err(MessagingError::database)?;
    sqlx::raw_sql(HARDENING_SCHEMA_SQL)
        .execute(pool)
        .await
        .map_err(MessagingError::database)?;
    Ok(())
}

/// Insert a staged outbox row using a pool connection. Convenience for callers
/// with no open transaction; a repeated tenant-scoped business key is ignored
/// (`ON CONFLICT`), so enqueue is itself idempotent. The subject must be a
/// canonical routing class and the payload within size limits (see
/// [`validate_for_publish`]).
///
// RECONCILE: this pool-based enqueue runs in its OWN connection, so it is *not*
// atomic with the caller's domain change — which defeats the whole point of a
// transactional outbox. Codex's `Outbox::enqueue` took a `&mut Transaction`;
// that behaviour is preserved as [`enqueue_outbox_tx`] below, which is the
// correct entry point for the outbox pattern. This pool variant is kept for
// backwards compatibility and one-off/manual enqueues.
pub async fn enqueue_outbox(pool: &PgPool, rec: &OutboxRecord) -> Result<(), MessagingError> {
    validate_outbox_record(rec)?;
    sqlx::query(OUTBOX_INSERT_SQL)
        .bind(rec.id)
        .bind(&rec.subject)
        .bind(rec.tenant_id)
        .bind(&rec.idempotency_key)
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
/// never lost nor sent for a rolled-back change. A repeated tenant-scoped
/// business key is ignored (`ON CONFLICT`), keeping enqueue idempotent.
pub async fn enqueue_outbox_tx(
    tx: &mut Transaction<'_, Postgres>,
    rec: &OutboxRecord,
) -> Result<(), MessagingError> {
    // Strengthens codex's non-empty-subject guard: the subject must be a
    // canonical routing class (no wildcards / injected tokens) and the payload
    // within MAX_MESSAGE_BYTES, so poison rows never enter the outbox.
    validate_outbox_record(rec)?;
    sqlx::query(OUTBOX_INSERT_SQL)
        .bind(rec.id)
        .bind(&rec.subject)
        .bind(rec.tenant_id)
        .bind(&rec.idempotency_key)
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
        (id, subject, tenant_id, idempotency_key, dedup_id, payload, status, attempts, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     ON CONFLICT DO NOTHING";

const CLAIM_PENDING_SQL: &str = "WITH candidates AS (
        SELECT id
          FROM message_outbox
         WHERE status = 'pending'
           AND available_at <= now()
           AND (claim_expires_at IS NULL OR claim_expires_at <= now())
         ORDER BY created_at
         LIMIT $1
         FOR UPDATE SKIP LOCKED
    )
    UPDATE message_outbox AS outbox
       SET attempts = outbox.attempts + 1,
           claim_owner = $2,
           claim_expires_at = now() + ($3 * interval '1 millisecond')
      FROM candidates
     WHERE outbox.id = candidates.id
    RETURNING outbox.id, outbox.subject, outbox.tenant_id,
              outbox.idempotency_key, outbox.dedup_id, outbox.payload,
              outbox.status, outbox.attempts, outbox.created_at";

/// Atomically lease up to `limit` due rows to `claim_owner` and commit that
/// ownership before returning. Other workers skip an active claim; after
/// `claim_ttl` it becomes reclaimable if this worker crashed.
///
/// The returned records have already had their attempt count incremented.
pub async fn claim_pending_outbox(
    pool: &PgPool,
    claim_owner: Uuid,
    claim_ttl: Duration,
    limit: i64,
) -> Result<Vec<OutboxRecord>, MessagingError> {
    if limit <= 0 {
        return Err(MessagingError::database(
            "outbox claim limit must be positive",
        ));
    }
    let claim_ttl_ms = i64::try_from(claim_ttl.as_millis())
        .ok()
        .filter(|millis| *millis > 0)
        .ok_or_else(|| MessagingError::database("outbox claim TTL must be positive and finite"))?;
    let rows = sqlx::query(CLAIM_PENDING_SQL)
        .bind(limit)
        .bind(claim_owner)
        .bind(claim_ttl_ms)
        .fetch_all(pool)
        .await
        .map_err(MessagingError::database)?;

    let mut records = rows
        .iter()
        .map(row_to_outbox)
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_by_key(|record| record.created_at);
    Ok(records)
}

/// Mark a row published only if `claim_owner` still owns it. Returns `false`
/// when the lease expired and another worker reclaimed the row.
pub async fn mark_published(
    pool: &PgPool,
    id: Uuid,
    claim_owner: Uuid,
    at: DateTime<Utc>,
) -> Result<bool, MessagingError> {
    let result = sqlx::query(
        "UPDATE message_outbox
            SET status = 'published', published_at = $3,
                last_error = NULL, claim_owner = NULL, claim_expires_at = NULL
          WHERE id = $1 AND claim_owner = $2",
    )
    .bind(id)
    .bind(claim_owner)
    .bind(at)
    .execute(pool)
    .await
    .map_err(MessagingError::database)?;
    Ok(result.rows_affected() == 1)
}

/// Mark an owned row failed (retries exhausted). Returns `false` if ownership
/// has already moved to another relay.
pub async fn mark_failed(
    pool: &PgPool,
    id: Uuid,
    claim_owner: Uuid,
    error: &str,
) -> Result<bool, MessagingError> {
    let result = sqlx::query(
        "UPDATE message_outbox
            SET status = 'failed', last_error = $3,
                claim_owner = NULL, claim_expires_at = NULL
          WHERE id = $1 AND claim_owner = $2",
    )
    .bind(id)
    .bind(claim_owner)
    .bind(error)
    .execute(pool)
    .await
    .map_err(MessagingError::database)?;
    Ok(result.rows_affected() == 1)
}

/// Release one owned claim after a transient publish failure, record the error,
/// and defer the next attempt with exponential backoff. Returns `false` if the
/// row was already reclaimed by another owner.
pub async fn reschedule_publish(
    pool: &PgPool,
    id: Uuid,
    claim_owner: Uuid,
    error: &str,
) -> Result<bool, MessagingError> {
    let result = sqlx::query(
        "UPDATE message_outbox
            SET last_error = $3,
                available_at = now()
                    + least(interval '5 minutes',
                            interval '1 second'
                                * power(2, least(greatest(attempts - 1, 0), 8))),
                claim_owner = NULL,
                claim_expires_at = NULL
          WHERE id = $1 AND claim_owner = $2 AND status = 'pending'",
    )
    .bind(id)
    .bind(claim_owner)
    .bind(error)
    .execute(pool)
    .await
    .map_err(MessagingError::database)?;
    Ok(result.rows_affected() == 1)
}

/// Release every still-pending row owned by a batch claim. Used when a relay
/// stops a batch after the first broker failure so untouched rows are
/// immediately available to other workers rather than waiting for lease expiry.
pub async fn release_outbox_claims(
    pool: &PgPool,
    claim_owner: Uuid,
) -> Result<u64, MessagingError> {
    let result = sqlx::query(
        "UPDATE message_outbox
            SET claim_owner = NULL, claim_expires_at = NULL
          WHERE status = 'pending' AND claim_owner = $1",
    )
    .bind(claim_owner)
    .execute(pool)
    .await
    .map_err(MessagingError::database)?;
    Ok(result.rows_affected())
}

/// Try to record an incoming message for consumer dedup. Returns `true` the
/// first time this `message_id` is seen and `false` for a duplicate delivery, so
/// the caller runs the external effect at most once.
///
// RECONCILE / HAZARD: this pool-based claim commits on its OWN connection, so it
// is *not* atomic with the effect it guards. If the process crashes after this
// insert commits but before the effect runs, the redelivery loses the insert
// (`false`) and the effect is **skipped forever** — at-most-once with silent
// effect loss (`processed_at` is never consulted here). Prefer the tx-scoped
// [`crate::inbox::Inbox`] (`PgInbox`): its `begin`/`mark_processed` run inside
// the consumer's OWN transaction, so the effect and the dedup claim commit or
// roll back together. Use this pool variant only when the effect is itself
// idempotent/fenced downstream and effect-loss-on-crash is acceptable.
pub async fn inbox_try_insert(
    pool: &PgPool,
    message_id: Uuid,
    idempotency_key: &str,
    received_at: DateTime<Utc>,
) -> Result<bool, MessagingError> {
    inbox_try_insert_scoped(pool, None, message_id, idempotency_key, received_at).await
}

/// Tenant-scoped form of [`inbox_try_insert`]. The same business key may be
/// consumed independently in different tenants, while remaining unique inside
/// each tenant (or inside the explicit global `None` namespace).
pub async fn inbox_try_insert_scoped(
    pool: &PgPool,
    tenant_id: Option<Uuid>,
    message_id: Uuid,
    idempotency_key: &str,
    received_at: DateTime<Utc>,
) -> Result<bool, MessagingError> {
    let result = sqlx::query(
        "INSERT INTO message_inbox (message_id, tenant_id, idempotency_key, received_at)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING",
    )
    .bind(message_id)
    .bind(tenant_id)
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

/// A DB-coupled outbox drainer. It atomically commits an expiring owner lease on
/// a bounded batch, releases the database connection, publishes through a
/// [`Publisher`], then conditionally records each outcome by owner. This avoids
/// holding a database transaction open across network I/O while preventing two
/// live relays from publishing the same pending row.
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
    claim_ttl: Duration,
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
            claim_ttl: crate::outbox::DEFAULT_CLAIM_TTL,
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

    /// Override the durable claim lease. It must cover the expected worst-case
    /// time to publish a whole batch; expired claims are deliberately
    /// reclaimable after a worker crash.
    ///
    /// The JetStream stream's `duplicate_window` MUST be at least
    /// [`min_duplicate_window(claim_ttl)`](crate::outbox::min_duplicate_window):
    /// a shorter window lets a crash-window re-publish be stored as a *new*
    /// message and double-delivered. This crate cannot set that window (it is
    /// broker configuration); the deployment must.
    pub fn with_claim_ttl(mut self, claim_ttl: Duration) -> Self {
        self.claim_ttl = claim_ttl;
        self
    }

    /// Claim and publish one batch. Returns how many rows were published.
    ///
    /// Preserves codex's semantics: on the first publish failure it records the
    /// error + backoff and stops the batch (avoids hammering a down broker),
    /// releasing untouched claims immediately.
    pub async fn publish_batch(&self) -> Result<u64, MessagingError> {
        let claim_owner = Uuid::new_v4();
        let rows =
            claim_pending_outbox(self.pool, claim_owner, self.claim_ttl, self.batch_size).await?;

        let mut published: u64 = 0;
        for record in rows {
            let bytes = match serde_json::to_vec(&record.payload) {
                Ok(bytes) => bytes,
                Err(error) => {
                    // A payload that cannot re-serialize is a deterministic row
                    // defect exactly like a malformed subject: retrying cannot
                    // succeed. Park it (with the loud dead-letter log below)
                    // instead of burning all attempts on backoff and stopping
                    // the whole batch for one poison row.
                    self.park_failed(&record, claim_owner, &error.to_string())
                        .await?;
                    continue;
                }
            };
            if let Err(error) = validate_for_publish(&record.subject, bytes.len()) {
                // Deterministic row defect (malformed/injected subject or an
                // oversize payload, e.g. staged before this guard existed):
                // retrying cannot succeed, so park the row for operator
                // attention instead of burning attempts and blocking the batch.
                self.park_failed(&record, claim_owner, &error.to_string())
                    .await?;
                continue;
            }
            match self
                .publisher
                .publish(&record.subject, &record.dedup_id, &bytes)
                .await
            {
                Ok(()) => {
                    if mark_published(self.pool, record.id, claim_owner, Utc::now()).await? {
                        published += 1;
                    }
                }
                Err(error) => {
                    self.record_failure(&record, claim_owner, &error.to_string())
                        .await?;
                    release_outbox_claims(self.pool, claim_owner).await?;
                    break;
                }
            }
        }
        Ok(published)
    }

    async fn record_failure(
        &self,
        record: &OutboxRecord,
        claim_owner: Uuid,
        error: &str,
    ) -> Result<(), MessagingError> {
        let attempts = i32::try_from(record.attempts).unwrap_or(i32::MAX);
        if attempts >= self.max_attempts {
            self.park_failed(record, claim_owner, error).await?;
        } else {
            tracing::warn!(
                id = %record.id,
                subject = %record.subject,
                attempts = record.attempts,
                error,
                "outbox: publish failed; rescheduled with backoff"
            );
            reschedule_publish(self.pool, record.id, claim_owner, error).await?;
        }
        Ok(())
    }

    /// Park a row as `failed` — the outbox's dead letter. This is the moment a
    /// durable domain event permanently stops flowing to the bus, so it must be
    /// loud: without this log line the only trace is a row an operator would
    /// have to find with `SELECT ... WHERE status = 'failed'`.
    async fn park_failed(
        &self,
        record: &OutboxRecord,
        claim_owner: Uuid,
        error: &str,
    ) -> Result<(), MessagingError> {
        tracing::error!(
            id = %record.id,
            subject = %record.subject,
            attempts = record.attempts,
            error,
            "outbox: message parked as failed (dead-lettered); it will NOT be \
             retried — inspect message_outbox WHERE status = 'failed'"
        );
        mark_failed(self.pool, record.id, claim_owner, error).await?;
        Ok(())
    }

    /// Drain forever on a fixed interval. A batch failure is logged and the loop
    /// continues.
    pub async fn run(self, interval: Duration) -> Result<(), MessagingError> {
        let mut timer = tokio::time::interval(interval);
        loop {
            timer.tick().await;
            if let Err(error) = self.publish_batch().await {
                tracing::error!(%error, "outbox: publish batch failed; retrying on the next interval");
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
        tenant_id: row.try_get("tenant_id").map_err(MessagingError::database)?,
        idempotency_key: row
            .try_get("idempotency_key")
            .map_err(MessagingError::database)?,
        dedup_id: row.try_get("dedup_id").map_err(MessagingError::database)?,
        payload: row.try_get("payload").map_err(MessagingError::database)?,
        created_at: row
            .try_get("created_at")
            .map_err(MessagingError::database)?,
        status: OutboxStatus::from_str(&status_str).unwrap_or_else(|| {
            // An unrecognized status can only mean out-of-band schema drift or a
            // manual edit; treating it as Pending re-enters the row into the
            // drain loop, which is the safe direction — but never silently.
            tracing::warn!(status = %status_str, "outbox: unknown row status; treating as pending");
            OutboxStatus::Pending
        }),
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
        assert!(SCHEMA_SQL.contains("CREATE TABLE IF NOT EXISTS message_outbox_compat"));
        assert!(HARDENING_SCHEMA_SQL.contains("idempotency_key"));
        assert!(HARDENING_SCHEMA_SQL.contains("claim_owner"));
        assert!(HARDENING_SCHEMA_SQL.contains("claim_expires_at"));
        assert!(HARDENING_SCHEMA_SQL.contains("message_outbox_tenant_idempotency_uq"));
        assert!(HARDENING_SCHEMA_SQL.contains("message_inbox_tenant_idempotency_uq"));
        assert!(CLAIM_PENDING_SQL.contains("claim_expires_at <= now()"));
        assert!(CLAIM_PENDING_SQL.contains("claim_owner = $2"));
    }
}
