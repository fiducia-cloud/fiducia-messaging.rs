//! Postgres-backed outbox/inbox repository — the `postgres` feature.
//!
//! SeaORM throughout, per the fleet convention: the entity API
//! ([`crate::entity`]) for every query it can express, and raw SQL strictly via
//! [`sea_orm::Statement`] + [`FromQueryResult`] for the two it cannot (the
//! SKIP LOCKED claim CTE and the SQL-side exponential backoff). Runtime-checked
//! either way, so the crate builds with no `DATABASE_URL` and no live database.
//! The schema lives in `migrations/` and is embedded via [`SCHEMA_SQL`] /
//! [`HARDENING_SCHEMA_SQL`]; deployments apply it declaratively, or a caller
//! may run [`apply_schema`] explicitly. These functions are the durable
//! counterparts to the pure logic in [`crate::outbox`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveValue::Set, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    DbBackend, EntityTrait, FromQueryResult, QueryFilter, QueryTrait, Statement,
};
use uuid::Uuid;

use crate::entity::{message_inbox, message_outbox};
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
/// [`apply_schema`] can run it directly and a declarative migrator can apply
/// the same file as a tracked migration.
pub const SCHEMA_SQL: &str = include_str!("../migrations/0001_fiducia_messaging.sql");
/// Forward migration adding tenant-scoped idempotency and durable claim leases.
pub const HARDENING_SCHEMA_SQL: &str =
    include_str!("../migrations/0002_tenant_dedup_and_claim_leases.sql");
/// Forward migration adding the retention indexes behind the `purge_*` helpers.
pub const RETENTION_SCHEMA_SQL: &str = include_str!("../migrations/0003_retention_indexes.sql");

/// Apply the messaging schema. Idempotent (`CREATE TABLE IF NOT EXISTS`), so it
/// is safe to call on every process start.
pub async fn apply_schema(pool: &DatabaseConnection) -> Result<(), MessagingError> {
    pool.execute_unprepared(SCHEMA_SQL)
        .await
        .map_err(MessagingError::database)?;
    pool.execute_unprepared(HARDENING_SCHEMA_SQL)
        .await
        .map_err(MessagingError::database)?;
    pool.execute_unprepared(RETENTION_SCHEMA_SQL)
        .await
        .map_err(MessagingError::database)?;
    Ok(())
}

/// The shared staged-row insert: `ON CONFLICT DO NOTHING` on the tenant-scoped
/// business key, lease/backoff columns left `NotSet` so their Postgres defaults
/// apply — the exact column list the sqlx version bound.
async fn insert_outbox_row<C: ConnectionTrait>(
    conn: &C,
    rec: &OutboxRecord,
) -> Result<(), MessagingError> {
    validate_outbox_record(rec)?;
    message_outbox::Entity::insert(message_outbox::ActiveModel {
        id: Set(rec.id),
        subject: Set(rec.subject.clone()),
        tenant_id: Set(rec.tenant_id),
        idempotency_key: Set(rec.idempotency_key.clone()),
        dedup_id: Set(rec.dedup_id.clone()),
        payload: Set(rec.payload.clone()),
        status: Set(rec.status.as_str().to_owned()),
        // Saturate rather than wrap: `attempts as i32` turns a count above
        // `i32::MAX` into a NEGATIVE attempt count, which the backoff and
        // max-attempts arithmetic then reads as "no attempts yet".
        attempts: Set(i32::try_from(rec.attempts).unwrap_or(i32::MAX)),
        created_at: Set(rec.created_at),
        ..Default::default()
    })
    .on_conflict(OnConflict::new().do_nothing().to_owned())
    .exec_without_returning(conn)
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
// transactional outbox. Codex's `Outbox::enqueue` took a transaction; that
// behaviour is preserved as [`enqueue_outbox_tx`] below, which is the correct
// entry point for the outbox pattern. This pool variant is kept for backwards
// compatibility and one-off/manual enqueues.
pub async fn enqueue_outbox(
    pool: &DatabaseConnection,
    rec: &OutboxRecord,
) -> Result<(), MessagingError> {
    insert_outbox_row(pool, rec).await
}

/// Insert a staged outbox row **inside the caller's transaction** — the correct
/// transactional-outbox usage (folded from codex's `Outbox::enqueue`): the row
/// commits atomically with the domain change it accompanies, so a message is
/// never lost nor sent for a rolled-back change. A repeated tenant-scoped
/// business key is ignored (`ON CONFLICT`), keeping enqueue idempotent.
pub async fn enqueue_outbox_tx(
    tx: &DatabaseTransaction,
    rec: &OutboxRecord,
) -> Result<(), MessagingError> {
    // Strengthens codex's non-empty-subject guard: the subject must be a
    // canonical routing class (no wildcards / injected tokens) and the payload
    // within MAX_MESSAGE_BYTES, so poison rows never enter the outbox.
    insert_outbox_row(tx, rec).await
}

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

/// The claim CTE's `RETURNING` shape — raw SQL because the entity API cannot
/// express `FOR UPDATE SKIP LOCKED` inside an updating CTE.
#[derive(FromQueryResult)]
struct ClaimedOutboxRow {
    id: Uuid,
    subject: String,
    tenant_id: Option<Uuid>,
    idempotency_key: String,
    dedup_id: String,
    payload: serde_json::Value,
    status: String,
    attempts: i32,
    created_at: DateTime<Utc>,
}

impl ClaimedOutboxRow {
    fn into_record(self) -> OutboxRecord {
        OutboxRecord {
            id: self.id,
            subject: self.subject,
            tenant_id: self.tenant_id,
            idempotency_key: self.idempotency_key,
            dedup_id: self.dedup_id,
            payload: self.payload,
            created_at: self.created_at,
            status: OutboxStatus::from_str(&self.status).unwrap_or_else(|| {
                // An unrecognized status can only mean out-of-band schema drift
                // or a manual edit; treating it as Pending re-enters the row
                // into the drain loop, which is the safe direction — but never
                // silently.
                tracing::warn!(status = %self.status, "outbox: unknown row status; treating as pending");
                OutboxStatus::Pending
            }),
            attempts: self.attempts.max(0) as u32,
        }
    }
}

/// Convert a positive, finite duration to whole milliseconds for SQL interval
/// arithmetic, rejecting zero/overflow so a misconfigured knob fails loudly
/// instead of producing an instantly-expiring lease or an everything-qualifies
/// purge cutoff.
fn positive_ms(duration: Duration, what: &str) -> Result<i64, MessagingError> {
    i64::try_from(duration.as_millis())
        .ok()
        .filter(|millis| *millis > 0)
        .ok_or_else(|| MessagingError::database(format!("{what} must be positive and finite")))
}

/// Atomically lease up to `limit` due rows to `claim_owner` and commit that
/// ownership before returning. Other workers skip an active claim; after
/// `claim_ttl` it becomes reclaimable if this worker crashed.
///
/// The returned records have already had their attempt count incremented.
pub async fn claim_pending_outbox(
    pool: &DatabaseConnection,
    claim_owner: Uuid,
    claim_ttl: Duration,
    limit: i64,
) -> Result<Vec<OutboxRecord>, MessagingError> {
    if limit <= 0 {
        return Err(MessagingError::database(
            "outbox claim limit must be positive",
        ));
    }
    let claim_ttl_ms = positive_ms(claim_ttl, "outbox claim TTL")?;
    let rows = ClaimedOutboxRow::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        CLAIM_PENDING_SQL,
        [limit.into(), claim_owner.into(), claim_ttl_ms.into()],
    ))
    .all(pool)
    .await
    .map_err(MessagingError::database)?;

    let mut records = rows
        .into_iter()
        .map(ClaimedOutboxRow::into_record)
        .collect::<Vec<_>>();
    records.sort_by_key(|record| record.created_at);
    Ok(records)
}

/// Mark a row published only if `claim_owner` still owns it. Returns `false`
/// when the lease expired and another worker reclaimed the row.
pub async fn mark_published(
    pool: &DatabaseConnection,
    id: Uuid,
    claim_owner: Uuid,
    at: DateTime<Utc>,
) -> Result<bool, MessagingError> {
    let result = message_outbox::Entity::update_many()
        .set(message_outbox::ActiveModel {
            status: Set("published".to_owned()),
            published_at: Set(Some(at)),
            last_error: Set(None),
            claim_owner: Set(None),
            claim_expires_at: Set(None),
            ..Default::default()
        })
        .filter(message_outbox::Column::Id.eq(id))
        .filter(message_outbox::Column::ClaimOwner.eq(claim_owner))
        .exec(pool)
        .await
        .map_err(MessagingError::database)?;
    Ok(result.rows_affected == 1)
}

// Raw SQL: an existence probe of the row's *live* lease (`claim_expires_at >
// now()`), which the entity API cannot express against the database clock.
const CLAIM_STILL_HELD_SQL: &str = "SELECT id FROM message_outbox
      WHERE id = $1 AND claim_owner = $2 AND claim_expires_at > now()";

/// The `CLAIM_STILL_HELD_SQL` row shape.
#[derive(FromQueryResult)]
struct HeldClaimRow {
    #[allow(dead_code)]
    id: Uuid,
}

/// How many times a relay published a row it no longer owned (see
/// [`lost_lease_publishes`]).
static LOST_LEASE_PUBLISHES: AtomicU64 = AtomicU64::new(0);

/// Process-lifetime count of rows this relay published while its claim lease
/// had already lapsed — i.e. rows another worker may publish again.
///
/// A slow broker can push a batch past `claim_ttl`; the row is then reclaimable
/// and a second worker may publish it. If the gap exceeds the stream's
/// `duplicate_window` that is a genuine double delivery, so it must never be a
/// silent outcome. Scrape this alongside the relay's logs.
pub fn lost_lease_publishes() -> u64 {
    LOST_LEASE_PUBLISHES.load(Ordering::Relaxed)
}

fn record_lost_lease() -> u64 {
    LOST_LEASE_PUBLISHES.fetch_add(1, Ordering::Relaxed) + 1
}

/// Whether `claim_owner` still holds a *live* (unexpired) lease on `id`.
/// Checked before handing a row to the broker, so a batch that overran its
/// `claim_ttl` stops publishing rows another worker has already reclaimed.
pub async fn claim_still_held(
    pool: &DatabaseConnection,
    id: Uuid,
    claim_owner: Uuid,
) -> Result<bool, MessagingError> {
    let row = HeldClaimRow::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        CLAIM_STILL_HELD_SQL,
        [id.into(), claim_owner.into()],
    ))
    .one(pool)
    .await
    .map_err(MessagingError::database)?;
    Ok(row.is_some())
}

/// Mark an owned row failed (retries exhausted). Returns `false` if ownership
/// has already moved to another relay.
pub async fn mark_failed(
    pool: &DatabaseConnection,
    id: Uuid,
    claim_owner: Uuid,
    error: &str,
) -> Result<bool, MessagingError> {
    let result = message_outbox::Entity::update_many()
        .set(message_outbox::ActiveModel {
            status: Set("failed".to_owned()),
            last_error: Set(Some(error.to_owned())),
            claim_owner: Set(None),
            claim_expires_at: Set(None),
            ..Default::default()
        })
        .filter(message_outbox::Column::Id.eq(id))
        .filter(message_outbox::Column::ClaimOwner.eq(claim_owner))
        .exec(pool)
        .await
        .map_err(MessagingError::database)?;
    Ok(result.rows_affected == 1)
}

/// Release one owned claim after a transient publish failure, record the error,
/// and defer the next attempt with exponential backoff. Returns `false` if the
/// row was already reclaimed by another owner.
///
/// Raw SQL: the backoff arithmetic (`interval` math + `power`) has no entity
/// API expression.
pub async fn reschedule_publish(
    pool: &DatabaseConnection,
    id: Uuid,
    claim_owner: Uuid,
    error: &str,
) -> Result<bool, MessagingError> {
    let result = pool
        .execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE message_outbox
                SET last_error = $3,
                    available_at = now()
                        + least(interval '5 minutes',
                                interval '1 second'
                                    * power(2, least(greatest(attempts - 1, 0), 8))),
                    claim_owner = NULL,
                    claim_expires_at = NULL
              WHERE id = $1 AND claim_owner = $2 AND status = 'pending'",
            [id.into(), claim_owner.into(), error.into()],
        ))
        .await
        .map_err(MessagingError::database)?;
    Ok(result.rows_affected() == 1)
}

// Raw SQL (like the claim CTE it undoes): the attempt decrement is column
// arithmetic the entity API can't express as a plain `Set`.
const RELEASE_CLAIMS_SQL: &str = "UPDATE message_outbox
        SET claim_owner = NULL,
            claim_expires_at = NULL,
            attempts = greatest(attempts - 1, 0)
      WHERE status = 'pending' AND claim_owner = $1";

/// Release every still-pending row owned by a batch claim. Used when a relay
/// stops a batch after the first broker failure so untouched rows are
/// immediately available to other workers rather than waiting for lease expiry.
///
/// Releasing also undoes the claim-time attempt increment: a released row was
/// leased but never offered to the broker (the batch stopped before reaching
/// it). Without the decrement, a broker outage inflates `attempts` across the
/// whole claimable window once per drain tick — rows then hit `max_attempts`
/// and are parked as `failed` on their FIRST real publish attempt. Rows this
/// claim already resolved (published/rescheduled/parked) had their
/// `claim_owner` cleared, so only untouched rows match here. A crashed worker's
/// rows skip this path (lease expiry) and keep their increment — that is the
/// rare case, and counting it is the safe direction.
pub async fn release_outbox_claims(
    pool: &DatabaseConnection,
    claim_owner: Uuid,
) -> Result<u64, MessagingError> {
    let result = pool
        .execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            RELEASE_CLAIMS_SQL,
            [claim_owner.into()],
        ))
        .await
        .map_err(MessagingError::database)?;
    Ok(result.rows_affected())
}

/// The tenant-scoped inbox claim, as a statement, so the dedup namespace this
/// insert binds is assertable without a live database.
fn inbox_claim_statement(
    tenant_id: Option<Uuid>,
    message_id: Uuid,
    idempotency_key: &str,
    received_at: DateTime<Utc>,
) -> Statement {
    message_inbox::Entity::insert(message_inbox::ActiveModel {
        message_id: Set(message_id),
        tenant_id: Set(tenant_id),
        idempotency_key: Set(idempotency_key.to_owned()),
        received_at: Set(received_at),
        ..Default::default()
    })
    .on_conflict(OnConflict::new().do_nothing().to_owned())
    .build(DbBackend::Postgres)
}

/// Try to record an incoming message for consumer dedup, **in `tenant_id`'s
/// dedup namespace**. Returns `true` the first time this message is seen there
/// and `false` for a duplicate delivery, so the caller runs the external effect
/// at most once.
///
/// `tenant_id` is required and has no default on purpose. The uniqueness this
/// insert races against is `(tenant_id, idempotency_key)` — or, for `None`, the
/// explicit *global* namespace enforced by
/// `message_inbox_global_idempotency_uq`. Passing `None` for a tenant-owned
/// message would collapse every tenant into that one namespace, so tenant B's
/// different message sharing a business key with tenant A's (`invoice/2026-07`)
/// would lose the `ON CONFLICT DO NOTHING`, return `false`, and have its effect
/// **permanently skipped**. State the namespace the message actually belongs
/// to; `None` means "genuinely global", never "unknown".
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
    pool: &DatabaseConnection,
    tenant_id: Option<Uuid>,
    message_id: Uuid,
    idempotency_key: &str,
    received_at: DateTime<Utc>,
) -> Result<bool, MessagingError> {
    let result = pool
        .execute(inbox_claim_statement(
            tenant_id,
            message_id,
            idempotency_key,
            received_at,
        ))
        .await
        .map_err(MessagingError::database)?;
    Ok(result.rows_affected() == 1)
}

/// Mark an inbox row processed once its effect completes.
pub async fn inbox_mark_processed(
    pool: &DatabaseConnection,
    message_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), MessagingError> {
    let result = message_inbox::Entity::update_many()
        .set(message_inbox::ActiveModel {
            processed_at: Set(Some(at)),
            ..Default::default()
        })
        .filter(message_inbox::Column::MessageId.eq(message_id))
        .exec(pool)
        .await
        .map_err(MessagingError::database)?;
    if result.rows_affected == 0 {
        // Nothing matched: the effect ran without a recorded claim (or against
        // the wrong id), i.e. the at-most-once bookkeeping is broken at the call
        // site. The update itself stays a no-op, but never a silent one.
        tracing::warn!(%message_id, "inbox: mark_processed matched no row; was inbox_try_insert called for this id?");
    }
    Ok(())
}

// Time-bounded purges of terminal rows. Raw SQL: the age cutoff is interval
// arithmetic (`now() - $1 ms`) against a partial-index predicate, and keeping
// the exact SQL in a const lets the tests pin the safety property (only
// terminal rows ever match).
const PURGE_PUBLISHED_OUTBOX_SQL: &str = "DELETE FROM message_outbox
      WHERE status = 'published'
        AND published_at IS NOT NULL
        AND published_at < now() - ($1 * interval '1 millisecond')";
const PURGE_PROCESSED_INBOX_SQL: &str = "DELETE FROM message_inbox
      WHERE processed_at IS NOT NULL
        AND processed_at < now() - ($1 * interval '1 millisecond')";
const PURGE_PROCESSED_CONSUMER_INBOX_SQL: &str = "DELETE FROM message_inbox_consumer
      WHERE processed_at IS NOT NULL
        AND processed_at < now() - ($1 * interval '1 millisecond')";

async fn purge(
    pool: &DatabaseConnection,
    sql: &'static str,
    older_than: Duration,
) -> Result<u64, MessagingError> {
    let cutoff_ms = positive_ms(older_than, "retention age")?;
    let result = pool
        .execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [cutoff_ms.into()],
        ))
        .await
        .map_err(MessagingError::database)?;
    Ok(result.rows_affected())
}

/// Delete `published` outbox rows older than `older_than`, returning how many
/// were removed. Only terminal rows qualify — `pending` rows (however old) and
/// parked `failed` rows are never touched: the former are undelivered work, the
/// latter are the dead-letter queue an operator still needs to inspect.
///
/// `older_than` must comfortably exceed the JetStream stream's
/// `duplicate_window`; the row's `dedup_id` is the audit trail for what was
/// published inside it.
pub async fn purge_published_outbox(
    pool: &DatabaseConnection,
    older_than: Duration,
) -> Result<u64, MessagingError> {
    purge(pool, PURGE_PUBLISHED_OUTBOX_SQL, older_than).await
}

/// Delete processed `message_inbox` rows older than `older_than`. Unprocessed
/// claims are kept: deleting one would let a redelivery re-run its effect.
/// Keep the cutoff far beyond the transport's redelivery horizon — a purged
/// claim no longer dedups a very-late replay of the same message.
pub async fn purge_processed_inbox(
    pool: &DatabaseConnection,
    older_than: Duration,
) -> Result<u64, MessagingError> {
    purge(pool, PURGE_PROCESSED_INBOX_SQL, older_than).await
}

/// Delete processed `message_inbox_consumer` claims older than `older_than`,
/// with the same caveats as [`purge_processed_inbox`].
pub async fn purge_processed_consumer_inbox(
    pool: &DatabaseConnection,
    older_than: Duration,
) -> Result<u64, MessagingError> {
    purge(pool, PURGE_PROCESSED_CONSUMER_INBOX_SQL, older_than).await
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
    pool: &'a DatabaseConnection,
    publisher: &'a dyn Publisher,
    batch_size: i64,
    max_attempts: i32,
    claim_ttl: Duration,
}

impl<'a> OutboxPublisher<'a> {
    /// Build a publisher over a connection and a [`Publisher`]. Defaults: batch
    /// of 100, up to 8 attempts before a row is parked as `failed`.
    pub fn new(pool: &'a DatabaseConnection, publisher: &'a dyn Publisher) -> Self {
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

        // A transient database error mid-drain must not strand the rest of the
        // claim: those rows are untouched but still carry `claim_owner` and a
        // future `claim_expires_at`, so no worker could claim them for up to
        // `claim_ttl` (minutes). Release first, then propagate.
        match self.drain_claimed(rows, claim_owner).await {
            Ok(published) => Ok(published),
            Err(error) => {
                if let Err(release_error) = release_outbox_claims(self.pool, claim_owner).await {
                    tracing::error!(
                        %release_error,
                        "outbox: could not release claims after a batch error; rows stay leased until claim_ttl"
                    );
                }
                Err(error)
            }
        }
    }

    async fn drain_claimed(
        &self,
        rows: Vec<OutboxRecord>,
        claim_owner: Uuid,
    ) -> Result<u64, MessagingError> {
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
            // A slow broker can push a long batch past `claim_ttl`. Publishing
            // a row whose lease already lapsed races a worker that reclaimed
            // it, so stop before the broker call rather than after.
            if !claim_still_held(self.pool, record.id, claim_owner).await? {
                let lost = record_lost_lease();
                tracing::warn!(
                    id = %record.id,
                    subject = %record.subject,
                    claim_ttl_ms = self.claim_ttl.as_millis(),
                    lost_lease_publishes = lost,
                    "outbox: claim lease expired before publish; another worker owns this row now \
                     — skipping it and shortening the batch"
                );
                break;
            }
            match self
                .publisher
                .publish(&record.subject, &record.dedup_id, &bytes)
                .await
            {
                Ok(()) => {
                    if mark_published(self.pool, record.id, claim_owner, Utc::now()).await? {
                        published += 1;
                    } else {
                        // Owner-conditioned mark matched no row: the lease
                        // lapsed between the check above and the publish, and
                        // another worker reclaimed it. The row stays `pending`
                        // and WILL be published again — deduplicated by the
                        // broker only if the gap fits `duplicate_window`.
                        let lost = record_lost_lease();
                        tracing::warn!(
                            id = %record.id,
                            subject = %record.subject,
                            dedup_id = %record.dedup_id,
                            lost_lease_publishes = lost,
                            "outbox: published a row whose claim lease had lapsed; it stays pending \
                             and may be delivered twice if the gap exceeds the stream duplicate_window"
                        );
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
    /// continues. Prefer [`run_until`](Self::run_until) in deployables so a
    /// SIGTERM drains cleanly instead of stranding claims.
    pub async fn run(self, interval: Duration) -> Result<(), MessagingError> {
        self.run_until(interval, std::future::pending::<()>()).await
    }

    /// Drain on a fixed interval until `shutdown` resolves, then return.
    ///
    /// The in-flight batch always completes first (the shutdown race happens
    /// only between batches), so every leased row is marked or released before
    /// exit. Without this, a rolling restart kills the relay mid-batch and the
    /// claimed rows sit unpublishable until the claim TTL (minutes) expires —
    /// on every single deploy.
    pub async fn run_until(
        self,
        interval: Duration,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), MessagingError> {
        let mut timer = tokio::time::interval(interval);
        // A slow batch must not be chased by a burst of make-up ticks.
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased; // a pending shutdown wins over a due tick
                _ = &mut shutdown => {
                    tracing::info!("outbox: shutdown signal received; stopping after the drained batch");
                    return Ok(());
                }
                _ = timer.tick() => {
                    if let Err(error) = self.publish_batch().await {
                        tracing::error!(%error, "outbox: publish batch failed; retrying on the next interval");
                    }
                }
            }
        }
    }
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

    // The claim increments attempts up front; release must undo it for rows the
    // batch never reached, or a broker outage marches the whole claimable
    // window to max_attempts without a single real publish attempt.
    #[test]
    fn release_undoes_the_claim_attempt_increment() {
        assert!(CLAIM_PENDING_SQL.contains("attempts = outbox.attempts + 1"));
        assert!(RELEASE_CLAIMS_SQL.contains("attempts = greatest(attempts - 1, 0)"));
        assert!(RELEASE_CLAIMS_SQL.contains("status = 'pending' AND claim_owner = $1"));
    }

    // Purges may only ever remove terminal rows: published outbox rows and
    // processed inbox claims. Pending work and the failed dead-letter queue
    // must never match, and every purge must be time-bounded.
    #[test]
    fn purges_target_only_terminal_rows_and_are_time_bounded() {
        assert!(PURGE_PUBLISHED_OUTBOX_SQL.contains("status = 'published'"));
        assert!(PURGE_PUBLISHED_OUTBOX_SQL.contains("published_at IS NOT NULL"));
        assert!(!PURGE_PUBLISHED_OUTBOX_SQL.contains("failed"));
        for sql in [
            PURGE_PROCESSED_INBOX_SQL,
            PURGE_PROCESSED_CONSUMER_INBOX_SQL,
        ] {
            assert!(sql.contains("processed_at IS NOT NULL"));
        }
        for sql in [
            PURGE_PUBLISHED_OUTBOX_SQL,
            PURGE_PROCESSED_INBOX_SQL,
            PURGE_PROCESSED_CONSUMER_INBOX_SQL,
        ] {
            assert!(sql.trim_start().starts_with("DELETE FROM"));
            assert!(sql.contains("now() - ($1 * interval '1 millisecond')"));
        }
        assert!(RETENTION_SCHEMA_SQL.contains("message_outbox_published_retention_idx"));
        assert!(RETENTION_SCHEMA_SQL.contains("message_inbox_retention_idx"));
    }

    /// The inbox claim must bind the caller's dedup namespace, not silently
    /// default to the global one: a `NULL` tenant_id is a *namespace*
    /// (`message_inbox_global_idempotency_uq`), so a tenant-owned message
    /// claimed there would collide with every other tenant's same business key
    /// and have its effect permanently skipped.
    #[test]
    fn inbox_claim_binds_the_callers_tenant_dedup_namespace() {
        let tenant = Uuid::from_u128(7);
        let message = Uuid::from_u128(70);
        let at = DateTime::<Utc>::from_timestamp(1_770_000_000, 0).unwrap();

        let scoped = inbox_claim_statement(Some(tenant), message, "invoice/2026-07", at);
        let scoped_sql = scoped.to_string();
        assert!(scoped_sql.contains("tenant_id"), "{scoped_sql}");
        assert!(
            scoped_sql.contains(&tenant.to_string()),
            "the claim must carry the caller's tenant: {scoped_sql}"
        );
        assert!(scoped_sql.contains("ON CONFLICT"), "{scoped_sql}");
        assert!(scoped_sql.contains("DO NOTHING"), "{scoped_sql}");

        // The explicit global namespace still exists, and is distinguishable.
        let global = inbox_claim_statement(None, message, "invoice/2026-07", at).to_string();
        assert!(global.contains("NULL"), "{global}");
        assert_ne!(scoped_sql, global);

        // Both namespaces the statement can target are backed by a uniqueness
        // constraint, so neither is a free-for-all.
        assert!(HARDENING_SCHEMA_SQL.contains("message_inbox_tenant_idempotency_uq"));
        assert!(HARDENING_SCHEMA_SQL.contains("message_inbox_global_idempotency_uq"));
    }

    /// A row published after its lease lapsed can be published again by the
    /// worker that reclaimed it. That must be observable, not silent.
    #[test]
    fn lost_lease_publishes_are_counted() {
        let before = lost_lease_publishes();
        assert_eq!(record_lost_lease(), before + 1);
        assert_eq!(lost_lease_publishes(), before + 1);

        // The pre-publish guard is owner- AND expiry-conditioned, so a batch
        // that overran claim_ttl stops instead of racing the new owner.
        assert!(CLAIM_STILL_HELD_SQL.contains("claim_owner = $2"));
        assert!(CLAIM_STILL_HELD_SQL.contains("claim_expires_at > now()"));
        assert!(CLAIM_STILL_HELD_SQL.trim_start().starts_with("SELECT"));
    }

    /// Saturating (not wrapping) attempt counts: `attempts as i32` above
    /// `i32::MAX` produces a NEGATIVE count, which the backoff and
    /// max-attempts arithmetic reads as "no attempts yet" — an infinite retry.
    #[test]
    fn staged_attempt_counts_saturate_instead_of_wrapping_negative() {
        for attempts in [0u32, 1, 8, i32::MAX as u32, i32::MAX as u32 + 1, u32::MAX] {
            let stored = i32::try_from(attempts).unwrap_or(i32::MAX);
            assert!(stored >= 0, "{attempts} wrapped to {stored}");
        }
        assert_eq!(i32::try_from(u32::MAX).unwrap_or(i32::MAX), i32::MAX);
    }

    #[test]
    fn positive_ms_rejects_zero_and_overflow() {
        assert_eq!(positive_ms(Duration::from_millis(1500), "x").unwrap(), 1500);
        assert!(positive_ms(Duration::ZERO, "x").is_err());
        // sub-millisecond truncates to 0 -> rejected, not an instant lease
        assert!(positive_ms(Duration::from_micros(900), "x").is_err());
        // u128 millis beyond i64 -> rejected, not a wrapped negative interval
        assert!(positive_ms(Duration::from_secs(u64::MAX), "x").is_err());
    }
}
