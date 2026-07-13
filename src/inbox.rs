//! Per-consumer idempotent inbox — the `postgres` feature.
//!
//! Grafted from codex's `inbox` module. Where [`crate::outbox::Inbox`] is an
//! in-memory guard and [`crate::db::inbox_try_insert`] keys dedup on
//! `message_id` alone (a message is consumed once *globally*), this [`Inbox`]
//! keys the claim on `(consumer, message_id)` — so the SAME message can be
//! independently, idempotently processed by SEVERAL consumers.
//!
//! The pattern: inside the *same* transaction as its side effect a consumer
//! calls [`Inbox::begin`]. A first sighting inserts the claim row and returns
//! [`InboxDecision::Process`]; a redelivery loses the `ON CONFLICT DO NOTHING`
//! insert and returns [`InboxDecision::Duplicate`], so the effect is skipped.
//! On success the consumer calls [`Inbox::mark_processed`] in that same
//! transaction and commits — effect and claim commit atomically.
//!
// RECONCILE: two inboxes coexist by design. This Postgres, per-consumer one is
// re-exported at the crate root as `PgInbox` to avoid colliding with the
// in-memory `outbox::Inbox` (re-exported as `Inbox`). Codex's original
// `message_inbox` table (PRIMARY KEY (consumer, message_id)) is preserved as
// `message_inbox_consumer`, since MINE already owns the `message_inbox` name for
// the message-id-keyed variant.

use serde::Serialize;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::envelope::MessageEnvelope;

/// A Postgres-backed, per-consumer idempotent inbox.
#[derive(Clone)]
pub struct Inbox {
    pool: PgPool,
}

/// Whether a consumer should run the effect for a message, or skip it as a
/// duplicate delivery.
#[derive(Debug, PartialEq, Eq)]
pub enum InboxDecision {
    /// First time this `(consumer, message_id)` pair is seen — run the effect.
    Process,
    /// Already claimed by this consumer — skip the effect.
    Duplicate,
}

impl Inbox {
    /// Build an inbox over a pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The underlying pool (e.g. to open the transaction the caller threads into
    /// [`begin`](Self::begin)).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Claim `envelope` for `consumer` inside the caller's transaction. Returns
    /// [`InboxDecision::Process`] on the first sighting, [`InboxDecision::Duplicate`]
    /// on a redelivery. Only [`MessageEnvelope::message_id`] and
    /// [`MessageEnvelope::tenant_id`] are read.
    pub async fn begin<T: Serialize>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        consumer: &str,
        envelope: &MessageEnvelope<T>,
    ) -> Result<InboxDecision, InboxError> {
        if consumer.trim().is_empty() {
            return Err(InboxError::InvalidConsumer);
        }
        let inserted = sqlx::query(
            "INSERT INTO message_inbox_consumer (consumer, message_id, tenant_id)
             VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING",
        )
        .bind(consumer)
        .bind(envelope.message_id)
        .bind(envelope.tenant_id)
        .execute(&mut **tx)
        .await?;
        Ok(if inserted.rows_affected() == 1 {
            InboxDecision::Process
        } else {
            InboxDecision::Duplicate
        })
    }

    /// Stamp the claim processed, in the same transaction as the side effect.
    pub async fn mark_processed(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        consumer: &str,
        message_id: Uuid,
    ) -> Result<(), InboxError> {
        sqlx::query(
            "UPDATE message_inbox_consumer
                SET processed_at = now()
              WHERE consumer = $1 AND message_id = $2",
        )
        .bind(consumer)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

/// Failure modes of the per-consumer inbox (preserved from codex).
#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    /// The consumer name was blank.
    #[error("consumer must be non-empty")]
    InvalidConsumer,
    /// A Postgres operation failed.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}
