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

use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, DatabaseConnection, DatabaseTransaction, DbErr, EntityTrait,
    QueryFilter,
};
use serde::Serialize;
use uuid::Uuid;

use crate::entity::message_inbox_consumer;
use crate::envelope::MessageEnvelope;

/// A Postgres-backed, per-consumer idempotent inbox.
#[derive(Clone)]
pub struct Inbox {
    pool: DatabaseConnection,
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
    /// Build an inbox over a SeaORM connection (itself a pooled handle).
    pub fn new(pool: DatabaseConnection) -> Self {
        Self { pool }
    }

    /// The underlying connection (e.g. to open the transaction the caller
    /// threads into [`begin`](Self::begin)).
    pub fn pool(&self) -> &DatabaseConnection {
        &self.pool
    }

    /// Claim `envelope` for `consumer` inside the caller's transaction. Returns
    /// [`InboxDecision::Process`] on the first sighting, [`InboxDecision::Duplicate`]
    /// on a redelivery. Only [`MessageEnvelope::message_id`] and
    /// [`MessageEnvelope::tenant_id`] are read.
    pub async fn begin<T: Serialize>(
        &self,
        tx: &DatabaseTransaction,
        consumer: &str,
        envelope: &MessageEnvelope<T>,
    ) -> Result<InboxDecision, InboxError> {
        if consumer.trim().is_empty() {
            return Err(InboxError::InvalidConsumer);
        }
        // `ON CONFLICT DO NOTHING` + rows-affected, exactly as before; the
        // DB-defaulted `received_at` stays `NotSet` so Postgres stamps it.
        let inserted =
            message_inbox_consumer::Entity::insert(message_inbox_consumer::ActiveModel {
                consumer: Set(consumer.to_owned()),
                message_id: Set(envelope.message_id),
                tenant_id: Set(envelope.tenant_id),
                ..Default::default()
            })
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .exec_without_returning(tx)
            .await?;
        Ok(if inserted == 1 {
            InboxDecision::Process
        } else {
            InboxDecision::Duplicate
        })
    }

    /// Stamp the claim processed, in the same transaction as the side effect.
    pub async fn mark_processed(
        &self,
        tx: &DatabaseTransaction,
        consumer: &str,
        message_id: Uuid,
    ) -> Result<(), InboxError> {
        let result = message_inbox_consumer::Entity::update_many()
            .col_expr(
                message_inbox_consumer::Column::ProcessedAt,
                Expr::current_timestamp().into(),
            )
            .filter(message_inbox_consumer::Column::Consumer.eq(consumer))
            .filter(message_inbox_consumer::Column::MessageId.eq(message_id))
            .exec(tx)
            .await?;
        if result.rows_affected == 0 {
            // No claim row in this transaction means begin() was skipped or a
            // different consumer/message_id was passed — the at-most-once
            // bookkeeping is broken at the call site. Stay a no-op (the effect
            // already ran), but never a silent one.
            tracing::warn!(
                consumer,
                %message_id,
                "inbox: mark_processed matched no claim row; was begin() called in this transaction?"
            );
        }
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
    Database(#[from] DbErr),
}
