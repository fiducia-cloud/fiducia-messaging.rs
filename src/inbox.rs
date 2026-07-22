//! Per-consumer idempotent inbox — the `postgres` feature.
//!
//! Grafted from codex's `inbox` module. Where [`crate::outbox::Inbox`] is a
//! bounded in-memory guard and [`crate::db::inbox_try_insert`] keys dedup on
//! `message_id` within one tenant namespace (a message is consumed once per
//! tenant), this [`Inbox`] keys the claim on `(consumer, message_id)` — so the
//! SAME message can be independently, idempotently processed by SEVERAL
//! consumers, and a claim held by a *different* tenant is rejected rather than
//! reported as a duplicate (see [`InboxError::TenantMismatch`]).
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
    ///
    /// The claim key is `(consumer, message_id)` and `message_id` is
    /// *producer-supplied*, so on an untrusted-producer subject tenant B could
    /// replay tenant A's `message_id` and make A's genuine message look like a
    /// duplicate — a cross-tenant effect-suppression primitive. A conflicting
    /// row belonging to a different tenant is therefore rejected with
    /// [`InboxError::TenantMismatch`] rather than reported as a duplicate.
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
        if let Some(decision) = decide_fresh_claim(inserted) {
            return Ok(decision);
        }
        // Lost the insert: confirm the existing claim is this tenant's before
        // telling the caller to skip its effect.
        let existing =
            message_inbox_consumer::Entity::find_by_id((consumer.to_owned(), envelope.message_id))
                .one(tx)
                .await?;
        decide_conflicting_claim(
            existing.map(|row| row.tenant_id),
            envelope.message_id,
            envelope.tenant_id,
        )
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

/// The first-sighting half of [`Inbox::begin`]'s decision: winning the
/// `ON CONFLICT DO NOTHING` insert (exactly one row) means nobody had claimed
/// this `(consumer, message_id)` yet, so the effect must run.
fn decide_fresh_claim(rows_inserted: u64) -> Option<InboxDecision> {
    (rows_inserted == 1).then_some(InboxDecision::Process)
}

/// Resolve a lost `ON CONFLICT DO NOTHING` race: the claim row already exists,
/// so decide whether it is genuinely *this* tenant's redelivery.
///
/// `existing_tenant` is `None` when the row could not be read back at all (it
/// was purged between the insert and the read) and `Some(tenant)` otherwise,
/// where `tenant` may itself be `None` for the explicit global namespace.
fn decide_conflicting_claim(
    existing_tenant: Option<Option<Uuid>>,
    message_id: Uuid,
    envelope_tenant: Option<Uuid>,
) -> Result<InboxDecision, InboxError> {
    match existing_tenant {
        // Same tenant (or both explicitly global): an ordinary redelivery.
        Some(existing) if existing == envelope_tenant => Ok(InboxDecision::Duplicate),
        // A different tenant already holds this producer-supplied message_id.
        // Reporting `Duplicate` here would let that tenant suppress this one's
        // effect, so fail closed and let the caller roll back.
        Some(existing) => Err(InboxError::TenantMismatch {
            message_id,
            claimed_by: existing,
            envelope_tenant,
        }),
        // The row vanished between the insert and the read (retention purge).
        // Skip the effect: at-most-once is the safe direction.
        None => Ok(InboxDecision::Duplicate),
    }
}

/// Failure modes of the per-consumer inbox (preserved from codex).
#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    /// The consumer name was blank.
    #[error("consumer must be non-empty")]
    InvalidConsumer,
    /// The claim row for this `(consumer, message_id)` belongs to a different
    /// tenant. `message_id` is producer-supplied, so this is a cross-tenant
    /// replay attempting to suppress another tenant's effect.
    #[error(
        "inbox claim for message {message_id} is held by tenant {claimed_by:?}, \
         not the envelope's tenant {envelope_tenant:?}"
    )]
    TenantMismatch {
        /// The contested (producer-supplied) message id.
        message_id: Uuid,
        /// The tenant that already holds the claim.
        claimed_by: Option<Uuid>,
        /// The tenant on the envelope being claimed.
        envelope_tenant: Option<Uuid>,
    },
    /// A Postgres operation failed.
    #[error(transparent)]
    Database(#[from] DbErr),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> Uuid {
        Uuid::from_u128(42)
    }

    fn tenant(n: u128) -> Option<Uuid> {
        Some(Uuid::from_u128(n))
    }

    /// Winning the `ON CONFLICT DO NOTHING` insert is a first sighting: the
    /// consumer must run the effect.
    #[test]
    fn first_sighting_processes() {
        assert_eq!(decide_fresh_claim(1), Some(InboxDecision::Process));
    }

    /// Losing the insert to this same tenant's earlier claim is an ordinary
    /// redelivery: skip the effect.
    #[test]
    fn redelivery_is_a_duplicate() {
        assert_eq!(decide_fresh_claim(0), None, "a lost insert is not a claim");

        for scope in [tenant(1), None] {
            assert_eq!(
                decide_conflicting_claim(Some(scope), message(), scope).unwrap(),
                InboxDecision::Duplicate,
                "the same tenant's redelivery must be skipped"
            );
        }

        // A claim row purged by retention between the insert and the read still
        // resolves to the at-most-once direction rather than an error.
        assert_eq!(
            decide_conflicting_claim(None, message(), tenant(1)).unwrap(),
            InboxDecision::Duplicate
        );
    }

    /// `message_id` is producer-supplied. If tenant B replays tenant A's
    /// message_id, reporting `Duplicate` would let B suppress A's genuine
    /// effect — a cross-tenant effect-suppression primitive. Fail closed.
    #[test]
    fn a_foreign_tenants_claim_never_suppresses_this_tenants_effect() {
        let a = tenant(1);
        let b = tenant(2);

        let decision = decide_conflicting_claim(Some(a), message(), b);
        assert!(
            matches!(
                decision,
                Err(InboxError::TenantMismatch {
                    message_id,
                    claimed_by,
                    envelope_tenant,
                }) if message_id == message() && claimed_by == a && envelope_tenant == b
            ),
            "expected a tenant mismatch, got {decision:?}"
        );

        // The global namespace is a distinct scope in both directions.
        assert!(matches!(
            decide_conflicting_claim(Some(None), message(), a),
            Err(InboxError::TenantMismatch { .. })
        ));
        assert!(matches!(
            decide_conflicting_claim(Some(a), message(), None),
            Err(InboxError::TenantMismatch { .. })
        ));

        // ...and the error says which tenants collided, so an operator can see
        // the replay rather than a bare "duplicate".
        let rendered = decide_conflicting_claim(Some(a), message(), b)
            .unwrap_err()
            .to_string();
        assert!(rendered.contains(&message().to_string()), "{rendered}");
    }
}
