//! The transactional outbox + inbox — the heart of the crate.
//!
//! A DB commit and a NATS publish cannot be one atomic operation, so a producer
//! writes an [`OutboxRecord`] in the *same* transaction as its domain change,
//! and a [`Relay`] later drains those rows to a [`Publisher`](crate::Publisher).
//! Because the publisher dedups on `dedup_id`, replaying a batch after a crash
//! (published-but-not-marked) never double-delivers.
//!
//! On the consuming side, [`Inbox`] / [`InboxRecord`] give at-most-once external
//! effects: record the incoming key before acting; a duplicate loses the insert
//! and is skipped.
//!
//! Everything here is pure and deterministic — no clock, no id generation — so
//! the caller threads in ids/timestamps and the tests assert exact values.

use std::{collections::HashSet, fmt::Write};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::envelope::MessageEnvelope;
use crate::error::MessagingError;
use crate::publisher::Publisher;

/// Lifecycle of an outbox row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutboxStatus {
    /// Written, not yet published.
    Pending,
    /// Confirmed published to the bus.
    Published,
    /// Exhausted retries; needs operator attention.
    Failed,
}

impl OutboxStatus {
    /// The stored string form (matches the SQL `status` column).
    pub fn as_str(&self) -> &'static str {
        match self {
            OutboxStatus::Pending => "pending",
            OutboxStatus::Published => "published",
            OutboxStatus::Failed => "failed",
        }
    }

    /// Parse the stored string form. Returns `None` for an unknown value rather
    /// than erroring, so a stray DB value degrades to `Pending` at the call site.
    #[allow(clippy::should_implement_trait)] // intentional as_str/from_str pair, not std::str::FromStr
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(OutboxStatus::Pending),
            "published" => Some(OutboxStatus::Published),
            "failed" => Some(OutboxStatus::Failed),
            _ => None,
        }
    }
}

/// A row in `message_outbox`: a message staged for publish inside a domain
/// transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboxRecord {
    /// Row id.
    pub id: Uuid,
    /// Routing class the message goes to.
    pub subject: String,
    /// Owning tenant. `None` is an explicitly global message namespace.
    pub tenant_id: Option<Uuid>,
    /// Raw business idempotency key, unique within `tenant_id`.
    pub idempotency_key: String,
    /// Fixed-size JetStream dedup id derived from tenant + business key.
    pub dedup_id: String,
    /// The serialized envelope.
    pub payload: serde_json::Value,
    /// When the row was written.
    pub created_at: DateTime<Utc>,
    /// Where it is in its lifecycle.
    pub status: OutboxStatus,
    /// How many publish attempts have been made.
    pub attempts: u32,
}

impl OutboxRecord {
    /// A fresh global-scope `Pending` row with zero attempts.
    ///
    /// Use [`pending_for_tenant`](Self::pending_for_tenant) for tenant-owned
    /// work. `idempotency_key` is a business key, not a precomputed NATS id.
    pub fn pending(
        id: Uuid,
        subject: impl Into<String>,
        idempotency_key: impl Into<String>,
        payload: serde_json::Value,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self::pending_for_tenant(id, subject, None, idempotency_key, payload, created_at)
    }

    /// A fresh tenant-scoped `Pending` row with zero attempts.
    pub fn pending_for_tenant(
        id: Uuid,
        subject: impl Into<String>,
        tenant_id: Option<Uuid>,
        idempotency_key: impl Into<String>,
        payload: serde_json::Value,
        created_at: DateTime<Utc>,
    ) -> Self {
        let idempotency_key = idempotency_key.into();
        OutboxRecord {
            id,
            subject: subject.into(),
            tenant_id,
            dedup_id: tenant_scoped_dedup_id(tenant_id, &idempotency_key),
            idempotency_key,
            payload,
            created_at,
            status: OutboxStatus::Pending,
            attempts: 0,
        }
    }

    /// Stage an envelope for publish. The raw business key is retained for the
    /// tenant-scoped database uniqueness constraint; the NATS dedup id is a
    /// fixed-size digest of `(tenant_id, idempotency_key)`.
    pub fn from_envelope<T: Serialize>(
        id: Uuid,
        subject: impl Into<String>,
        envelope: &MessageEnvelope<T>,
    ) -> Result<Self, MessagingError> {
        Ok(Self::pending_for_tenant(
            id,
            subject,
            envelope.tenant_id,
            envelope.idempotency_key.clone(),
            envelope.to_json_value()?,
            envelope.created_at,
        ))
    }
}

/// Derive the JetStream `Nats-Msg-Id` from a tenant-scoped business key.
///
/// Length-prefixing prevents ambiguous concatenations, and SHA-256 keeps the
/// header bounded while avoiding disclosure of the business key. The versioned
/// domain separator makes any future encoding change explicit.
pub fn tenant_scoped_dedup_id(tenant_id: Option<Uuid>, idempotency_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"fiducia-messaging:nats-dedup:v1\0");
    match tenant_id {
        Some(tenant_id) => {
            hasher.update(b"tenant\0");
            hasher.update(tenant_id.as_bytes());
        }
        None => hasher.update(b"global\0"),
    }
    hasher.update((idempotency_key.len() as u64).to_be_bytes());
    hasher.update(idempotency_key.as_bytes());
    let digest = hasher.finalize();
    let mut output = String::with_capacity(3 + digest.len() * 2);
    output.push_str("v1-");
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

/// The result of draining a batch: which rows published and which failed, so the
/// caller can mark them accordingly in the DB.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RelayOutcome {
    /// Ids that published successfully (mark `published`).
    pub published: Vec<Uuid>,
    /// Ids that failed, each with the error text (leave `pending`/`failed`).
    pub failed: Vec<(Uuid, String)>,
}

impl RelayOutcome {
    /// Number of rows published.
    pub fn published_count(&self) -> usize {
        self.published.len()
    }

    /// Number of rows that failed.
    pub fn failed_count(&self) -> usize {
        self.failed.len()
    }

    /// Whether nothing was processed.
    pub fn is_empty(&self) -> bool {
        self.published.is_empty() && self.failed.is_empty()
    }
}

/// Drains pending outbox rows to a [`Publisher`].
///
/// Pure coordination: it holds no state and touches no DB. You hand it a batch
/// you claimed and it reports what to mark published vs. retry. Because the
/// publisher dedups on `dedup_id`, re-draining an already-published row is a
/// no-op that still counts as success — so a crash between publish and DB mark
/// is safe.
pub struct Relay<'a> {
    publisher: &'a dyn Publisher,
}

impl<'a> Relay<'a> {
    /// Build a relay over a publisher.
    pub fn new(publisher: &'a dyn Publisher) -> Self {
        Relay { publisher }
    }

    /// Publish every record in `batch`, collecting successes and failures. Never
    /// panics: a serialize or transport failure on one row is recorded and the
    /// drain continues.
    pub async fn drain(&self, batch: &[OutboxRecord]) -> RelayOutcome {
        let mut outcome = RelayOutcome::default();
        for rec in batch {
            let bytes = match serde_json::to_vec(&rec.payload) {
                Ok(b) => b,
                Err(e) => {
                    outcome.failed.push((rec.id, e.to_string()));
                    continue;
                }
            };
            match self
                .publisher
                .publish(&rec.subject, &rec.dedup_id, &bytes)
                .await
            {
                Ok(()) => outcome.published.push(rec.id),
                Err(e) => outcome.failed.push((rec.id, e.to_string())),
            }
        }
        outcome
    }
}

/// A row in `message_inbox`: proof a message was received, for consumer dedup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxRecord {
    /// The received message's id (primary dedup key).
    pub message_id: Uuid,
    /// The message's business idempotency key.
    pub idempotency_key: String,
    /// When it was received.
    pub received_at: DateTime<Utc>,
    /// When its effect completed, if it has.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processed_at: Option<DateTime<Utc>>,
}

impl InboxRecord {
    /// A freshly-received, not-yet-processed record.
    pub fn received(
        message_id: Uuid,
        idempotency_key: impl Into<String>,
        received_at: DateTime<Utc>,
    ) -> Self {
        InboxRecord {
            message_id,
            idempotency_key: idempotency_key.into(),
            received_at,
            processed_at: None,
        }
    }

    /// Stamp the effect as done.
    pub fn mark_processed(&mut self, at: DateTime<Utc>) {
        self.processed_at = Some(at);
    }

    /// Whether the effect has completed.
    pub fn is_processed(&self) -> bool {
        self.processed_at.is_some()
    }
}

/// In-memory at-most-once guard for *incoming* messages.
///
/// Before running an external effect for a message, call [`accept`](Self::accept)
/// with its idempotency key; a `false` means the key was already accepted (the
/// sender retried) and the effect must be skipped. The Postgres equivalent is
/// `db::inbox_try_insert`.
#[derive(Debug, Default)]
pub struct Inbox {
    seen: HashSet<String>,
}

impl Inbox {
    /// A fresh, empty inbox.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` the first time a global business key is seen, `false` for
    /// a duplicate.
    pub fn accept(&mut self, key: &str) -> bool {
        self.accept_for_tenant(None, key)
    }

    /// Tenant-scoped equivalent of [`accept`](Self::accept). The same business
    /// key may be accepted independently by different tenants.
    pub fn accept_for_tenant(&mut self, tenant_id: Option<Uuid>, key: &str) -> bool {
        self.seen.insert(tenant_scoped_dedup_id(tenant_id, key))
    }

    /// Whether a global business key has already been accepted.
    pub fn contains(&self, key: &str) -> bool {
        self.seen.contains(&tenant_scoped_dedup_id(None, key))
    }

    /// Whether a tenant-scoped business key has already been accepted.
    pub fn contains_for_tenant(&self, tenant_id: Option<Uuid>, key: &str) -> bool {
        self.seen.contains(&tenant_scoped_dedup_id(tenant_id, key))
    }

    /// Number of distinct keys accepted.
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Whether nothing has been accepted.
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::RecordingPublisher;
    use async_trait::async_trait;
    use chrono::TimeZone;

    fn at(secs: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 12, 0, 0, secs).unwrap()
    }

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn rec(n: u128, dedup: &str) -> OutboxRecord {
        OutboxRecord::pending(
            id(n),
            "fiducia.executions.completed.v1",
            dedup,
            serde_json::json!({ "n": n }),
            at(n as u32),
        )
    }

    #[tokio::test]
    async fn relay_drains_a_batch() {
        let publisher = RecordingPublisher::new();
        let relay = Relay::new(&publisher);
        let batch = vec![rec(1, "d-1"), rec(2, "d-2"), rec(3, "d-3")];

        let outcome = relay.drain(&batch).await;

        assert_eq!(outcome.published, vec![id(1), id(2), id(3)]);
        assert!(outcome.failed.is_empty());
        assert_eq!(publisher.len(), 3);
    }

    #[tokio::test]
    async fn duplicate_dedup_id_publishes_once() {
        let publisher = RecordingPublisher::new();
        let relay = Relay::new(&publisher);
        // Two distinct rows sharing a dedup_id (e.g. a re-enqueue after a crash).
        let batch = vec![rec(1, "same"), rec(2, "same")];

        let outcome = relay.drain(&batch).await;

        // Both are "successful" from the relay's view (the second is a no-op)...
        assert_eq!(outcome.published, vec![id(1), id(2)]);
        // ...but only one message actually reached the bus.
        assert_eq!(publisher.len(), 1);
    }

    #[tokio::test]
    async fn relay_records_publisher_failure_without_crashing() {
        struct FailingPublisher;
        #[async_trait]
        impl Publisher for FailingPublisher {
            async fn publish(
                &self,
                _subject: &str,
                _dedup_id: &str,
                _payload: &[u8],
            ) -> Result<(), MessagingError> {
                Err(MessagingError::Transport("nats down".into()))
            }
        }

        let publisher = FailingPublisher;
        let relay = Relay::new(&publisher);
        let batch = vec![rec(1, "d-1"), rec(2, "d-2")];

        let outcome = relay.drain(&batch).await;

        assert!(outcome.published.is_empty());
        assert_eq!(outcome.failed_count(), 2);
        assert_eq!(outcome.failed[0].0, id(1));
        assert!(outcome.failed[0].1.contains("nats down"));
    }

    #[test]
    fn inbox_accept_dedups() {
        let mut inbox = Inbox::new();
        assert!(inbox.accept("idem-1")); // first time
        assert!(!inbox.accept("idem-1")); // duplicate
        assert!(inbox.accept("idem-2"));
        assert_eq!(inbox.len(), 2);
        assert!(inbox.contains("idem-1"));
    }

    #[test]
    fn inbox_record_lifecycle() {
        let mut r = InboxRecord::received(id(1), "idem-1", at(0));
        assert!(!r.is_processed());
        r.mark_processed(at(5));
        assert!(r.is_processed());
        assert_eq!(r.processed_at, Some(at(5)));
    }

    #[test]
    fn outbox_from_envelope_scopes_business_key_to_tenant() {
        let tenant_a = id(70);
        let tenant_b = id(71);
        let env = MessageEnvelope::new_at(
            at(0),
            id(7),
            "execution.completed",
            serde_json::json!({ "ok": true }),
            "idem-key-7",
        )
        .with_tenant(tenant_a);
        let row =
            OutboxRecord::from_envelope(id(100), "fiducia.executions.completed.v1", &env).unwrap();
        assert_eq!(row.tenant_id, Some(tenant_a));
        assert_eq!(row.idempotency_key, "idem-key-7");
        assert_eq!(
            row.dedup_id,
            tenant_scoped_dedup_id(Some(tenant_a), "idem-key-7")
        );
        assert_ne!(
            row.dedup_id,
            tenant_scoped_dedup_id(Some(tenant_b), "idem-key-7")
        );
        assert_ne!(row.dedup_id, "idem-key-7");
        assert_eq!(row.dedup_id.len(), 67);
        assert_eq!(row.status, OutboxStatus::Pending);
        assert_eq!(row.attempts, 0);
        assert_eq!(row.created_at, at(0));
    }

    #[test]
    fn inbox_business_keys_are_tenant_scoped() {
        let mut inbox = Inbox::new();
        let tenant_a = id(80);
        let tenant_b = id(81);
        assert!(inbox.accept_for_tenant(Some(tenant_a), "sync-42"));
        assert!(!inbox.accept_for_tenant(Some(tenant_a), "sync-42"));
        assert!(inbox.accept_for_tenant(Some(tenant_b), "sync-42"));
        assert!(inbox.contains_for_tenant(Some(tenant_a), "sync-42"));
    }

    #[test]
    fn status_string_round_trips() {
        for s in [
            OutboxStatus::Pending,
            OutboxStatus::Published,
            OutboxStatus::Failed,
        ] {
            assert_eq!(OutboxStatus::from_str(s.as_str()), Some(s));
        }
        assert_eq!(OutboxStatus::from_str("nonsense"), None);
    }
}
