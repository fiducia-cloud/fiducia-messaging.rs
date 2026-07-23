//! # fiducia-messaging
//!
//! A **library over NATS, not a broker.** The platform runs on NATS JetStream
//! for delivery, [`fiducia-node`] for coordination/authority, and Postgres for
//! state. This crate is the glue those three share: a standard **message
//! envelope**, the transactional **outbox/inbox** pattern, and a **subject
//! taxonomy**. It deliberately does *not* implement queues, persistence, or
//! routing — JetStream already does that better than we would.
//!
//! ## The core principle
//!
//! *Messages say something happened or request work; fiducia-node decides who is
//! authorized to act.* A message is never an instruction that is trusted on
//! arrival. Two envelope fields carry this rule:
//!
//! * [`idempotency_key`](envelope::MessageEnvelope::idempotency_key) — a business
//!   key for the effect the message drives, scoped by `tenant_id`, so a
//!   redelivery collapses to a single external effect (at-most-once) without
//!   cross-tenant collisions.
//! * [`fencing_token`](envelope::MessageEnvelope::fencing_token) — a monotonic
//!   token from fiducia-node's coordination (a lock/lease). A handler about to
//!   mutate the outside world must present it via
//!   [`require_fencing_token`](envelope::MessageEnvelope::require_fencing_token);
//!   a stale holder's token is rejected.
//!
//! Together, over an at-least-once transport, they yield **effectively-once**
//! external effects: the fencing token stops a stale actor from acting, and the
//! idempotency key stops a duplicate from acting twice.
//!
//! A consumer enforces both with one call —
//! [`ensure_consumable`](envelope::MessageEnvelope::ensure_consumable) — before
//! running the effect a message drives, so neither check is silently skipped.
//! The broker side has a matching precondition: the JetStream stream's
//! `duplicate_window` must be at least
//! [`min_duplicate_window`](outbox::min_duplicate_window)`(claim_ttl)` or a
//! relay's crash-window re-publish is stored as a new message and delivered
//! twice — see that function. This crate cannot set the window; the deployment
//! must, and the per-consumer inbox is the durable backstop beyond it.
//!
//! ## The outbox/inbox pattern
//!
//! A Postgres commit and a NATS publish cannot be a single atomic operation. So
//! a producer writes an [`OutboxRecord`](outbox::OutboxRecord) in the *same*
//! transaction as its domain change, and a separate [`Relay`](outbox::Relay)
//! publishes pending rows. The outbox derives its NATS dedup id from
//! `(tenant_id, idempotency_key)`. If the relay crashes after publishing but
//! before marking the row, it republishes — and JetStream's publish dedup
//! (keyed on that tenant-scoped digest) drops the duplicate. Consumers use the
//! [`Inbox`](outbox::Inbox) / `message_inbox` table to make their own effects
//! at-most-once.
//!
//! ## Subjects are routing classes
//!
//! Subjects follow `fiducia.<group>.<event>.v<version>` (see [`subjects`]). The
//! rule the taxonomy encodes: **identifiers go in the envelope, not the
//! subject** — a subject names a *kind* of message so consumers can subscribe
//! with wildcards; ids would explode the subject space and break that.
//!
//! ## Feature flags
//!
//! The default build needs no network and no external services. Optional:
//! * `postgres` — a real SeaORM-backed outbox/inbox repo ([`db`]).
//! * `nats` — a real JetStream publisher ([`NatsPublisher`](publisher::NatsPublisher)).
//!
//! [`fiducia-node`]: https://github.com/fiducia-cloud/fiducia-node.rs

#![forbid(unsafe_code)]

pub mod connect;
pub mod envelope;
pub mod error;
pub mod outbox;
pub mod publisher;
pub mod subjects;

/// JetStream stream provisioning/verification — creates the `fiducia.*` stream
/// with an explicit config and fails closed when a deployed stream's
/// `duplicate_window` is below [`min_duplicate_window`](outbox::min_duplicate_window).
/// Behind `nats` (it speaks to the broker).
#[cfg(feature = "nats")]
pub mod stream;

/// The original, non-suffixed `Envelope<T>` from the codex-authored service,
/// retained verbatim for **wire backward-compatibility** (its exact serialized
/// shape). The integrated path uses [`MessageEnvelope`](envelope::MessageEnvelope);
/// this exists so a consumer speaking the original format still decodes. Pure
/// (serde/chrono/uuid), so it stays in the default offline build.
pub mod compat_envelope;

/// The codex-authored transaction-scoped `Outbox` + `OutboxPublisher` over the
/// compat envelope, kept verbatim behind `compat-service` so the original direct
/// PostgreSQL/NATS service API is preserved end-to-end. The integrated
/// equivalents are [`Relay`](outbox::Relay) / [`OutboxPublisher`](db::OutboxPublisher).
#[cfg(feature = "compat-service")]
pub mod transactional;

/// SeaORM entities for the durable tables (`message_outbox`, `message_inbox`,
/// `message_inbox_consumer`). Behind `postgres`.
#[cfg(feature = "postgres")]
pub mod entity;

#[cfg(feature = "postgres")]
pub mod db;

/// Per-consumer Postgres inbox (grafted from codex). Behind `postgres`.
#[cfg(feature = "postgres")]
pub mod inbox;

// Key types, re-exported at the crate root.
pub use envelope::{MessageEnvelope, ENVELOPE_VERSION};
pub use error::MessagingError;
pub use outbox::{
    min_duplicate_window, tenant_scoped_dedup_id, validate_for_publish, Inbox, InboxRecord,
    OutboxRecord, OutboxStatus, Relay, RelayOutcome, DEFAULT_CLAIM_TTL, MAX_MESSAGE_BYTES,
    MAX_PUBLISH_BACKOFF,
};
pub use publisher::{PublishedMessage, Publisher, RecordingPublisher};
pub use subjects::{Subject, SubjectError};

#[cfg(feature = "nats")]
pub use publisher::NatsPublisher;

// RECONCILE: two inboxes coexist. `Inbox` (above, from `outbox`) is the
// in-memory, message-id/idempotency-key guard that runs in the default offline
// build. `PgInbox` (below, from `inbox`) is codex's Postgres per-consumer claim.
// Distinct root names avoid the collision; both keep their own module-local name
// `Inbox`.
#[cfg(feature = "postgres")]
pub use inbox::{Inbox as PgInbox, InboxDecision, InboxError};

/// The DB-coupled outbox drainer (SKIP LOCKED + backoff + retry metadata).
/// Behind `postgres`. The pure, transport-agnostic alternative is [`Relay`].
#[cfg(feature = "postgres")]
pub use db::OutboxPublisher;

// RECONCILE: codex's original service types are preserved verbatim behind
// `compat-service` under Compat* names so they don't clash with the integrated
// `OutboxPublisher` (db) / `MessageEnvelope`. The compat envelope is reachable at
// `compat_envelope::Envelope`; these are its transaction-scoped outbox + drainer.
#[cfg(feature = "compat-service")]
pub use transactional::{
    Outbox as CompatOutbox, OutboxError as CompatOutboxError,
    OutboxPublisher as CompatOutboxPublisher,
};
