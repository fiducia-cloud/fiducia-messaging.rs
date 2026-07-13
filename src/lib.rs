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
//!   key for the effect the message drives, so a redelivery collapses to a single
//!   external effect (at-most-once).
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
//! ## The outbox/inbox pattern
//!
//! A Postgres commit and a NATS publish cannot be a single atomic operation. So
//! a producer writes an [`OutboxRecord`](outbox::OutboxRecord) in the *same*
//! transaction as its domain change, and a separate [`Relay`](outbox::Relay)
//! publishes pending rows. If the relay crashes after publishing but before
//! marking the row, it republishes — and JetStream's publish dedup (keyed on the
//! row's `dedup_id`) drops the duplicate. Consumers use the
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
//! * `postgres` — a real sqlx-backed outbox/inbox repo ([`db`]).
//! * `nats` — a real JetStream publisher ([`NatsPublisher`](publisher::NatsPublisher)).
//!
//! [`fiducia-node`]: https://github.com/fiducia-cloud/fiducia-node.rs

#![forbid(unsafe_code)]

pub mod envelope;
pub mod error;
pub mod outbox;
pub mod publisher;
pub mod subjects;

#[cfg(feature = "postgres")]
pub mod db;

/// Per-consumer Postgres inbox (grafted from codex). Behind `postgres`.
#[cfg(feature = "postgres")]
pub mod inbox;

// Key types, re-exported at the crate root.
pub use envelope::{MessageEnvelope, ENVELOPE_VERSION};
pub use error::MessagingError;
pub use outbox::{Inbox, InboxRecord, OutboxRecord, OutboxStatus, Relay, RelayOutcome};
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
