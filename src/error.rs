//! The crate error type.
//!
//! One `thiserror` enum shared across the envelope, the publisher, the outbox
//! relay, and the (optional) Postgres repo — no panics in library code, every
//! failure is a typed value the caller can match on.

use chrono::{DateTime, Utc};
use thiserror::Error;

/// Everything that can go wrong in fiducia-messaging.
#[derive(Debug, Error)]
pub enum MessagingError {
    /// A message authorizes an external mutation but carries no fencing token,
    /// so fiducia-node cannot confirm the sender is still the current authority.
    /// Refuse the effect rather than let a stale actor mutate the world.
    #[error("message '{message_type}' authorizes an external effect but carries no fencing token")]
    MissingFencingToken {
        /// The `message_type` of the offending envelope.
        message_type: String,
    },

    /// A message was handled after its `expires_at`.
    #[error("message expired at {expired_at}")]
    Expired {
        /// The envelope's `expires_at`.
        expired_at: DateTime<Utc>,
    },

    /// An envelope's `envelope_version` is not one this build understands.
    /// Folded from codex's `EnvelopeError::UnsupportedVersion` so a peer on a
    /// newer wire format is rejected rather than silently mis-decoded.
    #[error("unsupported envelope version {0}")]
    UnsupportedEnvelopeVersion(u16),

    /// An envelope failed validation because a required identity field
    /// (`message_type`, or a present-but-blank `source`) was empty. Folded from
    /// codex's `EnvelopeError::MissingIdentity`.
    #[error("message_type and source must be non-empty")]
    MissingIdentity,

    /// A message was staged or drained with a subject that is not a canonical
    /// `fiducia.<group>.<event>.v<version>` routing class. Wildcards, extra
    /// tokens, or otherwise injected characters (e.g. from a tenant-controlled
    /// string interpolated into a subject) are rejected before reaching NATS.
    #[error("invalid publish subject: {0}")]
    InvalidSubject(#[from] crate::subjects::SubjectError),

    /// A serialized message exceeds [`crate::outbox::MAX_MESSAGE_BYTES`]. NATS
    /// enforces a server-side `max_payload` (default 1 MiB); rejecting oversize
    /// messages at the outbox boundary keeps them from poisoning the relay.
    #[error("message payload is {actual} bytes; limit is {limit}")]
    PayloadTooLarge {
        /// Serialized payload size in bytes.
        actual: usize,
        /// The enforced limit ([`crate::outbox::MAX_MESSAGE_BYTES`]).
        limit: usize,
    },

    /// Payload (de)serialization failed.
    #[error("serialize/deserialize failed: {0}")]
    Serialize(#[from] serde_json::Error),

    /// The underlying transport (NATS / JetStream) rejected or dropped a
    /// publish. Kept as a string so the default build carries no NATS client.
    #[error("transport error: {0}")]
    Transport(String),

    /// A Postgres operation failed (only reachable under the `postgres`
    /// feature). Kept as a string so the default build carries no SeaORM.
    #[error("database error: {0}")]
    Database(String),
}

impl MessagingError {
    /// Build a [`MessagingError::Transport`] from anything printable.
    pub fn transport(err: impl std::fmt::Display) -> Self {
        MessagingError::Transport(err.to_string())
    }

    /// Build a [`MessagingError::Database`] from anything printable.
    pub fn database(err: impl std::fmt::Display) -> Self {
        MessagingError::Database(err.to_string())
    }
}
