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

    /// Payload (de)serialization failed.
    #[error("serialize/deserialize failed: {0}")]
    Serialize(#[from] serde_json::Error),

    /// The underlying transport (NATS / JetStream) rejected or dropped a
    /// publish. Kept as a string so the default build carries no NATS client.
    #[error("transport error: {0}")]
    Transport(String),

    /// A Postgres operation failed (only reachable under the `postgres`
    /// feature). Kept as a string so the default build carries no sqlx.
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
