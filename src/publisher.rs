//! The `Publisher` trait and its implementations.
//!
//! A [`Publisher`] is the seam between this crate and NATS JetStream. Production
//! uses [`NatsPublisher`] (the `nats` feature); tests use [`RecordingPublisher`],
//! which mirrors the one property the outbox relies on: **publish dedup by
//! `dedup_id`**. In JetStream the `dedup_id` is sent as the `Nats-Msg-Id` header
//! and the server drops duplicates within the stream's dedup window; the
//! recorder does the same in memory, so relay retries never double-deliver.

use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::MessagingError;

/// Publishes a message to a subject with a dedup id.
///
/// `dedup_id` is the JetStream `Nats-Msg-Id`: two publishes with the same
/// `dedup_id` within the stream's dedup window collapse to one stored message.
/// Callers pass the outbox row's `dedup_id`, a fixed-size digest of the
/// envelope's `(tenant_id, idempotency_key)`, so retries collapse without two
/// tenants sharing one raw business-key namespace.
#[async_trait]
pub trait Publisher: Send + Sync {
    /// Publish `payload` to `subject`, tagged with `dedup_id` for dedup.
    async fn publish(
        &self,
        subject: &str,
        dedup_id: &str,
        payload: &[u8],
    ) -> Result<(), MessagingError>;
}

/// One message captured by [`RecordingPublisher`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedMessage {
    /// The subject it was published to.
    pub subject: String,
    /// The dedup id it carried.
    pub dedup_id: String,
    /// The raw payload bytes.
    pub payload: Vec<u8>,
}

#[derive(Debug, Default)]
struct RecordingState {
    published: Vec<PublishedMessage>,
    seen: HashSet<String>,
}

/// In-memory [`Publisher`] for tests: records every accepted publish and
/// **deduplicates by `dedup_id`** (a repeat `dedup_id` is a no-op), mirroring
/// JetStream's publish dedup. Always succeeds — for failure paths, use a
/// purpose-built failing double.
#[derive(Debug, Default)]
pub struct RecordingPublisher {
    inner: Mutex<RecordingState>,
}

impl RecordingPublisher {
    /// A fresh recorder with no messages.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of the messages actually stored (post-dedup), in publish order.
    pub fn published(&self) -> Vec<PublishedMessage> {
        self.inner.lock().expect("mutex").published.clone()
    }

    /// How many distinct messages were stored.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("mutex").published.len()
    }

    /// Whether nothing has been stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether a given `dedup_id` has been seen.
    pub fn contains(&self, dedup_id: &str) -> bool {
        self.inner.lock().expect("mutex").seen.contains(dedup_id)
    }
}

#[async_trait]
impl Publisher for RecordingPublisher {
    async fn publish(
        &self,
        subject: &str,
        dedup_id: &str,
        payload: &[u8],
    ) -> Result<(), MessagingError> {
        let mut state = self.inner.lock().expect("mutex");
        // Dedup: a repeat dedup_id within the window is a no-op (server-side in
        // real JetStream). Still a success from the caller's perspective.
        if !state.seen.insert(dedup_id.to_string()) {
            return Ok(());
        }
        state.published.push(PublishedMessage {
            subject: subject.to_string(),
            dedup_id: dedup_id.to_string(),
            payload: payload.to_vec(),
        });
        Ok(())
    }
}

#[cfg(feature = "nats")]
pub use nats_impl::NatsPublisher;

#[cfg(feature = "nats")]
mod nats_impl {
    use super::*;
    use async_nats::jetstream;

    /// [`Publisher`] backed by a NATS JetStream context. Sets the `Nats-Msg-Id`
    /// header to `dedup_id` so the server collapses duplicates within the
    /// stream's dedup window — the same guarantee [`RecordingPublisher`] gives
    /// in tests — then awaits the publish ack for durability.
    pub struct NatsPublisher {
        js: jetstream::Context,
    }

    impl NatsPublisher {
        /// Wrap a JetStream context.
        pub fn new(js: jetstream::Context) -> Self {
            Self { js }
        }
    }

    #[async_trait]
    impl Publisher for NatsPublisher {
        async fn publish(
            &self,
            subject: &str,
            dedup_id: &str,
            payload: &[u8],
        ) -> Result<(), MessagingError> {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", dedup_id);
            let ack = self
                .js
                .publish_with_headers(subject.to_string(), headers, payload.to_vec().into())
                .await
                .map_err(MessagingError::transport)?;
            // Await the server ack so a caller marking the outbox row published
            // knows the message is durably stored.
            ack.await.map_err(MessagingError::transport)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn records_and_dedups() {
        let pubr = RecordingPublisher::new();
        assert!(pubr.is_empty());

        pubr.publish("fiducia.tests.completed.v1", "dedup-1", b"a")
            .await
            .unwrap();
        // Same dedup_id again -> no-op.
        pubr.publish("fiducia.tests.completed.v1", "dedup-1", b"a-again")
            .await
            .unwrap();
        pubr.publish("fiducia.tests.completed.v1", "dedup-2", b"b")
            .await
            .unwrap();

        assert_eq!(pubr.len(), 2);
        assert!(pubr.contains("dedup-1"));
        let stored = pubr.published();
        assert_eq!(stored[0].dedup_id, "dedup-1");
        assert_eq!(stored[0].payload, b"a"); // first write wins, not "a-again"
        assert_eq!(stored[1].dedup_id, "dedup-2");
    }
}
