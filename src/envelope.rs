//! The standard message envelope.
//!
//! Every message on the bus is a [`MessageEnvelope<T>`]: a typed `payload`
//! wrapped in routing/tracing/authority metadata that is identical across all
//! message types. Two fields carry the platform's core rule that *messages say
//! something happened or request work, but fiducia-node decides who may act*:
//!
//!   * `idempotency_key` — a business key for the effect this message drives, so
//!     a redelivery is deduplicated to a single external effect (at-most-once).
//!   * `fencing_token` — a monotonic token minted by fiducia-node's coordination
//!     (a lock/lease). Any handler about to mutate the outside world must present
//!     it; a stale holder's token is rejected. Together they give
//!     *effectively-once* external effects over an at-least-once transport.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::MessagingError;

/// A typed message plus the metadata every message on the bus carries.
///
/// Construct with [`MessageEnvelope::new`] (convenience, non-deterministic ids +
/// clock) or [`MessageEnvelope::new_at`] (deterministic — caller threads in
/// `now` and the `message_id`), then chain the `with_*` builders.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MessageEnvelope<T> {
    /// Unique id of *this* message. Doubles as the JetStream dedup id.
    pub message_id: Uuid,
    /// Stable routing/type name, e.g. `execution.completed`.
    pub message_type: String,
    /// Payload schema version; bump on a breaking payload change.
    pub schema_version: u32,
    /// Ties every message in one logical flow together (the root message's id).
    pub correlation_id: Uuid,
    /// The message that directly caused this one (for causation chains).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<Uuid>,
    /// Owning tenant, when the flow is tenant-scoped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<Uuid>,
    /// Owning workflow, when part of an orchestration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<Uuid>,
    /// The specific execution/run this message belongs to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<Uuid>,
    /// Business idempotency key for the effect this message drives.
    pub idempotency_key: String,
    /// Authority token from fiducia-node; required to authorize external
    /// mutations (see [`require_fencing_token`](Self::require_fencing_token)).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    /// When the message was produced.
    pub created_at: DateTime<Utc>,
    /// Optional deadline; a handler should drop the message past it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    /// W3C `traceparent` for distributed tracing continuity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_parent: Option<String>,
    /// The typed payload.
    pub payload: T,
}

impl<T> MessageEnvelope<T> {
    /// Convenience constructor: mints a fresh `message_id`/`correlation_id` and
    /// stamps `created_at = Utc::now()`. Because it calls `Utc::now()` and
    /// `Uuid::new_v4()`, it is deliberately *not* exercised by exact-value tests
    /// — use [`new_at`](Self::new_at) there.
    pub fn new(
        message_type: impl Into<String>,
        payload: T,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Self::new_at(
            Utc::now(),
            Uuid::new_v4(),
            message_type,
            payload,
            idempotency_key,
        )
    }

    /// Deterministic constructor. The caller supplies `now` and `message_id`;
    /// `correlation_id` is seeded to `message_id`, making this the root of a new
    /// correlation chain (override with [`with_correlation`](Self::with_correlation)).
    pub fn new_at(
        now: DateTime<Utc>,
        message_id: Uuid,
        message_type: impl Into<String>,
        payload: T,
        idempotency_key: impl Into<String>,
    ) -> Self {
        MessageEnvelope {
            message_id,
            message_type: message_type.into(),
            schema_version: 1,
            correlation_id: message_id,
            causation_id: None,
            tenant_id: None,
            workflow_id: None,
            execution_id: None,
            idempotency_key: idempotency_key.into(),
            fencing_token: None,
            created_at: now,
            expires_at: None,
            trace_parent: None,
            payload,
        }
    }

    /// Set the payload schema version.
    pub fn with_schema_version(mut self, version: u32) -> Self {
        self.schema_version = version;
        self
    }

    /// Attach the authority [`fencing_token`](Self::fencing_token).
    pub fn with_fencing_token(mut self, token: u64) -> Self {
        self.fencing_token = Some(token);
        self
    }

    /// Join an existing correlation chain instead of starting a new one.
    pub fn with_correlation(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = correlation_id;
        self
    }

    /// Record the message that directly caused this one.
    pub fn with_causation(mut self, causation_id: Uuid) -> Self {
        self.causation_id = Some(causation_id);
        self
    }

    /// Scope the message to a tenant.
    pub fn with_tenant(mut self, tenant_id: Uuid) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }

    /// Scope the message to a workflow.
    pub fn with_workflow(mut self, workflow_id: Uuid) -> Self {
        self.workflow_id = Some(workflow_id);
        self
    }

    /// Scope the message to an execution/run.
    pub fn with_execution(mut self, execution_id: Uuid) -> Self {
        self.execution_id = Some(execution_id);
        self
    }

    /// Set a deadline after which the message should be dropped.
    pub fn with_expiry(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Carry a W3C `traceparent`.
    pub fn with_trace_parent(mut self, trace_parent: impl Into<String>) -> Self {
        self.trace_parent = Some(trace_parent.into());
        self
    }

    /// Return the fencing token, or error if absent.
    ///
    /// Call this before any externally-visible mutation the message authorizes:
    /// the token is what fiducia-node uses to confirm the sender is still the
    /// current authority (a stale holder is rejected), turning an at-least-once
    /// delivery into an effectively-once external effect.
    pub fn require_fencing_token(&self) -> Result<u64, MessagingError> {
        self.fencing_token
            .ok_or_else(|| MessagingError::MissingFencingToken {
                message_type: self.message_type.clone(),
            })
    }

    /// True when `now` is at or past `expires_at`. Envelopes without an expiry
    /// never expire.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_some_and(|exp| now >= exp)
    }
}

impl<T: Serialize> MessageEnvelope<T> {
    /// Serialize the whole envelope to JSON bytes (the on-the-wire form).
    pub fn to_vec(&self) -> Result<Vec<u8>, MessagingError> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Serialize the whole envelope to a `serde_json::Value` (for an outbox row).
    pub fn to_json_value(&self) -> Result<serde_json::Value, MessagingError> {
        Ok(serde_json::to_value(self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct DemoPayload {
        work_item: String,
        attempt: u32,
    }

    fn fixed_now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 12, 8, 30, 0).unwrap()
    }

    fn fixed_id() -> Uuid {
        Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap()
    }

    #[test]
    fn round_trips_through_json() {
        let env = MessageEnvelope::new_at(
            fixed_now(),
            fixed_id(),
            "execution.requested",
            DemoPayload {
                work_item: "wi-1".into(),
                attempt: 2,
            },
            "idem-abc",
        )
        .with_fencing_token(42)
        .with_tenant(fixed_id())
        .with_expiry(fixed_now());

        let bytes = env.to_vec().expect("serialize");
        let back: MessageEnvelope<DemoPayload> =
            serde_json::from_slice(&bytes).expect("deserialize");

        assert_eq!(env, back);
        assert_eq!(back.correlation_id, fixed_id());
        assert_eq!(back.schema_version, 1);
        assert_eq!(back.fencing_token, Some(42));
    }

    #[test]
    fn omitted_options_round_trip_to_none() {
        let env = MessageEnvelope::new_at(
            fixed_now(),
            fixed_id(),
            "work-item.created",
            serde_json::json!({ "id": "wi-9" }),
            "idem-9",
        );
        let json = serde_json::to_string(&env).unwrap();
        // Absent optional fields are not serialized.
        assert!(!json.contains("fencing_token"));
        assert!(!json.contains("causation_id"));

        let back: MessageEnvelope<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(back.fencing_token, None);
        assert_eq!(back.causation_id, None);
        assert_eq!(env, back);
    }

    #[test]
    fn require_fencing_token_ok_and_err() {
        let base =
            MessageEnvelope::new_at(fixed_now(), fixed_id(), "runner.command", (), "idem-cmd");

        // Missing token -> typed error naming the message_type.
        let err = base.require_fencing_token().unwrap_err();
        assert!(matches!(
            err,
            MessagingError::MissingFencingToken { ref message_type } if message_type == "runner.command"
        ));

        // Present token -> returned.
        let held = base.with_fencing_token(7);
        assert_eq!(held.require_fencing_token().unwrap(), 7);
    }

    #[test]
    fn expiry_is_deterministic() {
        let expires = fixed_now();
        let env =
            MessageEnvelope::new_at(fixed_now(), fixed_id(), "t", (), "k").with_expiry(expires);

        assert!(!env.is_expired(expires - chrono::Duration::seconds(1)));
        assert!(env.is_expired(expires)); // at the boundary
        assert!(env.is_expired(expires + chrono::Duration::seconds(1)));

        // No expiry set -> never expires.
        let forever = MessageEnvelope::new_at(fixed_now(), fixed_id(), "t", (), "k");
        assert!(!forever.is_expired(expires + chrono::Duration::days(3650)));
    }

    #[test]
    fn convenience_new_fills_generated_fields() {
        let env = MessageEnvelope::new("x", (), "k");
        // correlation seeded from message_id; not asserting exact values.
        assert_eq!(env.correlation_id, env.message_id);
        assert_eq!(env.idempotency_key, "k");
        assert_eq!(env.fencing_token, None);
    }
}
