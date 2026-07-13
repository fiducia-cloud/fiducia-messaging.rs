use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

pub const ENVELOPE_VERSION: u16 = 1;

/// Original non-suffixed envelope retained for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope<T> {
    pub version: u16,
    pub message_id: Uuid,
    pub tenant_id: Uuid,
    pub message_type: String,
    pub source: String,
    pub occurred_at: DateTime<Utc>,
    pub trace_parent: Option<String>,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub payload: T,
}

#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("unsupported envelope version {0}")]
    UnsupportedVersion(u16),
    #[error("message_type and source must be non-empty")]
    MissingIdentity,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl<T> Envelope<T> {
    pub fn new(
        tenant_id: Uuid,
        message_type: impl Into<String>,
        source: impl Into<String>,
        payload: T,
    ) -> Self {
        let message_id = Uuid::new_v4();
        Self {
            version: ENVELOPE_VERSION,
            message_id,
            tenant_id,
            message_type: message_type.into(),
            source: source.into(),
            occurred_at: Utc::now(),
            trace_parent: None,
            causation_id: None,
            correlation_id: message_id,
            payload,
        }
    }

    pub fn validate(&self) -> Result<(), EnvelopeError> {
        if self.version != ENVELOPE_VERSION {
            return Err(EnvelopeError::UnsupportedVersion(self.version));
        }
        if self.message_type.trim().is_empty() || self.source.trim().is_empty() {
            return Err(EnvelopeError::MissingIdentity);
        }
        Ok(())
    }
}

impl<T: Serialize> Envelope<T> {
    pub fn encode(&self) -> Result<Vec<u8>, EnvelopeError> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }
}

impl<T: DeserializeOwned> Envelope<T> {
    pub fn decode(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        let value: Self = serde_json::from_slice(bytes)?;
        value.validate()?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_and_rejects_unknown_versions() {
        let original = Envelope::new(
            Uuid::new_v4(),
            "claim.created",
            "fiducia-memory",
            serde_json::json!({"id": 1}),
        );
        let mut decoded =
            Envelope::<serde_json::Value>::decode(&original.encode().unwrap()).unwrap();
        assert_eq!(decoded, original);
        decoded.version = 9;
        assert!(matches!(
            decoded.validate(),
            Err(EnvelopeError::UnsupportedVersion(9))
        ));
    }
}
