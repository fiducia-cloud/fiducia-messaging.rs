use crate::compat_envelope::Envelope;
use serde::Serialize;
use sqlx::{PgPool, Postgres, Transaction};
use std::time::Duration;
use uuid::Uuid;

const COMPAT_ENQUEUE_SQL: &str =
    "INSERT INTO message_outbox_compat (message_id, tenant_id, subject, envelope) VALUES ($1,$2,$3,$4)";
const COMPAT_CLAIM_SQL: &str =
    "SELECT message_id, subject, envelope FROM message_outbox_compat WHERE published_at IS NULL AND available_at <= now() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $1";
const COMPAT_MARK_PUBLISHED_SQL: &str =
    "UPDATE message_outbox_compat SET published_at=now(), attempts=attempts+1, last_error=NULL WHERE message_id=$1";
const COMPAT_RESCHEDULE_SQL: &str =
    "UPDATE message_outbox_compat SET attempts=attempts+1,last_error=$2,available_at=now()+least(interval '5 minutes', interval '1 second' * power(2, least(attempts, 8))) WHERE message_id=$1";

/// Injection guard for compat subjects. The compat service historically
/// accepted any non-empty subject, so this stays deliberately looser than the
/// canonical taxonomy (`subjects::Subject::parse`) — but a subject assembled
/// from an untrusted string must not smuggle NATS wildcards (`*`, `>`),
/// whitespace/control characters, or empty tokens (leading/trailing/double
/// dots) into the publish path.
fn is_publishable_subject(subject: &str) -> bool {
    !subject.trim().is_empty()
        && !subject
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || c == '*' || c == '>')
        && subject.split('.').all(|token| !token.is_empty())
}

fn validate_compat_publish(subject: &str, payload_len: usize) -> Result<(), OutboxError> {
    if !is_publishable_subject(subject) {
        return Err(OutboxError::InvalidSubject);
    }
    if payload_len > crate::outbox::MAX_MESSAGE_BYTES {
        return Err(OutboxError::PayloadTooLarge {
            actual: payload_len,
            limit: crate::outbox::MAX_MESSAGE_BYTES,
        });
    }
    Ok(())
}

/// Transaction-scoped outbox facade retained from the original service.
#[derive(Clone)]
pub struct Outbox {
    _pool: PgPool,
}

impl Outbox {
    pub fn new(pool: PgPool) -> Self {
        Self { _pool: pool }
    }

    pub async fn enqueue<T: Serialize>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        subject: &str,
        envelope: &Envelope<T>,
    ) -> Result<(), OutboxError> {
        // Validate before and after encoding: reject an injected subject without
        // doing serialization work, then enforce the same 1 MiB publish boundary
        // as the integrated outbox on the actual encoded bytes.
        validate_compat_publish(subject, 0)?;
        let body = envelope.encode()?;
        validate_compat_publish(subject, body.len())?;
        sqlx::query(COMPAT_ENQUEUE_SQL)
            .bind(envelope.message_id)
            .bind(envelope.tenant_id)
            .bind(subject)
            .bind(body)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}

pub struct OutboxPublisher {
    pool: PgPool,
    nats: async_nats::Client,
    batch_size: i64,
}

impl OutboxPublisher {
    pub fn new(pool: PgPool, nats: async_nats::Client) -> Self {
        Self {
            pool,
            nats,
            batch_size: 100,
        }
    }

    pub async fn publish_batch(&self) -> Result<u64, OutboxError> {
        let mut tx = self.pool.begin().await?;
        let rows: Vec<(Uuid, String, Vec<u8>)> = sqlx::query_as(COMPAT_CLAIM_SQL)
            .bind(self.batch_size)
            .fetch_all(&mut *tx)
            .await?;
        let mut published = 0;
        for (message_id, subject, body) in rows {
            if let Err(error) = validate_compat_publish(&subject, body.len()) {
                // Legacy rows may predate the enqueue guard. Never hand a
                // wildcard/injected subject or oversize payload to NATS; retain
                // the row with durable error/backoff metadata for operator
                // inspection, then continue with the rest of the batch.
                sqlx::query(COMPAT_RESCHEDULE_SQL)
                    .bind(message_id)
                    .bind(error.to_string())
                    .execute(&mut *tx)
                    .await?;
                continue;
            }
            // Tag the publish with the envelope's message_id as `Nats-Msg-Id` so
            // a JetStream stream over this subject collapses a crash-window
            // re-publish (published-but-not-marked) into one stored message — the
            // same dedup the integrated path gets from `dedup_id`. On a plain
            // core-NATS subject the header is simply ignored, so this is safe
            // either way and costs nothing.
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", message_id.to_string().as_str());
            match self
                .nats
                .publish_with_headers(subject, headers, body.into())
                .await
            {
                Ok(()) => {
                    self.nats
                        .flush()
                        .await
                        .map_err(|error| OutboxError::Nats(error.to_string()))?;
                    sqlx::query(COMPAT_MARK_PUBLISHED_SQL)
                        .bind(message_id)
                        .execute(&mut *tx)
                        .await?;
                    published += 1;
                }
                Err(error) => {
                    sqlx::query(COMPAT_RESCHEDULE_SQL)
                        .bind(message_id)
                        .bind(error.to_string())
                        .execute(&mut *tx)
                        .await?;
                    break;
                }
            }
        }
        tx.commit().await?;
        Ok(published)
    }

    pub async fn run(self, interval: Duration) -> Result<(), OutboxError> {
        let mut timer = tokio::time::interval(interval);
        loop {
            timer.tick().await;
            if let Err(error) = self.publish_batch().await {
                eprintln!("outbox publish batch failed: {error}");
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error("subject must contain non-empty dot tokens and no wildcards, whitespace, or control characters")]
    InvalidSubject,
    #[error("message payload is {actual} bytes; limit is {limit}")]
    PayloadTooLarge { actual: usize, limit: usize },
    #[error(transparent)]
    Envelope(#[from] crate::compat_envelope::EnvelopeError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("NATS operation failed: {0}")]
    Nats(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compat_subject_guard_blocks_injection() {
        // Historic freedom is preserved: any dot-token subject without
        // wildcards/whitespace is accepted, canonical or not.
        assert!(is_publishable_subject("fiducia.executions.completed.v1"));
        assert!(is_publishable_subject("legacy-subject"));
        for bad in [
            "",
            "   ",
            "fiducia..completed",
            ".fiducia.executions",
            "fiducia.executions.",
            "fiducia.>",
            "fiducia.*.v1",
            "fiducia.exec utions",
            "fiducia.exec\u{7}utions",
        ] {
            assert!(!is_publishable_subject(bad), "accepted {bad:?}");
        }
    }

    #[test]
    fn compat_publish_guard_enforces_the_shared_payload_limit() {
        assert!(validate_compat_publish(
            "fiducia.executions.completed.v1",
            crate::outbox::MAX_MESSAGE_BYTES
        )
        .is_ok());
        assert!(matches!(
            validate_compat_publish(
                "fiducia.executions.completed.v1",
                crate::outbox::MAX_MESSAGE_BYTES + 1
            ),
            Err(OutboxError::PayloadTooLarge {
                actual,
                limit,
            }) if actual == crate::outbox::MAX_MESSAGE_BYTES + 1
                && limit == crate::outbox::MAX_MESSAGE_BYTES
        ));
    }

    #[test]
    fn compatibility_queries_match_the_canonical_migration() {
        let schema = crate::db::SCHEMA_SQL;
        let (_, compat_and_rest) = schema
            .split_once("CREATE TABLE IF NOT EXISTS message_outbox_compat (")
            .expect("canonical migration must define message_outbox_compat");
        let (compat_schema, _) = compat_and_rest
            .split_once("\n);")
            .expect("compat table definition must terminate");
        for column in [
            "message_id",
            "tenant_id",
            "subject",
            "envelope",
            "attempts",
            "available_at",
            "published_at",
            "last_error",
            "created_at",
        ] {
            assert!(
                compat_schema.contains(column),
                "compat schema missing {column}"
            );
        }
        for query in [
            COMPAT_ENQUEUE_SQL,
            COMPAT_CLAIM_SQL,
            COMPAT_MARK_PUBLISHED_SQL,
            COMPAT_RESCHEDULE_SQL,
        ] {
            assert!(query.contains("message_outbox_compat"));
            assert!(!query.contains("message_outbox "));
        }
    }
}
