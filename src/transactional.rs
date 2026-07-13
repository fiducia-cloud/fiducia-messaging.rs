use crate::compat_envelope::Envelope;
use serde::Serialize;
use sqlx::{PgPool, Postgres, Transaction};
use std::time::Duration;
use uuid::Uuid;

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
        if subject.trim().is_empty() {
            return Err(OutboxError::InvalidSubject);
        }
        let body = envelope.encode()?;
        sqlx::query(
            "INSERT INTO message_outbox_compat (message_id, tenant_id, subject, envelope) VALUES ($1,$2,$3,$4)",
        )
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
        let rows: Vec<(Uuid, String, Vec<u8>)> = sqlx::query_as(
            "SELECT message_id, subject, envelope FROM message_outbox_compat WHERE published_at IS NULL AND available_at <= now() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $1",
        )
        .bind(self.batch_size)
        .fetch_all(&mut *tx)
        .await?;
        let mut published = 0;
        for (message_id, subject, body) in rows {
            match self.nats.publish(subject, body.into()).await {
                Ok(()) => {
                    self.nats
                        .flush()
                        .await
                        .map_err(|error| OutboxError::Nats(error.to_string()))?;
                    sqlx::query("UPDATE message_outbox_compat SET published_at=now(), attempts=attempts+1, last_error=NULL WHERE message_id=$1")
                        .bind(message_id)
                        .execute(&mut *tx)
                        .await?;
                    published += 1;
                }
                Err(error) => {
                    sqlx::query("UPDATE message_outbox_compat SET attempts=attempts+1,last_error=$2,available_at=now()+least(interval '5 minutes', interval '1 second' * power(2, least(attempts, 8))) WHERE message_id=$1")
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
    #[error("subject must be non-empty")]
    InvalidSubject,
    #[error(transparent)]
    Envelope(#[from] crate::compat_envelope::EnvelopeError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("NATS operation failed: {0}")]
    Nats(String),
}
