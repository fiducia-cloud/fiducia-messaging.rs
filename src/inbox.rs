use crate::Envelope;
use serde::Serialize;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

#[derive(Clone)]
pub struct Inbox {
    _pool: PgPool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum InboxDecision {
    Process,
    Duplicate,
}

impl Inbox {
    pub fn new(pool: PgPool) -> Self {
        Self { _pool: pool }
    }
    pub async fn begin<T: Serialize>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        consumer: &str,
        envelope: &Envelope<T>,
    ) -> Result<InboxDecision, InboxError> {
        if consumer.trim().is_empty() {
            return Err(InboxError::InvalidConsumer);
        }
        let inserted = sqlx::query("INSERT INTO message_inbox (consumer, message_id, tenant_id) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING")
            .bind(consumer).bind(envelope.message_id).bind(envelope.tenant_id).execute(&mut **tx).await?;
        Ok(if inserted.rows_affected() == 1 {
            InboxDecision::Process
        } else {
            InboxDecision::Duplicate
        })
    }
    pub async fn mark_processed(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        consumer: &str,
        message_id: Uuid,
    ) -> Result<(), InboxError> {
        sqlx::query(
            "UPDATE message_inbox SET processed_at=now() WHERE consumer=$1 AND message_id=$2",
        )
        .bind(consumer)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    #[error("consumer must be non-empty")]
    InvalidConsumer,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}
