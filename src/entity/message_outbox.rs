//! SeaORM entity for `message_outbox` — the transactional outbox. Columns
//! match migration 0001 plus the 0002 hardening columns (tenant-scoped
//! idempotency + durable claim leases). Lease/backoff bookkeeping columns
//! (`available_at`, `claim_owner`, `claim_expires_at`, `last_error`,
//! `published_at`) are DB-defaulted or owner-conditioned; inserts leave them
//! `NotSet` so Postgres defaults apply, exactly like the original column list.

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "message_outbox")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub subject: String,
    pub tenant_id: Option<Uuid>,
    pub idempotency_key: String,
    pub dedup_id: String,
    pub payload: Json,
    pub status: String,
    pub attempts: i32,
    pub available_at: DateTimeUtc,
    pub last_error: Option<String>,
    pub created_at: DateTimeUtc,
    pub published_at: Option<DateTimeUtc>,
    pub claim_owner: Option<Uuid>,
    pub claim_expires_at: Option<DateTimeUtc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
