//! SeaORM entity for `message_inbox_consumer` — the per-consumer inbox
//! (PRIMARY KEY (consumer, message_id)), so the same message can be
//! independently, idempotently processed by several consumers. Used by
//! `inbox::Inbox` (`PgInbox`). `received_at` is DB-defaulted; inserts leave it
//! `NotSet`.

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "message_inbox_consumer")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub consumer: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub message_id: Uuid,
    pub tenant_id: Option<Uuid>,
    pub received_at: DateTimeUtc,
    pub processed_at: Option<DateTimeUtc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
