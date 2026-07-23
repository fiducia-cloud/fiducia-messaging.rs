//! SeaORM entity for `message_inbox` — the message-id-keyed inbox (a message
//! is consumed at most once per tenant namespace). Used by `db::inbox_try_insert` /
//! `db::inbox_mark_processed`.

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "message_inbox")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub message_id: Uuid,
    pub tenant_id: Option<Uuid>,
    pub idempotency_key: String,
    pub received_at: DateTimeUtc,
    pub processed_at: Option<DateTimeUtc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
