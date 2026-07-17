//! SeaORM entities for the durable messaging tables — the `postgres` feature.
//!
//! Thin mirrors of the schema in `migrations/` (embedded as
//! [`crate::db::SCHEMA_SQL`] / [`crate::db::HARDENING_SCHEMA_SQL`]), following
//! the fleet convention (`fiducia-customer.rs`, `fiducia-admin.rs`): one file
//! per table, `DeriveEntityModel`, no relations. Queries the entity API cannot
//! express (the SKIP LOCKED claim CTE, SQL-side exponential backoff) stay as
//! raw SQL through `sea_orm::Statement` in [`crate::db`] /
//! [`crate::transactional`]; the compat table therefore needs no entity at all.

pub mod message_inbox;
pub mod message_inbox_consumer;
pub mod message_outbox;
