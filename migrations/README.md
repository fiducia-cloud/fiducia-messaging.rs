# migrations

Forward-only, idempotent sqlx migrations for the outbox/inbox tables. 0002
amends 0001 rather than editing it (sqlx checksums applied migrations). Also
embedded verbatim as `db::SCHEMA_SQL` / `db::HARDENING_SCHEMA_SQL` for
`apply_schema`, so a schema/code drift fails the contract tests.
