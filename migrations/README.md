# migrations

Forward-only, idempotent schema files for the outbox/inbox tables:

- `0001_fiducia_messaging.sql` — base outbox/inbox/compat tables.
- `0002_tenant_dedup_and_claim_leases.sql` — tenant-scoped idempotency +
  durable claim leases. Amends 0001 rather than editing it.
- `0003_retention_indexes.sql` — partial indexes behind the `db::purge_*`
  retention helpers.

Each is idempotent (`IF NOT EXISTS`), so they are additive and safe to re-apply;
new changes go in a new numbered file rather than editing an applied one.

Applied **declaratively out-of-band** (the crate's binaries do NOT run a
boot-time migrator) — or explicitly via `db::apply_schema`, which embeds all
three verbatim as `db::SCHEMA_SQL` / `db::HARDENING_SCHEMA_SQL` /
`db::RETENTION_SCHEMA_SQL`. The contract tests assert code and schema agree, so
drift between the queries and these files fails the build.
