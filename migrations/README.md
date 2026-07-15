# Migrations

PostgreSQL schema for the transactional outbox/inbox and tenant-scoped dedupe
leases. Apply in numeric order; preserve uniqueness and claim-lease semantics
when evolving delivery state.
