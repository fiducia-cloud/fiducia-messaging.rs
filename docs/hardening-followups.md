# fiducia-messaging — hardening follow-ups

Open items after the audit + SeaORM migration. The integrated path
(`db.rs`, `outbox.rs`, `inbox.rs`, the `fiducia-relay` binary) is in good shape;
these are the remaining risks and decisions. Each is **Open** (needs a fix) or
**Deployment** (correct in code, but the deployment must do its part).

## 1. Compat service holds a DB transaction across NATS I/O — HAZARD

`transactional.rs` (`compat-service` feature) `OutboxPublisher::publish_batch`
opens `pool.begin()`, then **publishes to NATS and `flush()`es inside that open
transaction**, committing only after the whole batch. A slow/stalled broker
therefore pins a Postgres transaction (and its row locks) open for the full
batch duration, and there is **no publish timeout** — a hung `flush()` can wedge
the connection indefinitely.

This code is deliberately preserved verbatim for wire/API back-compat, so it was
not changed in the audit. (One targeted exception has since landed in the same
file: `is_publishable_subject` now also rejects UUID tokens, so the compat guard
enforces the taxonomy's cardinal invariant — identifiers live in the envelope,
never the subject — like the canonical `subjects::validate_token`. The
transaction-across-NATS-I/O hazard below is untouched.) **Decide:** either (a) harden it to the integrated
pattern — claim+commit, release the connection, publish, then mark by owner
(what `db::OutboxPublisher` does) and add a per-publish timeout — or (b) retire
the compat launcher once no consumer depends on its exact behavior. Do not leave
it running as-is against a shared Postgres under load.

## 2. Boot-time migrator was removed — make sure the schema actually lands

The SeaORM migration dropped `sqlx::migrate!` from the binaries. Schema is now
applied **declaratively out-of-band** (dpm) or via `db::apply_schema`. `apply_schema`
does embed all three files including the new `0003_retention_indexes.sql`
(`RETENTION_SCHEMA_SQL`).

**Open:** confirm the declarative apply path (whatever runs the `migrations/`
set in each environment) includes `0003`. Without it the `purge_*` retention
DELETEs still run correctly but do **full table scans** on the hot messaging
tables instead of index scans.

## 3. Retention is opt-in — pick a horizon deliberately

The relay only purges when `RELAY_RETENTION_HOURS` is set (hourly). It is off by
default on purpose: deleting a processed `message_inbox` claim gives up dedup for
a **very-late redelivery** of that message.

**Deployment:** set `RELAY_RETENTION_HOURS` well **beyond** both the transport's
redelivery horizon and the JetStream `duplicate_window` (see §4). Published
outbox rows and processed inbox rows are the only things purged; `pending` work
and the `failed` dead-letter queue are never touched.

## 4. Broker `duplicate_window` is a deployment precondition, not enforceable here

Effectively-once across the relay's crash-window re-publish depends on the
JetStream stream's `duplicate_window >= min_duplicate_window(claim_ttl)` (=
`claim_ttl + MAX_PUBLISH_BACKOFF`, i.e. **10 min** at the defaults). A shorter
window silently turns a crash-window retry into a **double delivery**. This crate
cannot set stream config.

**Deployment:** assert the stream's `duplicate_window` at provisioning time and
in a smoke check. The per-consumer inbox (`PgInbox`) is the durable backstop
beyond the window, but the window keeps the common case off it.

The relay no longer *silently* relies on the window. `db::OutboxPublisher`
re-checks its lease (`db::claim_still_held`) before each publish, and an
owner-conditioned `mark_published` that matches no row now logs a warning and
increments `db::lost_lease_publishes()` — the exact condition under which a row
stays `pending` and can be published twice. **Deployment:** alert on
`lost_lease_publishes` being non-zero; it means batches are overrunning
`claim_ttl` and the window is now load-bearing.

## 5. Test coverage gap — no live-Postgres integration test

The `db`/`inbox` tests are DB-free (they assert SQL shape + embedded-schema
contracts), so the claim CTE, `FOR UPDATE SKIP LOCKED` concurrency, the
attempt-decrement-on-release, and the tenant-scoped uniqueness constraints are
**not exercised against a real Postgres**.

**Open:** add an opt-in integration test (behind an env flag, like the e2e
suites) that runs `apply_schema` on a throwaway Postgres and drives
enqueue → claim (2 concurrent workers) → publish/mark → release → purge, asserting
no double-claim and that a broker-outage loop does not march rows to
`max_attempts` (the bug fixed this session).

## 6. Graceful shutdown — verify the deployment sends SIGTERM

`fiducia-relay` now drains the in-flight batch on SIGTERM/SIGINT (`run_until`) so
a rolling restart doesn't strand claimed rows for the lease TTL. **Deployment:**
ensure the k8s `terminationGracePeriodSeconds` comfortably exceeds one batch's
worst-case publish time, or the pod is SIGKILLed mid-drain and the benefit is
lost.
