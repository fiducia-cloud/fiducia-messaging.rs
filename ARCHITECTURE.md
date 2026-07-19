# Architecture

`fiducia-messaging` is a **library over NATS JetStream, not a broker**. This
document explains what the crate is, where it sits in the fiducia.cloud
platform, its data model and NATS topology, the end-to-end message flows, the
delivery guarantees and how they are achieved, and how it is operated. All of
it is grounded in the code; file paths are cited throughout.

## 1. What this crate is, and what it deliberately is not

The fiducia.cloud platform's standing decision is a three-way split of
responsibilities:

| concern | owner |
| --- | --- |
| message delivery (queueing, persistence of in-flight messages, fan-out, replay) | **NATS JetStream** |
| authority — who may act (locks, leases, fencing) | **fiducia-node** |
| durable state | **Postgres / CockroachDB** |

`fiducia-messaging` is the thin glue those three share so every service speaks
the bus the same way. (That is the *intended* role: no service consumes the
crate yet, and the relay is not yet deployed — see the README's "Adoption
status" section for the honest current-state snapshot and the adoption path.)
It contributes exactly three things
(`src/lib.rs`, `Cargo.toml` description):

1. a standard **message envelope** (`src/envelope.rs`),
2. the transactional **outbox/inbox** pattern (`src/outbox.rs`, `src/db.rs`,
   `src/inbox.rs`, `migrations/`),
3. a **subject taxonomy** (`src/subjects.rs`).

It implements **no queueing, persistence, or routing of its own** — JetStream
does that. The crate's default build (`cargo test --locked`) pulls in no NATS
client and no database driver and runs entirely in-memory; `async-nats` and
`sea-orm` are optional dependencies gated behind the `nats` and `postgres`
features (`Cargo.toml`). There is one small deployable, the `fiducia-relay`
binary (`src/main.rs`), which is nothing more than a drain loop from the
outbox table to JetStream — "the library is the product", as its module doc
says.

### The core principle

> Messages say something *happened* or *request* work; **fiducia-node decides
> who is authorized to act.**

A message is never a trusted instruction on arrival. Two envelope fields carry
the rule (`src/envelope.rs`):

- `idempotency_key` — a business key for the *effect* the message drives,
  scoped by `tenant_id` (or the explicit global `None` namespace). A
  redelivery collapses to a single external effect.
- `fencing_token` — a monotonic authority token minted by fiducia-node (a
  lock/lease). A handler about to mutate the outside world must present it via
  `MessageEnvelope::require_fencing_token()`; a missing token is a typed
  `MessagingError::MissingFencingToken` error, and fiducia-node rejects a
  stale holder's token.

Together, over an at-least-once transport, these yield **effectively-once
external effects**: the fencing token stops a *stale* actor from acting, the
idempotency key stops a *duplicate* from acting twice.

## 2. Module map of `src/`

| module | build | what it is |
| --- | --- | --- |
| `src/envelope.rs` | always | `MessageEnvelope<T>` — the typed payload plus uniform metadata; validating `encode`/`decode`; `require_fencing_token`; `is_expired`. |
| `src/subjects.rs` | always | `Subject` builder/parser for `fiducia.<group>.<event>.v<version>`; canonical subject constants; token validation (rejects wildcards, dots, uppercase, and UUIDs-as-tokens). |
| `src/outbox.rs` | always | The pure core of the outbox/inbox pattern: `OutboxRecord`, `OutboxStatus`, `tenant_scoped_dedup_id`, `validate_for_publish` (subject + 1 MiB size guard), the transport-agnostic `Relay`, `RelayOutcome`, the in-memory `Inbox` guard, and `InboxRecord`. Fully deterministic — no clock or id generation inside. |
| `src/publisher.rs` | always (NATS impl behind `nats`) | The `Publisher` trait — the seam to JetStream. `RecordingPublisher` (in-memory, test double that mirrors JetStream publish dedup) and `NatsPublisher` (real JetStream context; sets `Nats-Msg-Id`, awaits the publish ack). |
| `src/error.rs` | always | The single `MessagingError` enum: `MissingFencingToken`, `Expired`, `UnsupportedEnvelopeVersion`, `MissingIdentity`, `InvalidSubject`, `PayloadTooLarge`, `Serialize`, `Transport(String)`, `Database(String)`. Transport/DB variants are strings so the default build carries no client crates. |
| `src/db.rs` | `postgres` | The SeaORM-backed durable side: `apply_schema` (embeds `migrations/` via `SCHEMA_SQL` / `HARDENING_SCHEMA_SQL`), `enqueue_outbox` / `enqueue_outbox_tx`, leased `claim_pending_outbox`, owner-conditioned `mark_published` / `mark_failed` / `reschedule_publish` / `release_outbox_claims`, the scoped inbox helpers, and the DB-coupled `OutboxPublisher` drainer. Runtime-checked queries only — no `DATABASE_URL` at build time. |
| `src/inbox.rs` | `postgres` | `PgInbox` (module-local name `Inbox`) — the **per-consumer** Postgres inbox over `message_inbox_consumer`; `begin(tx, consumer, envelope)` → `InboxDecision::{Process, Duplicate}`, `mark_processed`. |
| `src/compat_envelope.rs` | always | The original, pre-merge `Envelope<T>` wire shape, preserved verbatim so peers speaking the old format still decode. Pure serde. |
| `src/transactional.rs` | `compat-service` | The original direct PostgreSQL/NATS service (`CompatOutbox` / `CompatOutboxPublisher`) over the compat envelope and its own `message_outbox_compat` table. |
| `src/main.rs` | binary `fiducia-relay` | The drain loop: Postgres pool → migrations → NATS connect → `OutboxPublisher::run(500ms)`. Prints a usage note when built without `postgres,nats`. |
| `src/bin/fiducia-messaging-compat.rs` | binary, `compat-service` | The legacy service's launcher: pool → migrations → `CompatOutboxPublisher::run(250ms)`. |

Historical note visible throughout the source: the crate is a deliberate merge
of two lineages ("MINE" and "CODEX" in `RECONCILE:` comments). That is why two
inboxes and two outbox publishers coexist — each pair covers a different
usage shape, and the compat pieces preserve the original service's exact wire
and table formats.

## 3. The message model

### 3.1 `MessageEnvelope<T>` (`src/envelope.rs`)

Every message on the bus is a `MessageEnvelope<T>`: a typed `payload` wrapped
in metadata identical across all message types.

- **Identity / routing**: `message_id` (UUID of this envelope instance),
  `message_type` (stable type name, e.g. `execution.completed`), `source`
  (producing service, optional).
- **Versioning**, two orthogonal axes: `envelope_version` (the wire framing,
  checked against `ENVELOPE_VERSION = 1`; defaults on deserialize so
  pre-field envelopes decode) and `schema_version` (the typed payload; bump on
  breaking payload changes).
- **Causality**: `correlation_id` (ties a whole logical flow together; seeded
  to `message_id` for a chain root), `causation_id` (the direct parent).
- **Scope**: `tenant_id`, `workflow_id`, `execution_id` — all optional UUIDs.
  Note the taxonomy rule below: these identifiers live *here*, never in the
  subject.
- **Authority**: `idempotency_key` (required) and `fencing_token`
  (optional `u64`; required *at effect time* via `require_fencing_token`).
- **Lifecycle / tracing**: `created_at`, optional `expires_at`
  (`is_expired(now)` — handlers should drop expired messages), optional W3C
  `trace_parent`.

Construction is a builder chain; `new_at(now, message_id, ...)` is the
deterministic variant used in tests, `new(...)` the convenience one that calls
`Utc::now()`/`Uuid::new_v4()`. Serialization is JSON;
`encode()`/`decode()` are the validating pair (`validate()` rejects an unknown
`envelope_version` or a blank `message_type`/`source`), while `to_vec()`
skips validation. Absent optional fields are omitted from the JSON entirely
(`skip_serializing_if`).

### 3.2 The compat envelope (`src/compat_envelope.rs`)

The original service's `Envelope<T>` is retained verbatim for wire
backward-compatibility: `version`, `message_id`, **non-optional** `tenant_id`,
`message_type`, **non-optional** `source`, `occurred_at`, `trace_parent`,
`causation_id`, `correlation_id`, `payload`. It has no idempotency key and no
fencing token — which is precisely why the integrated envelope superseded it.
It stays in the default offline build (pure serde) so old messages can always
be decoded; the machinery that *publishes* it lives behind `compat-service`.

## 4. NATS topology: subjects as routing classes

Subjects follow `fiducia.<group>.<event>.v<version>` (`src/subjects.rs`),
e.g.:

```
fiducia.work-items.created.v1
fiducia.executions.{requested,progress,completed}.v1
fiducia.reviews.{requested,findings}.v1
fiducia.tests.{requested,completed}.v1
fiducia.runners.{heartbeat,commands}.v1
fiducia.github.events.v1
fiducia.jira.events.v1
```

The cardinal rule the module encodes: **identifiers go in the envelope, not
the subject.** A subject names a *kind* of message so consumers can subscribe
with wildcards (`Subject::group_wildcard("executions", 1)` →
`fiducia.executions.*.v1`). Baking a work-item/tenant/execution id into the
subject explodes the subject space and defeats those subscriptions.

Enforcement is mechanical, not advisory:

- `validate_token` accepts only non-empty lowercase `[a-z0-9-]` tokens with no
  leading/trailing hyphen — which structurally excludes NATS wildcards (`*`,
  `>`), dots (subject-level injection), and uppercase.
- A token that parses as a **UUID is rejected** (`IdentifierInSubject`) — an
  identifier leaking into a routing class.
- `Subject::parse` requires exactly four dot-tokens rooted at `fiducia` with a
  `v<N>` (N ≥ 1) version tail, then re-validates each token.

The crate does **not** define JetStream streams or consumers itself — stream
provisioning is a deployment concern of the platform. What the crate fixes is
the subject namespace those streams cover and the publish-side contract
(`Nats-Msg-Id` dedup header, ack-before-mark; see §6). Consumption from NATS
is likewise done by each service with its own JetStream consumer; this crate
supplies the *inbox* discipline that makes those deliveries safe (§5.3).

## 5. Storage schema: why a messaging crate has a database

The database is not a message store. It exists solely because **a Postgres
`COMMIT` and a NATS publish cannot be one atomic operation**: publish-then-
commit can lose the DB change, commit-then-publish can lose the message. The
transactional outbox/inbox pattern fixes this by making the *message* part of
the *domain transaction*, and the tables in `migrations/` are exactly that
pattern and nothing more.

Migrations are forward-only, idempotent (`CREATE ... IF NOT EXISTS`), and
dual-use: applied declaratively out-of-band by the deployment's migration
tooling, or directly via `db::apply_schema` which embeds the same files as
`db::SCHEMA_SQL` / `db::HARDENING_SCHEMA_SQL` (`src/db.rs`). Migration 0002 is
deliberately a separate file because migration runners record checksums —
editing an applied migration would strand deployments
(`migrations/0002_tenant_dedup_and_claim_leases.sql` header comment).

### 5.1 `message_outbox` (migrations 0001 + 0002)

One row per staged outgoing message:

- `id uuid PK`, `subject text` (routing class only), `payload jsonb` (the
  serialized `MessageEnvelope<T>`), `status text`
  (`pending` → `published` | `failed`), `created_at`, `published_at`.
- **Dedup**: `dedup_id text NOT NULL UNIQUE` — the JetStream `Nats-Msg-Id`.
  Since 0002 it is a SHA-256 digest of `(tenant_id, idempotency_key)` (§6.2);
  the raw `idempotency_key` and `tenant_id` columns (added in 0002) carry the
  business key itself, with partial unique indexes
  `message_outbox_tenant_idempotency_uq` (`WHERE tenant_id IS NOT NULL`) and
  `message_outbox_global_idempotency_uq` (`WHERE tenant_id IS NULL`) so the
  same business key is unique *within* a tenant, independent *across* tenants,
  and NULL tenant is a real (not NULL-magic) global namespace.
- **Retry machinery**: `attempts integer`, `available_at timestamptz`
  (exponential-backoff gate — a failed publish pushes it into the future),
  `last_error text` (durable operator-visible failure text).
- **Claim leases** (0002): `claim_owner uuid`, `claim_expires_at timestamptz` —
  which relay currently owns the row and until when (§6.3).
- Partial indexes `message_outbox_pending_idx` and
  `message_outbox_claimable_idx` keep the "claim due pending rows, oldest
  first" scan cheap once most rows are published.

### 5.2 The two inbox tables

- `message_inbox` (`message_id uuid PK`, `tenant_id`, `idempotency_key`,
  `received_at`, `processed_at`) — the message-id-keyed inbox: a message is
  consumed at most once **globally**. Used by `db::inbox_try_insert[_scoped]`
  / `db::inbox_mark_processed`. Migration 0002 tenant-scoped its business-key
  uniqueness the same way as the outbox.
- `message_inbox_consumer` (`PRIMARY KEY (consumer, message_id)`, plus
  `tenant_id`, `received_at`, `processed_at`) — the **per-consumer** inbox
  driven by `PgInbox` (`src/inbox.rs`): the same message can be independently,
  idempotently processed by several distinct consumers. A partial index on
  `processed_at` supports retention sweeps.

### 5.3 `message_outbox_compat`

The original service's bytea-envelope outbox (`envelope bytea`, non-optional
`tenant_id`, no `dedup_id`, no `status` column — "published" is
`published_at IS NOT NULL`). Only the `compat-service` code path
(`src/transactional.rs`, `src/bin/fiducia-messaging-compat.rs`) touches it;
the integrated relay never does. A schema-contract test in
`src/transactional.rs` (`compatibility_queries_match_the_canonical_migration`)
asserts every column the compat queries use exists in migration 0001, so
migration/code drift fails the build visibly.

## 6. Message flows and delivery semantics

### 6.1 Publish (producer side)

```
┌─ one DB transaction (the producer's) ──────────────────────┐
│  UPDATE / INSERT domain state …                            │
│  db::enqueue_outbox_tx(tx, OutboxRecord::from_envelope(…)) │  status='pending'
└────────────────────────────────────────────────────────────┘
                          │  (relay, separate process/loop)
                          ▼
     claim due pending rows (owner lease, FOR UPDATE SKIP LOCKED)
                          │
                          ▼
     Publisher::publish(subject, dedup_id, bytes)  ──▶  JetStream
     (NatsPublisher sets Nats-Msg-Id = dedup_id,
      then AWAITS the publish ack)
                          │
                          ▼
     mark_published(id, owner)   — only if owner still matches
```

Step by step, in code:

1. **Stage.** `OutboxRecord::from_envelope(id, subject, &envelope)`
   (`src/outbox.rs`) captures the envelope as jsonb, keeps the raw
   `idempotency_key` + `tenant_id` for the DB uniqueness constraint, and
   derives `dedup_id = tenant_scoped_dedup_id(tenant_id, idempotency_key)`.
   `db::enqueue_outbox_tx(tx, &rec)` inserts it **inside the caller's domain
   transaction** — the correct outbox usage; the pool-based `enqueue_outbox`
   exists only for one-off/manual enqueues and is explicitly flagged in a
   `RECONCILE` comment as *not* atomic with a domain change. Both validate
   first via `validate_for_publish` (canonical subject + ≤ 1 MiB serialized
   payload, `MAX_MESSAGE_BYTES` in `src/outbox.rs` matching the NATS default
   `max_payload`), so poison rows never enter the outbox. The insert is
   `ON CONFLICT DO NOTHING`: a repeated tenant-scoped business key is ignored,
   making enqueue itself idempotent.
2. **Relay.** Two interchangeable drainers share the table:
   - `outbox::Relay` — pure and transport-agnostic. You claim a batch
     yourself (e.g. `db::claim_pending_outbox` with your own owner UUID),
     call `relay.drain(&batch)`, get back
     `RelayOutcome { published, failed }`, and do the owner-conditioned marks
     yourself. It holds no state and touches no DB; a serialize/validation/
     transport failure on one row is recorded and the drain continues.
   - `db::OutboxPublisher` — DB-coupled. `publish_batch()` calls
     `claim_pending_outbox` (a single `UPDATE … FROM (SELECT … FOR UPDATE SKIP
     LOCKED)` that atomically increments `attempts`, stamps
     `claim_owner`/`claim_expires_at`, and **commits the lease before any
     network I/O**), then publishes each row *outside* any DB transaction. On
     the first transport failure it records error + backoff and stops the
     batch (don't hammer a down broker), calling `release_outbox_claims` so
     untouched rows are immediately reclaimable rather than waiting out the
     lease. Defaults: batch 100, 8 attempts before a row is parked `failed`,
     5-minute claim TTL; `run(interval)` drains forever.
3. **Re-validate at the boundary.** Both drainers re-run
   `validate_for_publish` per row before it touches the publisher, so a
   malformed subject (wildcard/injection) or oversize payload that reached the
   outbox by another path (e.g. staged before the guard existed) is failed —
   `OutboxPublisher` parks such deterministic defects as `failed` immediately
   instead of burning retry attempts — and never handed to NATS.
4. **Publish + ack.** `NatsPublisher::publish` (`src/publisher.rs`) sets the
   `Nats-Msg-Id` header to `dedup_id` and **awaits the JetStream publish
   acknowledgement** before returning, so a row is marked `published` only
   after the broker has durably stored the message.
5. **Mark.** `mark_published` / `mark_failed` / `reschedule_publish` all carry
   `WHERE id = $1 AND claim_owner = $2` — a stale worker whose lease expired
   and whose row was reclaimed cannot alter it (`src/db.rs`).

### 6.2 Deduplication: how "effectively once" is assembled

The transport is **at-least-once** (JetStream redelivery, plus the relay's
crash-window republish). Three layers turn that into effectively-once
*external effects*:

1. **JetStream publish dedup.** The `Nats-Msg-Id` header carries `dedup_id`;
   the server drops a duplicate publish within the stream's configured dedup
   window. `dedup_id` is not the raw business key but
   `tenant_scoped_dedup_id(tenant_id, idempotency_key)`
   (`src/outbox.rs`): SHA-256 over a versioned domain separator
   (`fiducia-messaging:nats-dedup:v1`), a `tenant`/`global` discriminator, and
   the length-prefixed key — fixed-size (67 chars, `v1-<hex>`), collision-safe
   against ambiguous concatenation, non-disclosing of the business key, and
   tenant-scoped so two tenants using the same key never collide. The
   in-memory `RecordingPublisher` mirrors exactly this dedup property so the
   retry guarantee is unit-tested without a server
   (`src/publisher.rs`, `duplicate_dedup_id_publishes_once` in
   `src/outbox.rs`).
2. **Durable outbox uniqueness.** The broker's dedup window is finite; the
   partial unique indexes on `(tenant_id, idempotency_key)` (migration 0002)
   prevent a completed business key from ever being re-*enqueued* after that
   window, and `ON CONFLICT DO NOTHING` makes the producer's insert idempotent.
3. **Consumer inbox.** Redelivery can still hand a consumer the same message
   twice. Before running the external effect, the consumer records the message
   and only proceeds if the record is new:
   - in-memory `outbox::Inbox::accept_for_tenant(tenant, key)` (offline
     core, keyed on the same tenant-scoped digest);
   - `db::inbox_try_insert_scoped` (`message_inbox`,
     `ON CONFLICT DO NOTHING`; `true` = first sighting) +
     `inbox_mark_processed`;
   - `PgInbox::begin(tx, consumer, &envelope)` (`src/inbox.rs`) —
     **inside the same transaction as the side effect**, so effect and claim
     commit atomically; a redelivery loses the `(consumer, message_id)`
     insert and returns `InboxDecision::Duplicate`. Per-consumer keying lets
     several consumers each process the same message exactly once.

On top of dedup sits **authority**: before any externally visible mutation the
handler calls `envelope.require_fencing_token()` and presents the token to
fiducia-node; a stale lease-holder is rejected there. Duplicates are stopped
by keys, stale actors by fencing — that combination is the crate's definition
of effectively-once.

### 6.3 Ordering, replay, failure handling

- **Ordering**: the relay claims and publishes `ORDER BY created_at` (oldest
  first), but there is no global ordering guarantee — backoff can reorder a
  failed row relative to newer ones, and JetStream consumers own their own
  ordering semantics. Consumers that need causality use the envelope's
  `correlation_id`/`causation_id` chain, not arrival order.
- **Crash replay (relay)**: published-but-not-marked rows are republished on
  restart; JetStream drops the duplicate on `dedup_id`. A crashed relay's
  claimed rows become reclaimable once `claim_expires_at` passes.
- **Transient publish failure**: `reschedule_publish` records `last_error` and
  defers `available_at` by `min(5 min, 1s · 2^min(attempts-1, 8))` — capped
  exponential backoff computed in SQL (`src/db.rs`).
- **Permanent failure**: after `max_attempts` (default 8) the row is parked as
  `status = 'failed'` with its `last_error` for operator attention; it is
  never silently dropped and never retried forever.
- **Expiry**: `expires_at` is advisory per-handler (`is_expired`), not
  enforced by the relay.

### 6.4 The compat path (`compat-service` feature)

`transactional::Outbox::enqueue(tx, subject, &compat_envelope)` writes the
encoded compat envelope (bytea) into `message_outbox_compat`;
`transactional::OutboxPublisher::publish_batch` claims with
`FOR UPDATE SKIP LOCKED` **inside one transaction held across the publishes**
(the original design, preserved verbatim), publishes over core NATS
(`nats.publish` + `flush()`, no JetStream, no `Nats-Msg-Id`), marks
`published_at`, and commits. Its subject guard
(`is_publishable_subject`) is deliberately looser than the canonical taxonomy
— any dot-token subject without wildcards/whitespace/control chars/empty
tokens passes, preserving the historical namespace — but it enforces the same
1 MiB ceiling, and a pre-existing invalid row is retained with backoff and
`last_error` for inspection rather than sent to NATS. This path exists only
for wire/API backward compatibility; new code uses the integrated path.

## 7. Public API surface

There is no HTTP API. The surface is (a) the **library API** re-exported at
the crate root (`src/lib.rs`) and (b) the **NATS subjects** of §4.

Key root exports: `MessageEnvelope`, `ENVELOPE_VERSION`, `MessagingError`,
`Subject`, `SubjectError`, `OutboxRecord`, `OutboxStatus`, `Relay`,
`RelayOutcome`, `Inbox` (in-memory), `InboxRecord`, `tenant_scoped_dedup_id`,
`validate_for_publish`, `MAX_MESSAGE_BYTES`, `Publisher`,
`RecordingPublisher`, `PublishedMessage`; behind `nats`: `NatsPublisher`;
behind `postgres`: the `db` module, `OutboxPublisher`, and
`PgInbox`/`InboxDecision`/`InboxError`; behind `compat-service`:
`CompatOutbox`/`CompatOutboxPublisher`. Name collisions from the two-lineage
merge are resolved by distinct root aliases (`PgInbox`, `Compat*`), documented
in `RECONCILE` comments in `src/lib.rs`.

Typical producer:

```rust
let env = MessageEnvelope::new("execution.requested", payload, "idem-42")
    .with_tenant(tenant_id)
    .with_execution(execution_id)
    .with_fencing_token(lease_token);
let rec = OutboxRecord::from_envelope(Uuid::new_v4(), subjects::EXECUTIONS_REQUESTED, &env)?;
db::enqueue_outbox_tx(&mut tx, &rec).await?;   // same tx as the domain change
tx.commit().await?;
```

Typical consumer (per-consumer inbox):

```rust
let mut tx = pool.begin().await?;
if inbox.begin(&mut tx, "execution-runner", &env).await? == InboxDecision::Process {
    let token = env.require_fencing_token()?;   // authority before effect
    run_side_effect(&mut tx, token).await?;
    inbox.mark_processed(&mut tx, "execution-runner", env.message_id).await?;
}
tx.commit().await?;                             // claim + effect atomically
```

## 8. Key invariants

1. **Identifiers in the envelope, never the subject.** Enforced by
   `Subject`'s token validation, including UUID rejection
   (`src/subjects.rs`).
2. **Fencing before effect.** Any handler mutating the outside world must
   pass `require_fencing_token()`; a missing token is a typed error, not a
   warning (`src/envelope.rs`, `src/error.rs`).
3. **Outbox row and domain change commit atomically** — use
   `enqueue_outbox_tx` / `PgInbox::begin` in the caller's transaction
   (`src/db.rs`, `src/inbox.rs`).
4. **Nothing malformed or oversize reaches NATS** — `validate_for_publish` on
   both enqueue and drain; deterministic defects park as `failed`
   (`src/outbox.rs`, `src/db.rs`).
5. **Idempotency is tenant-scoped.** The DB constrains the raw key per tenant
   (partial unique indexes, migration 0002); `Nats-Msg-Id` is a bounded digest
   of the same scope+key; `None` tenant is an explicit global namespace, not a
   wildcard (`src/outbox.rs`).
6. **Relay claims are durable and fenced by owner.** Lease committed before
   network I/O; every terminal update is `WHERE claim_owner = $2`; abandoned
   rows become claimable after lease expiry (`src/db.rs`).
7. **Publish is acked before the row is marked published**
   (`NatsPublisher::publish` awaits the JetStream ack, `src/publisher.rs`).
8. **The default build is offline and deterministic.** No NATS, no database
   driver, no
   network; the pure core takes clocks and ids as parameters so tests assert
   exact values (`Cargo.toml`, `src/outbox.rs`).
9. **All SQL is parameterized; connection URLs are never logged**
   (README "Security / hardening"; verified across `src/db.rs`,
   `src/transactional.rs`).
10. **Migrations are forward-only** — 0002 amends 0001 rather than editing it,
    because migration runners checksum applied migrations
    (`migrations/0002_tenant_dedup_and_claim_leases.sql`).

## 9. Operational notes

### Binaries and deployment

- **`fiducia-relay`** (`src/main.rs`; requires `--features postgres,nats`) —
  the only production deployable. On boot: connect Postgres (schema is
  applied declaratively out-of-band), connect NATS, wrap the JetStream context
  in `NatsPublisher`, then `OutboxPublisher::run(500ms)` forever. The
  `Dockerfile` builds exactly this binary (pinned `rust:1.97.0-slim-bookworm`
  by digest, `--locked --release`, stripped) into a digest-pinned distroless
  `cc-debian12:nonroot` image running as uid 65532.
- **`fiducia-messaging-compat`** (`src/bin/fiducia-messaging-compat.rs`;
  `--features compat-service`) — the legacy drain loop over
  `message_outbox_compat`, 250 ms interval. Deployed only where old-format
  producers still exist.

### Configuration (environment)

| var | binary | required | notes |
| --- | --- | --- | --- |
| `DATABASE_URL` | both | yes | Postgres URL; carries credentials — never logged, excluded from the flags schema. |
| `NATS_URL` | relay: no (default `nats://localhost:4222`); compat: yes | | may embed credentials. |
| `RELAY_BATCH` | relay | no | rows claimed per drain batch, default 100; unparseable falls back to default. |

Non-secret flags can be supplied on the CLI through the pinned
`vendor/flags-2-env` submodule: `scripts/with-flags2env.sh [flags…] -- cmd`
resolves flags to env vars via `.cli-flags.toml` (`relay-batch` →
`RELAY_BATCH`, `log` → `RUST_LOG`) and execs the command. `DATABASE_URL` and
`NATS_URL` are deliberately absent from that schema. The schema is audited in
CI (`.github/workflows/cli-flags.yml`).

### Migrations

Applied declaratively out-of-band by the deployment's migration tooling
(never by the binaries on boot), or manually: `db::apply_schema(&pool)`
executes the embedded files; everything is idempotent so re-running is safe.
Library consumers that manage their own migration set can vendor the
`migrations/` directory into their own migration chain.

### Testing and CI

- `cargo test --locked` — default, fully in-memory (RecordingPublisher mirrors
  JetStream dedup; the pure core is deterministic).
- `cargo test --locked --features postgres` — adds DB-free schema-contract
  checks (`embedded_schema_defines_both_tables` in `src/db.rs`, the compat
  contract test in `src/transactional.rs`); live-Postgres integration is out
  of scope for `cargo test`.
- CI (`.github/workflows/ci.yml`): Rust 1.95.0 pinned, flags2env contract
  audit, `cargo fmt --check`, all-features locked clippy with `-D warnings`,
  all-features locked tests, and `cargo audit` (one documented exception in
  `.cargo/audit.toml`: `rsa` reachable only as unreachable `sqlx-mysql`
  lock-file metadata).

### Sizing and tuning

- `MAX_MESSAGE_BYTES` = 1 MiB, matching the NATS server default `max_payload`;
  deployments with a smaller server limit still get the broker's rejection as
  backstop.
- `OutboxPublisher::with_batch_size / with_max_attempts / with_claim_ttl` —
  the claim TTL (default 300 s) must cover the worst-case time to publish a
  full batch, since an expired claim is deliberately reclaimable.
- JetStream stream config (not owned by this crate) must set a **publish
  dedup window** at least as long as the relay's worst-case
  crash-to-republish gap for layer 1 of §6.2 to hold; layers 2 and 3 hold
  regardless.
