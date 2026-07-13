# fiducia-messaging

A **library over NATS, not a broker.** The fiducia.cloud platform already runs
on NATS JetStream (delivery), [`fiducia-node`](https://github.com/fiducia-cloud/fiducia-node.rs)
(coordination/authority), and Postgres (state). This crate is the thin glue the
three share so every service speaks the bus the same way:

- a standard **message envelope**,
- the transactional **outbox/inbox** pattern,
- and a **subject taxonomy**.

It deliberately implements no queueing, persistence, or routing of its own —
JetStream does that better than we would. The default build pulls in no NATS
client and no database driver and needs no network: `cargo test` runs entirely
in-memory.

## The core principle

> Messages say something *happened* or *request* work; fiducia-node decides who
> is authorized to *act*.

A message is never a trusted instruction on arrival. Two envelope fields carry
the rule:

| field | purpose |
| --- | --- |
| `idempotency_key` | Business key for the effect this message drives. A redelivery collapses to a single external effect (**at-most-once**). |
| `fencing_token` | Monotonic authority token from fiducia-node (a lock/lease). A handler about to mutate the outside world must present it; a stale holder's token is rejected. |

Over an at-least-once transport these give **effectively-once** external
effects: the fencing token stops a *stale* actor from acting, the idempotency
key stops a *duplicate* from acting twice. Call
`envelope.require_fencing_token()` before any external mutation — it returns a
typed `MissingFencingToken` error rather than letting an unauthorized effect
through.

## The message envelope

`MessageEnvelope<T>` wraps a typed `payload` in identical metadata for every
message: ids (`message_id`, `correlation_id`, `causation_id`), scope
(`tenant_id`, `workflow_id`, `execution_id`), authority (`idempotency_key`,
`fencing_token`), lifecycle (`created_at`, `expires_at`), tracing
(`trace_parent`), provenance (`source`), and two orthogonal versions:
`envelope_version` (the wire-format framing, checked against `ENVELOPE_VERSION`)
and `schema_version` (the typed payload). `envelope.validate()` rejects an
unknown `envelope_version` or a blank identity; `encode()` / `decode()` are the
validating serialize/deserialize pair (`to_vec` skips validation).

```rust
use fiducia_messaging::MessageEnvelope;

let env = MessageEnvelope::new("execution.requested", payload, "idem-42")
    .with_tenant(tenant_id)
    .with_execution(execution_id)
    .with_fencing_token(lease_token); // required to authorize the run
```

For tests, use `MessageEnvelope::new_at(now, message_id, ..)` so ids and
timestamps are deterministic — the convenience `new` calls `Utc::now()` /
`Uuid::new_v4()` and is not asserted on.

## The outbox/inbox pattern

**Why it exists:** a Postgres `COMMIT` and a NATS publish cannot be one atomic
operation. If you publish then commit, a crash in between loses the DB change; if
you commit then publish, a crash loses the message. Neither ordering is safe.

**The fix:** write the message as a row in the *same* transaction as the domain
change, then relay it out-of-band.

```
┌─ one DB transaction ─────────────┐
│  UPDATE domain state …           │
│  INSERT INTO message_outbox …    │   <- OutboxRecord (status = pending)
└──────────────────────────────────┘
                 │
        relay (separate process)
                 ▼
   claim pending rows ─▶ Publisher.publish(subject, dedup_id, bytes) ─▶ JetStream
                 │
        mark rows published
```

If the relay crashes after publishing but before marking a row, it republishes
on restart — and JetStream drops the duplicate because the row's `dedup_id` is
sent as the `Nats-Msg-Id` header (server-side publish dedup). The in-memory
`RecordingPublisher` mirrors that dedup so the same guarantee is unit-tested.

There are **two publishers**, sharing one `message_outbox` table:

- `outbox::Relay` — pure and transport-agnostic. Hand it a claimed batch and a
  `&dyn Publisher` and it returns a `RelayOutcome { published, failed }`; it
  holds no state and touches no DB, so a serialize/transport failure on one row
  is recorded and the drain continues. You own the claim/mark DB calls.
- `db::OutboxPublisher` (feature `postgres`) — DB-coupled. `publish_batch()`
  claims a bounded batch of *due* pending rows with `FOR UPDATE SKIP LOCKED`,
  publishes each through the `Publisher`, and records the outcome in one
  transaction with **exponential backoff** (`available_at`), **durable retry
  metadata** (`attempts`, `last_error`), and a `failed` park once attempts are
  exhausted. `NatsPublisher` awaits the JetStream publish-ack before returning,
  so a row is marked `published` only after the broker durably stored it
  (NATS-flush-before-mark). `run(interval)` drains forever.

**Consumers** get the mirror image, and there are likewise **two inboxes**:

- `outbox::Inbox` — in-memory guard (part of the offline core): `accept(key)`
  returns `false` for a duplicate. `db::inbox_try_insert` is its Postgres
  message-id-keyed equivalent (`message_inbox`) — a message is consumed once
  *globally*.
- `PgInbox` (feature `postgres`, from the `inbox` module) — a Postgres
  **per-consumer** claim (`message_inbox_consumer`, `PRIMARY KEY (consumer,
  message_id)`). Inside the same transaction as its side effect a consumer calls
  `Inbox::begin(tx, consumer, &envelope)` → `InboxDecision::{Process, Duplicate}`
  and, on success, `mark_processed(...)`. Because the claim is per-consumer, the
  same message can be independently, idempotently processed by several
  consumers, each obtaining effective exactly-once side effects.

Either way: record the incoming message before running the effect; a duplicate
delivery loses the insert and is skipped, so the external effect runs at most
once.

## Subject taxonomy

Subjects are **routing classes**, composed as `fiducia.<group>.<event>.v<version>`:

```
fiducia.work-items.created.v1
fiducia.executions.{requested,progress,completed}.v1
fiducia.reviews.{requested,findings}.v1
fiducia.tests.{requested,completed}.v1
fiducia.runners.{heartbeat,commands}.v1
fiducia.github.events.v1
fiducia.jira.events.v1
```

The rule the taxonomy encodes: **identifiers go in the envelope, not the
subject.** A subject names a *kind* of message so consumers can subscribe with
wildcards (`fiducia.executions.*.v1`). Baking a work-item or tenant id into the
subject explodes the subject space and defeats those subscriptions. The `Subject`
builder validates tokens (lowercase `[a-z0-9-]`, no wildcards, no dots) and
rejects any token that parses as a UUID — an identifier leaking into a routing
class.

```rust
use fiducia_messaging::Subject;

let subject = Subject::new("executions", "completed", 1)?; // fiducia.executions.completed.v1
```

## Feature flags

Both are **off by default**; the crate builds and tests with neither.

| feature | adds |
| --- | --- |
| `postgres` | `db` module — sqlx-backed outbox/inbox repo (`apply_schema`, `enqueue_outbox`, `enqueue_outbox_tx`, `claim_pending_outbox`, `mark_published`, `inbox_try_insert`, …) plus `OutboxPublisher`; and the `inbox` module — the per-consumer `PgInbox`. Runtime-checked queries, so no `DATABASE_URL` at build. Schema lives in [`migrations/`](migrations/) (embedded as `db::SCHEMA_SQL`; run the dir with `sqlx::migrate!` or `db::apply_schema`). |
| `nats` | `NatsPublisher` — a real JetStream `Publisher` that sets `Nats-Msg-Id` for publish dedup. |

The `fiducia-relay` binary (a thin outbox→JetStream drain loop) is built with
`--features postgres,nats`; without them it prints a usage note.

```sh
cargo test                                  # default, in-memory, no services
cargo test --features postgres              # + DB-free schema checks
cargo run --bin fiducia-relay --features postgres,nats
```

## Configuration

The `fiducia-relay` binary (feature `postgres,nats`) reads its whole config from
the environment (`src/main.rs`):

| var | required | secret | description |
| --- | --- | --- | --- |
| `DATABASE_URL` | yes | **yes** | Postgres connection string, e.g. `postgres://user:pass@host/db`. **Carries DB credentials** — never log it, keep it out of shell history and CI logs. The relay exits if it is unset. |
| `NATS_URL` | no | no | NATS server URL. Defaults to `nats://localhost:4222`. May embed credentials if your deployment uses `nats://user:pass@host`; treat as secret then. |
| `RELAY_BATCH` | no | no | Integer — outbox rows claimed per drain batch. Defaults to `100`; unparseable values fall back to the default. |

The `fiducia-messaging-compat` binary (feature `compat-service`) reads
`DATABASE_URL` and `NATS_URL` with the same meaning (both required there).

### flags-2-env

Config can be driven from CLI flags instead of raw env vars, via the pinned
[`ORESoftware/flags-2-env`](https://github.com/ORESoftware/flags-2-env) parser
and the [`.cli-flags.toml`](.cli-flags.toml) schema, which maps each flag to the
env var above. `scripts/with-flags2env.sh` runs the parser, exports the env map,
then execs the command:

```bash
git submodule update --init --recursive
make -C vendor/flags-2-env all
scripts/with-flags2env.sh --database-url=postgres://user:pass@host/db --nats-url=nats://localhost:4222 --relay-batch=100 -- cargo run --bin fiducia-relay --features postgres,nats
```

The schema is audited in CI ([`.github/workflows/cli-flags.yml`](.github/workflows/cli-flags.yml)).

## Security / hardening

- **All SQL is parameterized.** Every query binds values (`$1`, `$2`, …); no
  SQL is built by string concatenation. The `format!` calls in `src/subjects.rs`
  build NATS subject strings, not SQL.
- **`DATABASE_URL` carries credentials and is never logged.** The relay prints
  only the (credential-free) `NATS_URL` target on startup; the DB URL is passed
  straight to the pool.

### Accepted advisories

`cargo audit` reports the following, which are **accepted** and must not be
force-fixed:

- **`rsa` RUSTSEC-2023-0071** (Marvin timing sidechannel) — transitive
  dependency with **no upstream fix available**. Reachable only under the `nats`
  feature.
- **`rustls-webpki` advisories** (RUSTSEC-2026-0049 / 0098 / 0099 / 0104) —
  pulled in **only** by `async-nats 0.38` behind the optional `nats` feature,
  which connects to a **trusted, operator-controlled NATS server**, not
  attacker-supplied certificates. Fixing requires a **breaking `async-nats` major
  bump**; the upgrade is tracked for the next `async-nats` major. The default,
  network-free build pulls in none of this.

## License

MIT
