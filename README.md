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
`fencing_token`), lifecycle (`created_at`, `expires_at`), and tracing
(`trace_parent`, `schema_version`).

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

`Relay` is pure: hand it a claimed batch and a `&dyn Publisher` and it returns a
`RelayOutcome { published, failed }`; it holds no state and touches no DB, so a
serialize/transport failure on one row is recorded and the drain continues.

**Consumers** get the mirror image via `Inbox` (in-memory) or `message_inbox`
(`db::inbox_try_insert`): record the incoming `message_id` before running the
effect; a duplicate delivery loses the insert and is skipped, so the external
effect runs at most once.

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
| `postgres` | `db` module — sqlx-backed outbox/inbox repo (`apply_schema`, `enqueue_outbox`, `claim_pending_outbox`, `mark_published`, `inbox_try_insert`, …). Runtime-checked queries, so no `DATABASE_URL` at build. Schema in [`sql/messaging.sql`](sql/messaging.sql). |
| `nats` | `NatsPublisher` — a real JetStream `Publisher` that sets `Nats-Msg-Id` for publish dedup. |

The `fiducia-relay` binary (a thin outbox→JetStream drain loop) is built with
`--features postgres,nats`; without them it prints a usage note.

```sh
cargo test                                  # default, in-memory, no services
cargo test --features postgres              # + DB-free schema checks
cargo run --bin fiducia-relay --features postgres,nats
```

## Compatibility service preserved from `fiducia-messaging`

The original non-suffixed repository is merged into this history. Its versioned,
tenant-aware envelope remains exported as `Envelope`, alongside the richer
`MessageEnvelope`. Its transaction-scoped PostgreSQL `Outbox`,
`TransactionalInbox`, and bounded core-NATS publisher are available with the
`compat-service` feature:

```sh
cargo test --all-features
cargo run --bin fiducia-messaging-compat --features compat-service
```

The compatibility worker uses `FOR UPDATE SKIP LOCKED`, durable retry metadata,
exponential backoff, and a NATS flush before marking delivery. Set
`DATABASE_URL` and `NATS_URL`; apply the bundled migration in each service
database. Delivery remains at least once, with effectively-once side effects
when consumers claim and mark inbox messages in the same transaction as their
domain changes.

The distroless image contains both `fiducia-relay` and
`fiducia-messaging-compat`; the relay remains the default entrypoint.

## License

MIT
