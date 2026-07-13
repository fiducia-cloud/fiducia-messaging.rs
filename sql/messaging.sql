-- fiducia-messaging schema: the transactional outbox + inbox tables.
--
-- The outbox exists because a DB commit and a NATS publish cannot be one atomic
-- operation. Producers INSERT a `message_outbox` row inside the *same* Postgres
-- transaction that mutates domain state; a separate relay then reads pending
-- rows and publishes them. If the relay crashes between publish and mark, it
-- republishes on restart — JetStream collapses the duplicate on `dedup_id`.
--
-- The inbox gives consumers at-most-once external effects: a handler inserts the
-- incoming `message_id` (dedup key) before acting; a duplicate delivery loses
-- the INSERT and is skipped.
--
-- Idempotent: safe to run on every boot (see `db::apply_schema`).

CREATE TABLE IF NOT EXISTS message_outbox (
    id           uuid PRIMARY KEY,
    -- Routing class only, e.g. `fiducia.executions.completed.v1`. Identifiers
    -- live in the payload envelope, never in the subject.
    subject      text        NOT NULL,
    -- JetStream `Nats-Msg-Id` for publish dedup; unique so the same logical
    -- message is enqueued at most once.
    dedup_id     text        NOT NULL UNIQUE,
    -- The serialized `MessageEnvelope<T>`.
    payload      jsonb       NOT NULL,
    status       text        NOT NULL DEFAULT 'pending',
    attempts     integer     NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

-- Partial index so the relay's "claim pending, oldest first" scan stays cheap
-- once most rows are published.
CREATE INDEX IF NOT EXISTS message_outbox_pending_idx
    ON message_outbox (created_at)
    WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS message_inbox (
    message_id      uuid PRIMARY KEY,
    -- Business idempotency key from the envelope; unique so two distinct
    -- message_ids cannot claim the same effect.
    idempotency_key text        NOT NULL UNIQUE,
    received_at     timestamptz NOT NULL DEFAULT now(),
    processed_at    timestamptz
);
