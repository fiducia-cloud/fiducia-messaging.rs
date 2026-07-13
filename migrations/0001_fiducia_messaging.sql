-- fiducia-messaging schema: the transactional outbox + the two inbox shapes.
--
-- This migration is the UNIFIED schema for the merged crate. It folds together:
--   * MINE  — sql/messaging.sql: message_outbox (jsonb payload + dedup_id +
--     status) and the message-id-keyed message_inbox.
--   * CODEX — migrations/0001_outbox_inbox.sql: outbox backoff columns
--     (available_at, last_error) and the per-consumer inbox
--     (PRIMARY KEY (consumer, message_id)).
--
-- The outbox exists because a DB commit and a NATS publish cannot be one atomic
-- operation. Producers INSERT a `message_outbox` row inside the *same* Postgres
-- transaction that mutates domain state (see `db::enqueue_outbox_tx`); a relay
-- (`outbox::Relay` or `db::OutboxPublisher`) then drains pending rows and
-- publishes them. If the relay crashes between publish and mark, it republishes
-- on restart — JetStream collapses the duplicate on `dedup_id`.
--
-- Idempotent (CREATE ... IF NOT EXISTS), so it doubles as the body embedded by
-- `db::apply_schema` and as a sqlx migration run by `sqlx::migrate!`.

CREATE TABLE IF NOT EXISTS message_outbox (
    id           uuid PRIMARY KEY,
    -- Routing class only, e.g. `fiducia.executions.completed.v1`. Identifiers
    -- live in the payload envelope, never in the subject.
    subject      text        NOT NULL CHECK (length(trim(subject)) > 0),
    -- JetStream `Nats-Msg-Id` for publish dedup; unique so the same logical
    -- message is enqueued at most once.
    dedup_id     text        NOT NULL UNIQUE,
    -- The serialized `MessageEnvelope<T>`.
    payload      jsonb       NOT NULL,
    status       text        NOT NULL DEFAULT 'pending',
    attempts     integer     NOT NULL DEFAULT 0,
    -- CODEX: backoff gate — a failed publish pushes this into the future so the
    -- relay skips the row until it is due (see `db::OutboxPublisher`).
    available_at timestamptz NOT NULL DEFAULT now(),
    -- CODEX: durable retry metadata — the text of the last publish failure.
    last_error   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

-- Partial index so the relay's "claim pending, due, oldest first" scan stays
-- cheap once most rows are published.
CREATE INDEX IF NOT EXISTS message_outbox_pending_idx
    ON message_outbox (available_at, created_at)
    WHERE status = 'pending';

-- MINE: message-id-keyed inbox — a message is consumed at most once globally.
-- Used by `db::inbox_try_insert` / `db::inbox_mark_processed`.
CREATE TABLE IF NOT EXISTS message_inbox (
    message_id      uuid PRIMARY KEY,
    -- Business idempotency key from the envelope; unique so two distinct
    -- message_ids cannot claim the same effect.
    idempotency_key text        NOT NULL UNIQUE,
    received_at     timestamptz NOT NULL DEFAULT now(),
    processed_at    timestamptz
);

-- CODEX: per-consumer inbox — each consumer claims a given message once, so the
-- same message can be independently (and idempotently) processed by several
-- consumers. Used by the `inbox::Inbox` (`PgInbox`) begin/mark_processed pair.
-- `tenant_id` is nullable here to accept `MessageEnvelope`'s optional tenant.
CREATE TABLE IF NOT EXISTS message_inbox_consumer (
    consumer     text        NOT NULL CHECK (length(trim(consumer)) > 0),
    message_id   uuid        NOT NULL,
    tenant_id    uuid,
    received_at  timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    PRIMARY KEY (consumer, message_id)
);

CREATE INDEX IF NOT EXISTS message_inbox_consumer_retention_idx
    ON message_inbox_consumer (processed_at)
    WHERE processed_at IS NOT NULL;

-- ---------------------------------------------------------------------------
-- compat-service table: the codex-original outbox schema, kept separate from
-- the integrated `message_outbox` above so the `compat-service` feature
-- (src/transactional.rs) runs verbatim against its own table. Only created/used
-- when the compat launcher is deployed; the integrated relay never touches it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS message_outbox_compat (
    message_id   uuid PRIMARY KEY,
    tenant_id    uuid NOT NULL,
    subject      text NOT NULL CHECK (length(trim(subject)) > 0),
    envelope     bytea NOT NULL,
    attempts     integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    last_error   text,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS message_outbox_compat_pending_idx
    ON message_outbox_compat (available_at, created_at) WHERE published_at IS NULL;
