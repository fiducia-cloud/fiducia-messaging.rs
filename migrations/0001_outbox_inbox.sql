CREATE TABLE IF NOT EXISTS message_outbox (
 message_id uuid PRIMARY KEY, tenant_id uuid NOT NULL, subject text NOT NULL CHECK (length(trim(subject)) > 0),
 envelope bytea NOT NULL, attempts integer NOT NULL DEFAULT 0, available_at timestamptz NOT NULL DEFAULT now(),
 published_at timestamptz, last_error text, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS message_outbox_pending_idx ON message_outbox (available_at, created_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS message_inbox (
 consumer text NOT NULL CHECK (length(trim(consumer)) > 0), message_id uuid NOT NULL, tenant_id uuid NOT NULL,
 received_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz,
 PRIMARY KEY (consumer, message_id)
);
CREATE INDEX IF NOT EXISTS message_inbox_retention_idx ON message_inbox (processed_at) WHERE processed_at IS NOT NULL;
