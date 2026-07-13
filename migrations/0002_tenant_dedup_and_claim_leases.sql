-- Tenant-scoped idempotency and durable relay claims.
--
-- This is intentionally forward-only instead of editing migration 0001: SQLx
-- records migration checksums, and changing an applied migration would strand
-- existing deployments.

ALTER TABLE message_outbox
    ADD COLUMN IF NOT EXISTS tenant_id uuid,
    ADD COLUMN IF NOT EXISTS idempotency_key text,
    ADD COLUMN IF NOT EXISTS claim_owner uuid,
    ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz;

-- Rows created before tenant-scoped keys used the raw business key directly as
-- dedup_id. Preserve that value as their global-scope idempotency key; new rows
-- use the v1 tenant-scoped digest for dedup_id.
UPDATE message_outbox
   SET idempotency_key = dedup_id
 WHERE idempotency_key IS NULL;

ALTER TABLE message_outbox
    ALTER COLUMN idempotency_key SET NOT NULL;

-- NULL tenant_id denotes the explicit global namespace. Partial indexes give
-- it normal NULL-equals-NULL uniqueness while allowing the same business key in
-- different non-NULL tenants.
CREATE UNIQUE INDEX IF NOT EXISTS message_outbox_tenant_idempotency_uq
    ON message_outbox (tenant_id, idempotency_key)
    WHERE tenant_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS message_outbox_global_idempotency_uq
    ON message_outbox (idempotency_key)
    WHERE tenant_id IS NULL;

CREATE INDEX IF NOT EXISTS message_outbox_claimable_idx
    ON message_outbox (available_at, claim_expires_at, created_at)
    WHERE status = 'pending';

-- Scope the legacy message_inbox business key too. message_id remains the
-- globally unique delivery identity; this constraint prevents one tenant's
-- business key from blocking another tenant.
ALTER TABLE message_inbox
    ADD COLUMN IF NOT EXISTS tenant_id uuid;

ALTER TABLE message_inbox
    DROP CONSTRAINT IF EXISTS message_inbox_idempotency_key_key;

CREATE UNIQUE INDEX IF NOT EXISTS message_inbox_tenant_idempotency_uq
    ON message_inbox (tenant_id, idempotency_key)
    WHERE tenant_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS message_inbox_global_idempotency_uq
    ON message_inbox (idempotency_key)
    WHERE tenant_id IS NULL;
