-- Retention support. The outbox/inbox tables grow without bound — the classic
-- transactional-outbox operational failure: published/processed rows are dead
-- weight that slowly bloats the table, its indexes, and every backup. The
-- `db::purge_*` helpers delete terminal rows past an age cutoff; these partial
-- indexes make those time-bounded DELETEs index scans instead of full-table
-- scans on the hot messaging tables.
--
-- Forward-only (0001/0002 are already applied as tracked migrations; never
-- edit an applied one). Idempotent, so `db::apply_schema` can also run it
-- directly, in line with the declarative out-of-band schema apply.

CREATE INDEX IF NOT EXISTS message_outbox_published_retention_idx
    ON message_outbox (published_at)
    WHERE status = 'published';

CREATE INDEX IF NOT EXISTS message_inbox_retention_idx
    ON message_inbox (processed_at)
    WHERE processed_at IS NOT NULL;

-- (message_inbox_consumer already has message_inbox_consumer_retention_idx
-- from 0001.)
