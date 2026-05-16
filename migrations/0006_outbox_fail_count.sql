-- migrations/0006_outbox_fail_count.sql
--
-- Adds fail_count to the outbox table in the BUSINESS SERVICE database.
--
-- The outbox worker increments this on every failed publish attempt and
-- permanently marks the row FAILED once fail_count reaches the threshold
-- (default 5, configurable in the worker).  Without this column, a
-- permanently broken event would be retried on every poll cycle forever.

ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS fail_count INT NOT NULL DEFAULT 0;

-- Partial index for monitoring/alerting on permanently failed outbox rows.
CREATE INDEX IF NOT EXISTS outbox_failed_idx
    ON outbox (created_at DESC)
    WHERE status = 'FAILED';
