-- migrations/0003_add_blocked_status.sql
--
-- Adds BLOCKED as a valid email_log status (recipient on block/allow-list).
-- BLOCKED rows are terminal: ACK'd, not DLQ'd, never retried.

-- Drop the old CHECK constraint and recreate it with BLOCKED included.
ALTER TABLE email_log
    DROP CONSTRAINT IF EXISTS email_log_status_check;

ALTER TABLE email_log
    ADD CONSTRAINT email_log_status_check
    CHECK (status IN ('PENDING', 'SENT', 'FAILED', 'BLOCKED'));

-- Index to make "how many blocked this week?" queries fast.
CREATE INDEX IF NOT EXISTS email_log_blocked_idx
    ON email_log (created_at DESC)
    WHERE status = 'BLOCKED';
