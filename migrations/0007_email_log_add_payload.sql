-- migrations/0007_email_log_add_payload.sql
--
-- Stores the original template payload alongside each email_log row so that
-- manual retries (POST /emails/:id/retry) can re-publish the event with the
-- correct template variables instead of an empty payload {}.
--
-- The column is nullable for backwards compatibility with rows written before
-- this migration; those rows will simply re-publish with an empty payload if
-- retried (same behaviour as before).

ALTER TABLE email_log
    ADD COLUMN IF NOT EXISTS payload JSONB;
