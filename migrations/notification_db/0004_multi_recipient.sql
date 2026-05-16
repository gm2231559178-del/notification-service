-- migrations/0004_multi_recipient.sql
--
-- Converts email_log from single-recipient (UNIQUE event_id) to
-- multi-recipient (UNIQUE (event_id, recipient_email)).
--
-- Each event can now have one row per recipient, independently tracked.
-- Existing single-recipient rows are unaffected.

-- 1. Rename column FIRST (was "recipient" in 0001, already
--    "recipient_email" if you ran the v3 schema fresh).
--    Guard with DO $$ to be safe for both upgrade paths.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'email_log' AND column_name = 'recipient'
    ) THEN
        ALTER TABLE email_log RENAME COLUMN recipient TO recipient_email;
    END IF;
END$$;

-- 2. Drop the old single-column unique constraint.
ALTER TABLE email_log DROP CONSTRAINT IF EXISTS email_log_event_id_key;

-- 3. Add the composite unique constraint.
--    This is also the idempotency key: ON CONFLICT (event_id, recipient_email).
ALTER TABLE email_log
    ADD CONSTRAINT email_log_event_recipient_key
    UNIQUE (event_id, recipient_email);

-- 4. Index to support "all rows for an event" queries efficiently.
CREATE INDEX IF NOT EXISTS email_log_event_id_idx ON email_log (event_id);