-- migrations/0014_email_log_sender_account.sql
--
-- Adds a `sender_account` column to `email_log` so the named SMTP account
-- used for the original delivery is preserved across manual retries.
--
-- Without this column, POST /emails/:id/retry reconstructs the event envelope
-- without a `sender_account` field, causing the consumer to fall back to the
-- global [mailer] default instead of the account originally selected.  This
-- would silently send the retry from the wrong From address.
--
-- NULL means the global [mailer] default was used (backwards compatible).

ALTER TABLE email_log
    ADD COLUMN IF NOT EXISTS sender_account TEXT;
