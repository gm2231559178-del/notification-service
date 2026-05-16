-- migrations/0009_email_log_retry_fields.sql
--
-- Stores the per-event from_override and attachments alongside each
-- email_log row so that manual retries (POST /emails/:id/retry) can
-- re-publish the full original event instead of a stripped-down envelope.
--
-- Both columns are nullable for backwards-compatibility with rows written
-- before this migration; those rows will re-publish without From override
-- or attachments on retry (same behaviour as before).
--
-- from_override JSONB example:
--   { "email": "orders@acme.com", "name": "Acme Orders" }
--
-- attachments JSONB example:
--   [{ "url": "https://...", "filename": "inv.pdf", "content_type": "application/pdf" }]

ALTER TABLE email_log
    ADD COLUMN IF NOT EXISTS from_override JSONB,
    ADD COLUMN IF NOT EXISTS attachments   JSONB;
