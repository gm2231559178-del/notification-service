-- migrations/0013_email_log_total_attempts.sql
--
-- Adds a `total_attempts` counter that is never reset, even when an operator
-- triggers a manual retry via POST /emails/:id/retry (which resets `retry_count`
-- back to 0 to give the recipient a fresh set of automatic retries).
--
-- This separation makes two distinct concepts explicit:
--   retry_count    — how many automatic retries remain in the CURRENT attempt
--                    window; reset to 0 on manual retry so the full retry
--                    budget is available again.
--   total_attempts — lifetime delivery attempt counter; never decremented,
--                    useful for auditing and detecting persistently failing
--                    addresses.
--
-- Backfill: existing rows get total_attempts = retry_count (best approximation
-- from available data; rows that were manually retried will under-count, but
-- that is acceptable for historical data).

ALTER TABLE email_log
    ADD COLUMN IF NOT EXISTS total_attempts INT NOT NULL DEFAULT 0;

-- Backfill historical rows with their current retry_count as a lower bound.
UPDATE email_log SET total_attempts = retry_count WHERE total_attempts = 0;
