-- migrations/0005_outbox_from_override.sql
--
-- No schema change is required — `from_override` is stored inside the existing
-- JSONB `payload` column of the outbox table in the BUSINESS SERVICE database.
--
-- This migration documents the updated payload contract and adds a partial
-- index that makes it cheap to query events that carry a From override.
--
-- Updated outbox payload contract (JSONB):
--
--   {
--     -- REQUIRED: at least one recipient
--     "recipients": [
--       { "email": "user@example.com", "name": "Alice" }
--     ],
--
--     -- Legacy single-recipient form (still accepted, auto-promoted):
--     -- "recipient": { "email": "user@example.com", "name": "Alice" },
--
--     -- REQUIRED: template variables
--     "payload": { "orderId": "123", "amount": 99.90 },
--
--     -- OPTIONAL: override the From address for this specific event.
--     -- When omitted, the globally configured mailer.from_email is used.
--     "from_override": {
--       "email": "orders@acme.com",   -- required
--       "name":  "Acme Orders"        -- optional; falls back to mailer.from_name
--     },
--
--     -- OPTIONAL
--     "metadata": { "source": "order-service" }
--   }
--
-- Partial index for auditing / monitoring events that carry a From override.
-- Run this on the BUSINESS SERVICE database.

CREATE INDEX IF NOT EXISTS outbox_from_override_idx
    ON outbox ((payload->>'from_override'))
    WHERE payload ? 'from_override';
