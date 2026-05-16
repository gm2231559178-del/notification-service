-- migrations/0002_create_outbox.sql
-- Run this migration in the BUSINESS SERVICE database, not the
-- notification-service database.
--
-- The outbox worker polls this table and forwards PENDING rows to RabbitMQ.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS outbox (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Stable ID forwarded as EmailEvent.event_id — used for idempotency
    -- in the notification service.
    event_id     UUID        NOT NULL UNIQUE DEFAULT gen_random_uuid(),

    -- Logical event type, e.g. 'ORDER_CONFIRMATION', 'PASSWORD_RESET'.
    event_type   TEXT        NOT NULL,

    -- Full event body. Must contain at least:
    --   { "recipients": [{ "email": "...", "name": "..." }],
    --     "payload":    { <template variables> } }
    --
    -- Legacy single-recipient form is also accepted (auto-promoted by outbox worker):
    --   { "recipient": { "email": "...", "name": "..." }, "payload": {...} }
    --
    -- Optional per-event From address override:
    --   { ..., "from_override": { "email": "orders@acme.com", "name": "Acme Orders" } }
    payload      JSONB       NOT NULL,

    status       TEXT        NOT NULL DEFAULT 'PENDING'
                             CHECK (status IN ('PENDING', 'IN_PROGRESS', 'PUBLISHED', 'FAILED')),

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- FOR UPDATE SKIP LOCKED requires an index on (status, created_at).
CREATE INDEX IF NOT EXISTS outbox_status_created_idx
    ON outbox (status, created_at ASC)
    WHERE status = 'PENDING';
