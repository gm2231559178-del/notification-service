-- migrations/0001_create_email_log.sql

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS email_log (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id     UUID        NOT NULL UNIQUE,
    event_type   TEXT        NOT NULL,
    recipient    TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'PENDING'
                             CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    retry_count  INT         NOT NULL DEFAULT 0,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS email_log_status_idx    ON email_log (status);
CREATE INDEX IF NOT EXISTS email_log_created_at_idx ON email_log (created_at DESC);
