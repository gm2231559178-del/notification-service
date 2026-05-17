-- migrations/0010_create_email_template.sql
--
-- Stores email templates in the database.  The template store is the
-- authoritative source: if no active row exists for an event_type the
-- consumer immediately marks the delivery FAILED (permanent error, no retry).
--
-- Operators add new event types by inserting a row here. No code change or
-- redeploy is required. The in-memory cache (default TTL 5 minutes) means
-- a new row is picked up within that window; call
-- DELETE /templates/<event_type>/cache for immediate effect.
--
-- Column notes:
--   type         — matches EmailEvent.event_type (e.g. 'ORDER_CONFIRMATION')
--   subject      — Handlebars-style {{variable}} template string
--   body_html    — HTML body template
--   body_text    — Plain-text body template (required; used as fallback by
--                  mail clients that don't render HTML)
--   version      — monotonically increasing integer; bump when editing a
--                  template so audit logs can reference which version sent
--   active       — set to FALSE to disable an event type without deleting it
--   created_at / updated_at — standard audit columns

CREATE TABLE IF NOT EXISTS email_template (
    type         TEXT        PRIMARY KEY,
    subject      TEXT        NOT NULL,
    body_html    TEXT        NOT NULL,
    body_text    TEXT        NOT NULL,
    version      INT         NOT NULL DEFAULT 1,
    active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed the three built-in templates so existing events continue to work
-- and operators have a concrete example to copy when adding new ones.
INSERT INTO email_template (type, subject, body_html, body_text) VALUES
(
    'ORDER_CONFIRMATION',
    'Order {{orderId}} confirmed',
    '<h1>Hi {{name}},</h1><p>Your order <strong>{{orderId}}</strong> of ${{amount}} has been confirmed.</p>',
    'Hi {{name}}, Your order {{orderId}} of ${{amount}} has been confirmed.'
),
(
    'PASSWORD_RESET',
    'Reset your password',
    '<p>Click <a href="{{resetLink}}">here</a> to reset your password.</p>',
    'Visit this link to reset your password: {{resetLink}}'
),
(
    'WELCOME',
    'Welcome to {{appName}}!',
    '<h1>Welcome, {{name}}!</h1><p>Thanks for joining {{appName}}.</p>',
    'Welcome, {{name}}! Thanks for joining {{appName}}.'
)
ON CONFLICT (type) DO NOTHING;
