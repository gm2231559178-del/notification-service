-- migrations/0010_create_email_template.sql
--
-- Stores email templates in the database so new event types can be added
-- without a code change or service redeployment.
--
-- The notification service loads templates at startup and caches them
-- in memory. The cache is refreshed on each consumer reconnect, and can
-- be force-refreshed by restarting the service.
--
-- Fallback behaviour: if the DB has no row for an event_type, the service
-- falls back to the compile-time templates in mailer::template::templates_for().
-- This keeps the existing ORDER_CONFIRMATION / PASSWORD_RESET / WELCOME
-- templates working with no data migration required.
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
