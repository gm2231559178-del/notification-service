-- migrations/business_db/0012_send_notification_fn.sql
--
-- Installs notify_send_email(), a convenience wrapper that enqueues an email
-- event into the outbox table from inside any business-service transaction.
--
-- Usage (single recipient):
--
--   SELECT notify_send_email(
--     p_event_type   => 'ORDER_CONFIRMATION',
--     p_recipient    => '{"email":"alice@example.com","name":"Alice"}'::jsonb,
--     p_payload      => '{"orderId":"123","amount":"99.00"}'::jsonb
--   );
--
-- Usage (multiple recipients):
--
--   SELECT notify_send_email(
--     p_event_type   => 'TEAM_INVITE',
--     p_recipients   => '[
--       {"email":"alice@example.com","name":"Alice"},
--       {"email":"bob@example.com","name":"Bob"}
--     ]'::jsonb,
--     p_payload      => '{"teamName":"Acme Engineering"}'::jsonb
--   );
--
-- Usage (with From override and attachment):
--
--   SELECT notify_send_email(
--     p_event_type    => 'INVOICE_READY',
--     p_recipient     => '{"email":"alice@example.com"}'::jsonb,
--     p_payload       => '{"invoiceId":"INV-42"}'::jsonb,
--     p_from_override => '{"email":"billing@acme.com","name":"Acme Billing"}'::jsonb,
--     p_attachments   => '[{
--       "url":          "https://storage.example.com/invoices/inv-42.pdf?token=xyz",
--       "filename":     "invoice-42.pdf",
--       "content_type": "application/pdf",
--       "max_age_secs": 300
--     }]'::jsonb,
--     p_metadata      => '{"source":"billing-service"}'::jsonb
--   );
--
-- The function validates its inputs and raises an exception (SQLSTATE P0001)
-- on any structural error so the calling transaction is rolled back cleanly
-- instead of silently inserting a broken row.
--
-- Prerequisite: run 0002_create_outbox.sql, 0005_outbox_from_override.sql,
--               0006_outbox_fail_count.sql, and 0008_outbox_attachments.sql first.

-- ── Helper: validate a single recipient object ────────────────────────────────

CREATE OR REPLACE FUNCTION _notify_validate_recipient(p_r jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_email text;
    v_local text;
    v_domain text;
BEGIN
    IF p_r IS NULL OR jsonb_typeof(p_r) <> 'object' THEN
        RAISE EXCEPTION 'notify_send_email: each recipient must be a JSON object, got: %', p_r
            USING ERRCODE = 'P0001';
    END IF;

    v_email := p_r->>'email';

    IF v_email IS NULL OR v_email = '' THEN
        RAISE EXCEPTION 'notify_send_email: recipient missing required "email" field'
            USING ERRCODE = 'P0001';
    END IF;

    -- Basic structural check mirrors common::is_valid_email:
    --   total ≤ 254 chars, exactly one @, non-empty local and domain parts.
    IF length(v_email) > 254 THEN
        RAISE EXCEPTION 'notify_send_email: recipient email too long (> 254 chars): %', v_email
            USING ERRCODE = 'P0001';
    END IF;

    IF (length(v_email) - length(replace(v_email, '@', ''))) <> 1 THEN
        RAISE EXCEPTION 'notify_send_email: recipient email must contain exactly one "@": %', v_email
            USING ERRCODE = 'P0001';
    END IF;

    v_local  := split_part(v_email, '@', 1);
    v_domain := split_part(v_email, '@', 2);

    IF v_local  = '' THEN
        RAISE EXCEPTION 'notify_send_email: recipient email has empty local part: %', v_email
            USING ERRCODE = 'P0001';
    END IF;
    IF v_domain = '' THEN
        RAISE EXCEPTION 'notify_send_email: recipient email has empty domain part: %', v_email
            USING ERRCODE = 'P0001';
    END IF;
END;
$$;

-- ── Helper: validate the attachments array ────────────────────────────────────

CREATE OR REPLACE FUNCTION _notify_validate_attachments(p_atts jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_att  jsonb;
    v_idx  int := 0;
    v_url  text;
    v_fn   text;
    v_ct   text;
BEGIN
    IF p_atts IS NULL THEN
        RETURN;
    END IF;

    IF jsonb_typeof(p_atts) <> 'array' THEN
        RAISE EXCEPTION 'notify_send_email: p_attachments must be a JSON array'
            USING ERRCODE = 'P0001';
    END IF;

    FOR v_att IN SELECT jsonb_array_elements(p_atts) LOOP
        v_idx := v_idx + 1;

        v_url := v_att->>'url';
        v_fn  := v_att->>'filename';
        v_ct  := v_att->>'content_type';

        IF v_url IS NULL OR v_url = '' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] missing required "url"', v_idx
                USING ERRCODE = 'P0001';
        END IF;
        IF v_url NOT LIKE 'http://%' AND v_url NOT LIKE 'https://%' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] url must start with http:// or https://, got: %', v_idx, v_url
                USING ERRCODE = 'P0001';
        END IF;

        IF v_fn IS NULL OR v_fn = '' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] missing required "filename"', v_idx
                USING ERRCODE = 'P0001';
        END IF;
        IF v_fn LIKE '%/%' OR v_fn LIKE '%\%' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] filename must not contain path separators, got: %', v_idx, v_fn
                USING ERRCODE = 'P0001';
        END IF;

        IF v_ct IS NULL OR v_ct = '' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] missing required "content_type"', v_idx
                USING ERRCODE = 'P0001';
        END IF;
        IF v_ct NOT LIKE '%/%' THEN
            RAISE EXCEPTION 'notify_send_email: attachment[%] content_type must be a valid MIME type (e.g. "application/pdf"), got: %', v_idx, v_ct
                USING ERRCODE = 'P0001';
        END IF;
    END LOOP;
END;
$$;

-- ── Main function ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION notify_send_email(
    -- Required
    p_event_type    text,

    -- Recipient(s): supply exactly one of p_recipient or p_recipients.
    --   p_recipient  — single recipient object: {"email":"...","name":"..."}
    --   p_recipients — array of recipient objects
    p_recipient     jsonb    DEFAULT NULL,
    p_recipients    jsonb    DEFAULT NULL,

    -- Required: template variables forwarded to the renderer
    p_payload       jsonb    DEFAULT '{}',

    -- Optional: override the From address for this event only
    --   {"email":"orders@acme.com","name":"Acme Orders"}
    p_from_override jsonb    DEFAULT NULL,

    -- Optional: URL-based attachment references (see 0008 migration for schema)
    p_attachments   jsonb    DEFAULT NULL,

    -- Optional: arbitrary metadata forwarded verbatim to the consumer
    --   {"source":"order-service"}
    p_metadata      jsonb    DEFAULT NULL,

    -- Optional: stable idempotency key. When NULL a random UUID is generated.
    --   Callers that already hold a UUID for the business entity (e.g. order_id)
    --   can pass it here to guarantee at-most-once insertion even on retry.
    p_event_id      uuid     DEFAULT NULL
)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    v_event_id    uuid;
    v_recipients  jsonb;
    v_payload     jsonb;
    v_r           jsonb;
BEGIN
    -- ── 1. Validate event_type ─────────────────────────────────────────────
    IF p_event_type IS NULL OR p_event_type = '' THEN
        RAISE EXCEPTION 'notify_send_email: p_event_type must not be empty'
            USING ERRCODE = 'P0001';
    END IF;

    -- ── 2. Resolve recipients ──────────────────────────────────────────────
    IF p_recipient IS NOT NULL AND p_recipients IS NOT NULL THEN
        RAISE EXCEPTION 'notify_send_email: supply p_recipient OR p_recipients, not both'
            USING ERRCODE = 'P0001';
    END IF;

    IF p_recipient IS NOT NULL THEN
        PERFORM _notify_validate_recipient(p_recipient);
        v_recipients := jsonb_build_array(p_recipient);

    ELSIF p_recipients IS NOT NULL THEN
        IF jsonb_typeof(p_recipients) <> 'array' THEN
            RAISE EXCEPTION 'notify_send_email: p_recipients must be a JSON array'
                USING ERRCODE = 'P0001';
        END IF;
        IF jsonb_array_length(p_recipients) = 0 THEN
            RAISE EXCEPTION 'notify_send_email: p_recipients must not be empty'
                USING ERRCODE = 'P0001';
        END IF;
        FOR v_r IN SELECT jsonb_array_elements(p_recipients) LOOP
            PERFORM _notify_validate_recipient(v_r);
        END LOOP;
        v_recipients := p_recipients;

    ELSE
        RAISE EXCEPTION 'notify_send_email: one of p_recipient or p_recipients is required'
            USING ERRCODE = 'P0001';
    END IF;

    -- ── 3. Validate from_override ──────────────────────────────────────────
    IF p_from_override IS NOT NULL THEN
        IF jsonb_typeof(p_from_override) <> 'object' THEN
            RAISE EXCEPTION 'notify_send_email: p_from_override must be a JSON object'
                USING ERRCODE = 'P0001';
        END IF;
        IF p_from_override->>'email' IS NULL OR p_from_override->>'email' = '' THEN
            RAISE EXCEPTION 'notify_send_email: p_from_override must contain a non-empty "email" field'
                USING ERRCODE = 'P0001';
        END IF;
        -- Reuse single-recipient email validator for the override address.
        PERFORM _notify_validate_recipient(jsonb_build_object('email', p_from_override->>'email'));
    END IF;

    -- ── 4. Validate attachments ────────────────────────────────────────────
    PERFORM _notify_validate_attachments(p_attachments);

    -- ── 5. Build payload envelope ──────────────────────────────────────────
    v_event_id := COALESCE(p_event_id, gen_random_uuid());

    -- Build the full event payload that the outbox worker will forward to
    -- RabbitMQ verbatim as an EmailEvent JSON message.
    v_payload := jsonb_build_object(
        'recipients',    v_recipients,
        'payload',       COALESCE(p_payload, '{}'),
        'from_override', p_from_override,    -- NULL is preserved and omitted by the worker
        'attachments',   COALESCE(p_attachments, '[]'::jsonb),
        'metadata',      COALESCE(p_metadata, '{}'::jsonb)
    );

    -- ── 6. Insert into outbox (idempotent on p_event_id) ──────────────────
    -- ON CONFLICT DO NOTHING means callers can safely retry on transaction
    -- failure without risk of duplicate sends. The event_id uniqueness
    -- constraint is the idempotency guard, just as in the consumer layer.
    INSERT INTO outbox (event_id, event_type, payload)
    VALUES             (v_event_id, p_event_type, v_payload)
    ON CONFLICT (event_id) DO NOTHING;

    RETURN v_event_id;
END;
$$;

-- ── Usage examples (commented out) ───────────────────────────────────────────
--
-- Simple order confirmation (single recipient):
--
--   BEGIN;
--     INSERT INTO orders (...) VALUES (...) RETURNING id INTO v_order_id;
--
--     PERFORM notify_send_email(
--       p_event_type => 'ORDER_CONFIRMATION',
--       p_recipient  => jsonb_build_object('email', v_email, 'name', v_name),
--       p_payload    => jsonb_build_object('orderId', v_order_id, 'amount', v_amount),
--       p_event_id   => v_order_id   -- use order UUID as idempotency key
--     );
--   COMMIT;
--
-- Team invite (multiple recipients, custom From, attachment):
--
--   SELECT notify_send_email(
--     p_event_type    => 'TEAM_INVITE',
--     p_recipients    => '[
--       {"email":"alice@example.com","name":"Alice"},
--       {"email":"bob@example.com","name":"Bob"}
--     ]'::jsonb,
--     p_payload       => '{"teamName":"Acme Engineering","inviterName":"Carol"}'::jsonb,
--     p_from_override => '{"email":"noreply@acme.com","name":"Acme"}'::jsonb,
--     p_metadata      => '{"source":"team-service"}'::jsonb
--   );