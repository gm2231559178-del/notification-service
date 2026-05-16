-- Store the recipient's display name so it is preserved on manual retry.
--
-- Before this migration, republish_event reconstructed recipients as
-- { "email": "..." } only — losing the original "name" field. Templates that
-- use {{name}} would then render as the literal placeholder on retry.
--
-- The column is nullable: rows written before this migration have NULL,
-- which republish_event treats as "name not known" and omits from the
-- re-published recipient object (same behaviour as before for old rows).

ALTER TABLE email_log
    ADD COLUMN IF NOT EXISTS recipient_name TEXT;
