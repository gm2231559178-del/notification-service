use std::sync::Arc;

use common::{
    is_valid_email, AppError, FromOverride, GroupRetryMode, MailerKind, NotificationEvent,
    NotificationStatus, Recipient,
};
use mailer::message::ResolvedAttachment;
use mailer::{
    render_html_template, render_template, EmailMessage, EmailSender, MailboxRef, SenderRegistry,
};
use metrics::{counter, histogram};
use rate_limiter::MailRateLimiter;
use recipient_filter::RecipientFilter;
use store::{
    BlockListChecker, EmailInsertPendingArgs, InsertResult, NotificationStore, TemplateResolver,
    CHANNEL_EMAIL,
};
use tracing::{info, instrument, warn};

/// Pre-filtered CC and BCC recipient lists, computed **once per event** before
/// per-recipient tasks are spawned.
///
/// CC/BCC are event-level options shared across every TO recipient.  Computing
/// the filter result once in `delivery.rs` and passing it in here prevents
/// N×M filter evaluations (once per TO × per CC/BCC address) and multiplied
/// log noise for events with many recipients.
#[derive(Debug, Clone)]
pub struct EffectiveCcBcc {
    /// CC recipients that passed the filter (invalid/blocked addresses removed).
    pub cc: Vec<common::Recipient>,
    /// BCC recipients that passed the filter.
    pub bcc: Vec<common::Recipient>,
}

/// Shared, cheaply-cloneable context passed to every per-recipient processor call.
///
/// # Clone cost
///
/// `Clone` on this struct is shallow — every field is either an `Arc<_>` or a
/// type that wraps one internally (`TemplateStore`, `BlockListStore`, and
/// `MailRateLimiter` all hold their state behind an `Arc`; `SenderRegistry`
/// clones a `HashMap<String, Arc<dyn EmailSender>>`; `RecipientFilter` clones
/// two `HashSet`s whose size is bounded by config).  Cloning `ProcessorContext`
/// **never** deep-copies DB pools, connection state, or cache data.  Adding a
/// field that does not follow this pattern (e.g. a `Vec<u8>` or an unbounded
/// cache) would silently make every per-recipient clone expensive.
#[derive(Clone)]
pub struct ProcessorContext {
    pub store: Arc<dyn NotificationStore>,
    pub template_store: Arc<dyn TemplateResolver>,
    /// Global default sender (SMTP or webhook) used when no named account matches.
    pub sender: Arc<dyn EmailSender>,
    /// Registry of named per-business-system SMTP accounts.
    pub sender_registry: SenderRegistry,
    /// Static config-file recipient filter (block/allow lists from config.toml).
    pub filter: RecipientFilter,
    /// DB-backed block/allow-list. Checked after `filter`; entries can be added
    /// or removed at runtime via the HTTP API without a service restart.
    pub block_list_store: Arc<dyn BlockListChecker>,
    pub rate_limiter: MailRateLimiter,
}

/// Result of processing one recipient within an event.
#[derive(Debug)]
pub enum RecipientOutcome {
    Sent,
    Blocked(String),
    /// Recipient is already in a terminal state (SENT or BLOCKED) — skip.
    Skipped,
    /// Row already exists and is non-terminal; carries the current DB
    /// `retry_count` so the runner can seed its in-memory attempt counter
    /// without a second round-trip.
    Duplicate {
        retry_count: i32,
    },
    Failed(AppError),
    /// Group send failed after individual `notification_log` rows were already written
    /// for every recipient (`group_retry_mode = Individual`).  The runner
    /// should fall back to the individual-send path so only unsent recipients
    /// are retried, rather than re-sending the whole group email.
    GroupFailedWithIndividualRows(AppError),
}

// ── Public entry points ───────────────────────────────────────────────────────

/// Process a single recipient for an event (individual send mode).
///
/// `email_opts` is the resolved `EmailOptions` extracted from the event's
/// `channel_overrides.email`. The caller (runner) is responsible for
/// unwrapping and validating that email options are present before calling
/// this function.
///
/// `attachments` are pre-fetched at the event level (once for all recipients)
/// and passed in as resolved bytes. This avoids re-fetching pre-signed URLs
/// for every recipient, which would waste bandwidth and risk URL expiry for
/// later recipients in the list.
#[instrument(skip(ctx, event, email_opts, recipient, attachments, cc_bcc, shutdown),
             fields(event_id = %event.event_id, email = %recipient.email))]
pub async fn process_recipient(
    ctx: &ProcessorContext,
    event: &NotificationEvent,
    email_opts: &common::EmailOptions,
    recipient: &Recipient,
    attachments: &[ResolvedAttachment],
    cc_bcc: &EffectiveCcBcc,
    shutdown: &tokio_util::sync::CancellationToken,
) -> RecipientOutcome {
    // ── 0. Recipient email validation (before DB write) ─────────────────────
    if !is_valid_email(&recipient.email) {
        return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
            "invalid recipient email address: {}",
            recipient.email
        )));
    }

    // ── 1. Template lookup (before DB write) ────────────────────────────────
    let prefetched_template = match ctx
        .template_store
        .resolve(&event.event_type, CHANNEL_EMAIL)
        .await
    {
        Ok(t) => t,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // ── 2. from_override validation (before DB write) ───────────────────────
    let (from_email_override, from_name_override) =
        resolve_from_override(email_opts.from_override.as_ref());
    if let Some(ref addr) = from_email_override {
        if !is_valid_email(addr) {
            let msg = format!("invalid from_override email address: {addr}");
            return RecipientOutcome::Failed(AppError::permanent_mailer(msg));
        }
    }

    // ── 2b. CC / BCC ──────────────────────────────────────────────────────────
    // Validation and filtering were done once at the delivery level in
    // `delivery.rs` before per-recipient tasks were spawned.  Use the
    // pre-filtered lists directly — avoids N×M filter evaluations and log
    // noise for events with many TO recipients.
    let effective_cc = &cc_bcc.cc;
    let effective_bcc = &cc_bcc.bcc;

    // ── 2c. Template rendering (before DB write) ──────────────────────────────
    // Rendering is done here, before the idempotency DB write, so that a
    // permanently broken template (bad Handlebars syntax) returns Failed without
    // ever creating a PENDING row.  Without this, a bad template creates a row
    // on the first attempt, returns Failed, and the retry loop burns through
    // max_retries on a Duplicate path before giving up — producing log noise and
    // wasting retry budget on an error that is not transient.
    let rendered = match render_all_templates(
        &prefetched_template.subject,
        &prefetched_template.body_html,
        &prefetched_template.body_text,
        &event.payload,
    ) {
        Ok(r) => r,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // ── 3. Idempotency ───────────────────────────────────────────────────────
    // Use map_err + ? rather than .ok() so that a serialization failure
    // surfaces as a permanent error instead of silently storing NULL and
    // losing the from_override / attachments / cc / bcc in the DB row.
    // In practice serde_json::to_value never fails on these well-typed structs,
    // but making failures loud protects against future field changes.
    let fields = match serialize_email_fields(email_opts, effective_cc, effective_bcc) {
        Ok(f) => f,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    match ctx
        .store
        .insert_pending(&EmailInsertPendingArgs {
            event_id: event.event_id,
            event_type: &event.event_type,
            recipient_email: &recipient.email,
            recipient_name: recipient.name.as_deref(),
            payload: &event.payload,
            from_override: fields.from_override.as_ref(),
            attachments: fields.attachments.as_ref(),
            sender_account: email_opts.sender_account.as_deref(),
            cc: fields.cc.as_ref(),
            bcc: fields.bcc.as_ref(),
            send_mode: email_opts.send_mode.as_str(),
            group_retry_mode: None, // individual mode — group_retry_mode is not applicable
            to_recipients: None,    // individual mode — every recipient has its own row
            event_timestamp: event.timestamp,
        })
        .await
    {
        Ok(InsertResult::Inserted) => {}
        Ok(InsertResult::Duplicate {
            retry_count,
            status,
        }) => match NotificationStatus::try_from(status.as_str()) {
            Ok(NotificationStatus::Sent) | Ok(NotificationStatus::Blocked) => {
                info!("Skipping already-terminal recipient");
                return RecipientOutcome::Skipped;
            }
            Ok(_) => return RecipientOutcome::Duplicate { retry_count },
            Err(e) => return RecipientOutcome::Failed(e),
        },
        Err(e) => return RecipientOutcome::Failed(e),
    }

    // ── 4. Recipient filter (config-file) ────────────────────────────────────
    // Skip entirely when no filter rules are configured: `is_passthrough` is
    // O(1) and avoids the string-lowercasing and HashSet lookups inside `check`.
    if !ctx.filter.is_passthrough() {
        if let Err(AppError::Blocked(reason)) = ctx.filter.check(&recipient.email) {
            warn!(reason = %reason, "Recipient blocked by config filter — dropping");
            let _ = ctx
                .store
                .mark_blocked(event.event_id, &recipient.email, &reason)
                .await;
            counter!("emails_blocked_total", "event_type" => event.event_type.clone()).increment(1);
            return RecipientOutcome::Blocked(reason);
        }
    }

    // ── 4b. DB-backed block/allow-list ────────────────────────────────────────
    // Checked after the static filter so config-file rules always win.
    // DB entries can be added/removed at runtime via the HTTP API.
    // We do NOT call is_empty() here as a guard — is_empty() itself calls
    // snapshot() which is a cache hit; calling it before check() would double
    // the cache accesses.  check() handles the empty/passthrough case
    // correctly and efficiently in one snapshot() call.
    if let Err(AppError::Blocked(reason)) = ctx.block_list_store.check(&recipient.email).await {
        warn!(reason = %reason, "Recipient blocked by DB block_list — dropping");
        let _ = ctx
            .store
            .mark_blocked(event.event_id, &recipient.email, &reason)
            .await;
        counter!("emails_blocked_total", "event_type" => event.event_type.clone()).increment(1);
        return RecipientOutcome::Blocked(reason);
    }

    // (subject, body_html, body_text rendered above at step 2c, before the DB write)

    let msg = build_email_message(
        event,
        recipient,
        vec![], // individual mode: one To: address only
        rendered,
        (from_email_override, from_name_override),
        attachments,
        effective_cc,
        effective_bcc,
    );

    // ── 6 & 7. Rate-limit + send ─────────────────────────────────────────────
    execute_send(
        ctx,
        &msg,
        email_opts.sender_account.as_deref(),
        &event.event_type,
        shutdown,
        SendTargets::Individual {
            event_id: event.event_id,
            email: &recipient.email,
        },
    )
    .await
}

/// Serialize a non-empty recipient slice to a JSON value for storage in `notification_log`.
///
/// Returns `None` for an empty slice (stored as SQL NULL), or `Some(Value)`
/// otherwise. Errors surface as `AppError::permanent_mailer` so callers can
/// propagate them without a second DB write.
///
/// Used by both `process_recipient` and `process_group` to serialize the
/// effective (post-filter) CC and BCC recipient lists. The `field` string
/// ("cc" / "bcc") is included in error messages to aid diagnostics.
fn serialize_recipient_list(
    list: &[Recipient],
    field: &str,
) -> Result<Option<serde_json::Value>, AppError> {
    if list.is_empty() {
        return Ok(None);
    }
    // `Recipient` derives `Serialize`, so we serialise the slice directly.
    // Using manual `json!` construction was fragile: any new field added to
    // `Recipient` would be silently omitted from the stored JSONB, causing
    // round-trip divergence on retry.
    serde_json::to_value(list)
        .map(Some)
        .map_err(|e| AppError::permanent_mailer(format!("failed to serialize {field}: {e}")))
}

/// Process all recipients as a single group email (group send mode).
///
/// All addresses in `email_opts.recipients` appear together in the `To:`
/// header of one email.
///
/// ## Idempotency / retry behaviour
///
/// The strategy depends on `email_opts.group_retry_mode`:
///
/// **`GroupRetryMode::Whole`** (default) — only the primary (first) recipient
/// gets a `notification_log` row.  On retry the whole group email is re-sent as a
/// unit.  Simple, but if SMTP accepted the message for some recipients before
/// the connection dropped, those recipients may receive the email twice.
///
/// **`GroupRetryMode::Individual`** — a `notification_log` row is inserted for
/// **every** recipient before the send attempt.  On failure the function
/// returns `RecipientOutcome::GroupFailedWithIndividualRows` so the runner
/// can fall back to `process_one_recipient` for each address, skipping those
/// that already have a `SENT` row.  Retried recipients receive a separate
/// email (the `To:` header shows only their own address); the shared-`To:`
/// visibility of the original group email is not preserved on retry.
#[instrument(skip(ctx, event, email_opts, attachments, cc_bcc, shutdown),
             fields(event_id = %event.event_id, recipient_count = email_opts.recipients.len()))]
pub async fn process_group(
    ctx: &ProcessorContext,
    event: &NotificationEvent,
    email_opts: &common::EmailOptions,
    attachments: &[ResolvedAttachment],
    cc_bcc: &EffectiveCcBcc,
    max_recipients: usize,
    shutdown: &tokio_util::sync::CancellationToken,
) -> RecipientOutcome {
    let recipients = &email_opts.recipients;

    // The primary recipient is used for notification_log tracking. We take the first
    // address; the rest go into to_extra on the EmailMessage.
    let primary = match recipients.first() {
        Some(r) => r,
        None => {
            return RecipientOutcome::Failed(AppError::permanent_mailer(
                "group send: recipients list is empty",
            ));
        }
    };

    // ── 0a. Recipient count guard (defence-in-depth) ─────────────────────────
    // The primary enforcement happens in `delivery.rs` (`max_recipients_per_event`)
    // before `process_group` is called; the same limit is passed in here so both
    // layers always use the same configured value.  This check fires only if a
    // future call-site bypasses the outer guard (e.g. a test harness).
    if recipients.len() > max_recipients {
        return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
            "group send: recipient count {} exceeds maximum allowed ({})",
            recipients.len(),
            max_recipients,
        )));
    }

    // ── 0b. Validate all To: addresses ───────────────────────────────────────
    for r in recipients {
        if !is_valid_email(&r.email) {
            return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid recipient email address: {}",
                r.email
            )));
        }
    }

    // ── 1. Template lookup ───────────────────────────────────────────────────
    let prefetched_template = match ctx
        .template_store
        .resolve(&event.event_type, CHANNEL_EMAIL)
        .await
    {
        Ok(t) => t,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // ── 2. from_override + cc/bcc validation and filter check ────────────────
    let (from_email_override, from_name_override) =
        resolve_from_override(email_opts.from_override.as_ref());
    if let Some(ref addr) = from_email_override {
        if !is_valid_email(addr) {
            return RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid from_override email address: {addr}"
            )));
        }
    }
    // CC/BCC validation and filtering were done once at the delivery level
    // (delivery.rs) before this function was called, for the same reason as in
    // process_recipient: avoids N×M filter evaluations and log noise.
    let effective_cc = &cc_bcc.cc;
    let effective_bcc = &cc_bcc.bcc;

    // ── 2c. Template rendering (before DB write) ──────────────────────────────────
    // Same rationale as process_recipient step 2c: fail before writing any DB row
    // so a permanently broken template does not burn retry budget.
    let rendered = match render_all_templates(
        &prefetched_template.subject,
        &prefetched_template.body_html,
        &prefetched_template.body_text,
        &event.payload,
    ) {
        Ok(r) => r,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // ── 3. Idempotency ───────────────────────────────────────────────────────
    // Use map_err + ? for the same reason as in process_recipient: failures
    // should be loud, not silently stored as NULL.
    let fields = match serialize_email_fields(email_opts, effective_cc, effective_bcc) {
        Ok(f) => f,
        Err(e) => return RecipientOutcome::Failed(e),
    };
    let group_retry_mode_str = match email_opts.group_retry_mode {
        GroupRetryMode::Whole => "whole",
        GroupRetryMode::Individual => "individual",
    };

    // Shared (event-level) fields for every insert_pending call in this group send.
    // Bundled into a struct so the inner `fn` below takes two parameters instead
    // of eight, keeping clippy happy without the lifetime-inference limitation
    // that prevents closures from expressing higher-ranked lifetimes (for<'a>).
    struct SharedArgs<'a> {
        event: &'a NotificationEvent,
        email_opts: &'a common::EmailOptions,
        fields: &'a SerializedEmailFields,
        group_retry_mode_str: &'a str,
        /// Serialized full To: list. Non-None only for GroupRetryMode::Whole so
        /// that the single primary row records who else received the email.
        to_recipients_json: Option<&'a serde_json::Value>,
    }
    fn make_args<'a>(r: &'a Recipient, s: &'a SharedArgs<'a>) -> EmailInsertPendingArgs<'a> {
        EmailInsertPendingArgs {
            event_id: s.event.event_id,
            event_type: &s.event.event_type,
            recipient_email: &r.email,
            recipient_name: r.name.as_deref(),
            payload: &s.event.payload,
            from_override: s.fields.from_override.as_ref(),
            attachments: s.fields.attachments.as_ref(),
            sender_account: s.email_opts.sender_account.as_deref(),
            cc: s.fields.cc.as_ref(),
            bcc: s.fields.bcc.as_ref(),
            send_mode: s.email_opts.send_mode.as_str(),
            group_retry_mode: Some(s.group_retry_mode_str),
            to_recipients: s.to_recipients_json,
            event_timestamp: s.event.timestamp,
        }
    }

    // For GroupRetryMode::Whole, serialize the full recipient list once so the
    // primary row records every address that received this group email.
    // For GroupRetryMode::Individual every recipient gets its own row, so
    // to_recipients is redundant and left NULL.
    let to_recipients_json: Option<serde_json::Value> =
        if email_opts.group_retry_mode == GroupRetryMode::Whole {
            match serde_json::to_value(&email_opts.recipients).map_err(|e| {
                AppError::permanent_mailer(format!("failed to serialize to_recipients: {e}"))
            }) {
                Ok(v) => Some(v),
                Err(e) => return RecipientOutcome::Failed(e),
            }
        } else {
            None
        };

    let shared = SharedArgs {
        event,
        email_opts,
        fields: &fields,
        group_retry_mode_str,
        to_recipients_json: to_recipients_json.as_ref(),
    };

    // ── 3a. Idempotency inserts ───────────────────────────────────────────────
    //
    // For GroupRetryMode::Individual every recipient gets its own row so that
    // re-delivery after a partial send can skip already-SENT addresses.  All
    // rows are written in a single call so the set is written atomically:
    // either every row exists or none do.  A crash during the insert rolls
    // back the transaction; on re-delivery the batch is retried in full.
    //
    // For GroupRetryMode::Whole only the primary row is written (one row
    // tracks the whole group send as a unit).
    let rows_to_insert: Vec<_> = if email_opts.group_retry_mode == GroupRetryMode::Individual {
        recipients.iter().map(|r| make_args(r, &shared)).collect()
    } else {
        vec![make_args(primary, &shared)]
    };

    let insert_results = match ctx.store.insert_pending_batch(&rows_to_insert).await {
        Ok(r) => r,
        Err(e) => return RecipientOutcome::Failed(e),
    };

    // ── 3b. Inspect insert results ────────────────────────────────────────────
    //
    // The primary row is always first and drives the terminal-state check.
    //
    // Secondary rows (GroupRetryMode::Individual only) are also inspected so
    // we can:
    //   • Count how many were already SENT in a prior partial attempt, and
    //     exclude them from the To: header of the retry email (partial send).
    //   • Warn operators when GroupRetryMode::Whole causes a full-group resend
    //     that may reach recipients who were already delivered to.
    let mut results_iter = insert_results.into_iter();

    let primary_insert = match results_iter.next() {
        Some(r) => r,
        None => {
            return RecipientOutcome::Failed(AppError::permanent_mailer(
                "insert_pending_batch returned empty results",
            ))
        }
    };

    match primary_insert {
        InsertResult::Duplicate {
            retry_count,
            ref status,
        } => match NotificationStatus::try_from(status.as_str()) {
            Ok(NotificationStatus::Sent) | Ok(NotificationStatus::Blocked) => {
                // For GroupRetryMode::Whole the primary row represents the whole
                // group; terminal primary → entire event is done.
                // For GroupRetryMode::Individual every recipient has its own row;
                // the caller (runner) drives per-recipient retries, so Skipped
                // here means this individual row is already terminal.
                info!("Group send: skipping already-terminal event");
                return RecipientOutcome::Skipped;
            }
            Ok(_) => {
                return RecipientOutcome::Duplicate { retry_count };
            }
            Err(e) => return RecipientOutcome::Failed(e),
        },
        InsertResult::Inserted => {}
    }

    // ── 3c. Secondary-row status inspection (GroupRetryMode::Individual) ──────
    //
    // When retry_mode = Individual we inserted one row per recipient.
    // Walk the remaining results to build the set of addresses that are
    // already SENT from a previous partial attempt.  These will be stripped
    // from the To: list so the retry email is only sent to genuinely unsent
    // addresses — avoiding double-delivery to recipients the SMTP server
    // already accepted in the prior attempt.
    //
    // For GroupRetryMode::Whole there is only one row (the primary), so
    // results_iter is now exhausted and this block is a no-op.  Instead,
    // a warn is emitted below when the whole-group email is about to be
    // resent and some recipients may have already received it.
    let mut already_sent_emails: std::collections::HashSet<&str> = std::collections::HashSet::new();

    if email_opts.group_retry_mode == GroupRetryMode::Individual {
        // `recipients` and the secondary insert results are in the same order
        // (skip(1) because index 0 is the primary, already consumed above).
        for (r, result) in recipients.iter().skip(1).zip(results_iter) {
            match result {
                InsertResult::Duplicate { ref status, .. } => {
                    match NotificationStatus::try_from(status.as_str()) {
                        Ok(NotificationStatus::Sent) | Ok(NotificationStatus::Blocked) => {
                            // Already delivered or explicitly blocked in a prior
                            // attempt — exclude from the To: header of the retry.
                            already_sent_emails.insert(&r.email);
                        }
                        Ok(_) | Err(_) => {
                            // Non-terminal duplicate or unknown status — include in
                            // the retry; execute_send will re-attempt delivery.
                        }
                    }
                }
                InsertResult::Inserted => {} // fresh row — include normally
            }
        }

        if !already_sent_emails.is_empty() {
            warn!(
                event_id          = %event.event_id,
                already_sent      = already_sent_emails.len(),
                total_recipients  = recipients.len(),
                "Group send (Individual mode): {} recipient(s) already SENT/BLOCKED in a \
                 prior partial attempt — excluding from retry To: list to avoid double-delivery",
                already_sent_emails.len(),
            );
        }
    }

    // ── 4. Recipient filter — partition To: addresses into allowed / blocked ───
    // Design note — TO vs CC/BCC asymmetry:
    //   • Blocked TO recipients are excluded from the delivery; if *all* TO
    //     addresses are blocked the entire group send is dropped.  If only *some*
    //     are blocked, the mail is delivered to the remaining (allowed) addresses
    //     and the blocked ones are marked BLOCKED in the DB.
    //   • Blocked CC/BCC addresses (step 2 above) are silently excluded and
    //     delivery always continues — silencing a whole delivery for one unwanted
    //     copy recipient would be disproportionate.
    //   Operators: to recover from an all-blocked drop, remove the blocked TO
    //   addresses from the event payload and re-publish, or update the blocklist.
    let mut allowed_recipients: Vec<&Recipient> = Vec::with_capacity(recipients.len());
    for r in recipients {
        match ctx.filter.check(&r.email) {
            Ok(()) => allowed_recipients.push(r),
            Err(AppError::Blocked(ref reason)) => {
                warn!(
                    blocked_email = %r.email,
                    reason = %reason,
                    recipient_count = recipients.len(),
                    "Group send: TO recipient blocked — excluding from delivery"
                );
                if let Err(ref db_err) = ctx
                    .store
                    .mark_blocked(event.event_id, &r.email, reason)
                    .await
                {
                    warn!(
                        event_id = %event.event_id,
                        email    = %r.email,
                        error    = %db_err,
                        "Group send: mark_blocked DB write failed — row remains PENDING; \
                         operator should manually mark it BLOCKED or it will be retried"
                    );
                }
                counter!("emails_blocked_total", "event_type" => event.event_type.clone())
                    .increment(1);
            }
            // Non-Blocked filter errors are treated as pass-through (fail-open)
            // so an unexpected filter error never silently drops a recipient.
            Err(_) => allowed_recipients.push(r),
        }
    }

    // If every TO address was blocked there is nothing left to send.
    if allowed_recipients.is_empty() {
        // The primary row was already marked BLOCKED in the loop above.
        // For Individual mode, all other rows were also marked there.
        warn!(
            event_id = %event.event_id,
            "Group send: all TO recipients blocked — dropping delivery"
        );
        return RecipientOutcome::Blocked("all TO recipients blocked by filter".into());
    }

    // ── 4b. Partial-send: exclude already-SENT/BLOCKED recipients ─────────────
    // For GroupRetryMode::Individual, `already_sent_emails` was populated in
    // step 3c with addresses whose DB row came back as Duplicate { Sent | Blocked }
    // — meaning they were successfully delivered (or explicitly blocked) in a
    // prior partial attempt.  Strip them from the To: list now so the retry
    // email is only sent to genuinely unsent addresses.
    //
    // For GroupRetryMode::Whole this set is always empty (only the primary row
    // is written), so the filter below is a no-op.  The delivery.rs caller
    // already emits a warn about potential double-delivery in that mode; no
    // additional warn is needed here.
    let allowed_recipients: Vec<&Recipient> = if already_sent_emails.is_empty() {
        allowed_recipients
    } else {
        allowed_recipients
            .into_iter()
            .filter(|r| !already_sent_emails.contains(r.email.as_str()))
            .collect()
    };

    // Shadow `recipients` so the rest of the function operates only on the
    // allowed + unsent subset — template rendering, EmailMessage construction,
    // DB mark_sent calls, and the `to_count` log field all stay consistent.
    let recipients = allowed_recipients;

    // (rendered at step 2c, before the DB write)

    // `primary` is `recipients[0]` from the original (pre-filter) list and was
    // confirmed Inserted above.  It is never in `already_sent_emails` so it is
    // always present in the shadowed `recipients` after the partial-send strip.
    // `to_extra` holds all remaining unsent addresses.
    let to_extra: Vec<MailboxRef> = recipients
        .iter()
        .skip(1)
        .map(|r| MailboxRef {
            email: r.email.clone(),
            name: r.name.clone(),
        })
        .collect();

    let msg = build_email_message(
        event,
        primary,
        to_extra,
        rendered,
        (from_email_override, from_name_override),
        attachments,
        effective_cc,
        effective_bcc,
    );

    // ── 6 & 7. Rate-limit + send ─────────────────────────────────────────────
    execute_send(
        ctx,
        &msg,
        email_opts.sender_account.as_deref(),
        &event.event_type,
        shutdown,
        SendTargets::Group {
            event_id: event.event_id,
            primary_email: &primary.email,
            secondaries: if email_opts.group_retry_mode == GroupRetryMode::Individual {
                recipients
                    .iter()
                    .skip(1)
                    .map(|r| r.email.as_str())
                    .collect()
            } else {
                vec![]
            },
            retry_mode: &email_opts.group_retry_mode,
            to_count: recipients.len(),
        },
    )
    .await
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Describes which `notification_log` rows to mark SENT after a successful send.
///
/// Passed to [`execute_send`] so the shared rate-limit + send + mark_sent
/// logic can handle both individual and group sends without duplicating code.
pub(crate) enum SendTargets<'a> {
    /// Individual send: exactly one row to mark SENT.
    Individual {
        event_id: uuid::Uuid,
        email: &'a str,
    },
    /// Group send: primary row always marked SENT; secondaries marked SENT only
    /// when `retry_mode` is `Individual` (i.e. `secondaries` is non-empty).
    Group {
        event_id: uuid::Uuid,
        primary_email: &'a str,
        /// Pre-filtered slice of secondary recipient emails to mark SENT.
        /// Empty for `GroupRetryMode::Whole`.
        secondaries: Vec<&'a str>,
        retry_mode: &'a GroupRetryMode,
        to_count: usize,
    },
}

/// Shared steps 6–7: rate-limit wait → sender selection → send → mark_sent → metrics.
///
/// Called by both [`process_recipient`] and [`process_group`] to eliminate the
/// duplicate rate-limit / send / mark_sent / metric blocks.  Each caller passes
/// the appropriate [`SendTargets`] variant so `mark_sent` and the log message
/// stay correct for both individual and group sends.
async fn execute_send(
    ctx: &ProcessorContext,
    msg: &mailer::EmailMessage,
    sender_account: Option<&str>,
    event_type: &str,
    shutdown: &tokio_util::sync::CancellationToken,
    targets: SendTargets<'_>,
) -> RecipientOutcome {
    // ── Rate-limit token ──────────────────────────────────────────────────────
    if let Err(outcome) = acquire_rate_limit_token(&ctx.rate_limiter, event_type, shutdown).await {
        return outcome;
    }

    // ── Sender selection ──────────────────────────────────────────────────────
    let sender = resolve_sender(
        &ctx.sender_registry,
        &ctx.sender,
        sender_account,
        event_type,
    );

    // ── Send ──────────────────────────────────────────────────────────────────
    let send_start = std::time::Instant::now();
    match sender.send(msg).await {
        Ok(()) => {
            let elapsed = send_start.elapsed().as_secs_f64();
            mark_sent_for_targets(&ctx.store, &targets, event_type).await;

            counter!("emails_sent_total",
                "event_type" => event_type.to_owned())
            .increment(1);
            histogram!("email_send_duration_seconds",
                "event_type" => event_type.to_owned())
            .record(elapsed);

            match &targets {
                SendTargets::Individual { .. } => info!("Email delivered"),
                SendTargets::Group { to_count, .. } => {
                    info!(to_count = %to_count, "Group email delivered")
                }
            }

            RecipientOutcome::Sent
        }
        Err(e) => {
            counter!("emails_failed_total",
                "event_type" => event_type.to_owned(),
                "reason"     => error_reason_label(&e)
            )
            .increment(1);

            match &targets {
                SendTargets::Individual { .. } => warn!(error = %e, "Send failed"),
                SendTargets::Group { .. } => warn!(error = %e, "Group send failed"),
            }

            match targets {
                SendTargets::Individual { .. } => RecipientOutcome::Failed(e),
                SendTargets::Group { retry_mode, .. } => match retry_mode {
                    GroupRetryMode::Individual => {
                        RecipientOutcome::GroupFailedWithIndividualRows(e)
                    }
                    GroupRetryMode::Whole => RecipientOutcome::Failed(e),
                },
            }
        }
    }
}

/// Acquire a rate-limit token before sending.
///
/// Returns `Ok(())` when a token is available (immediately or after waiting).
/// Returns `Err(RecipientOutcome::Failed)` on shutdown.
pub(crate) async fn acquire_rate_limit_token(
    rate_limiter: &MailRateLimiter,
    event_type: &str,
    shutdown: &tokio_util::sync::CancellationToken,
) -> Result<(), RecipientOutcome> {
    match rate_limiter.wait_for_token(shutdown).await {
        rate_limiter::TokenResult::Acquired => {}
        rate_limiter::TokenResult::AcquiredAfterWait => {
            counter!("email_rate_limit_waits_total",
                "event_type" => event_type.to_owned())
            .increment(1);
        }
        rate_limiter::TokenResult::Shutdown => {
            return Err(RecipientOutcome::Failed(AppError::Queue(
                "service shutdown during rate-limit wait".into(),
            )));
        }
    }
    Ok(())
}

/// Resolve which sender to use: named account from the registry, or global fallback.
pub(crate) fn resolve_sender(
    registry: &SenderRegistry,
    global_sender: &Arc<dyn EmailSender>,
    sender_account: Option<&str>,
    event_type: &str,
) -> Arc<dyn EmailSender> {
    match registry.resolve(sender_account) {
        Some(s) => s,
        None => {
            if let Some(account) = sender_account {
                warn!(
                    account,
                    event_type,
                    "Named sender_account not found in registry — falling back to global sender. \
                     Check [sender_accounts.{account}] in config."
                );
            }
            Arc::clone(global_sender)
        }
    }
}

/// Mark notification_log rows as SENT after a successful send.
///
/// Handles both individual and group targets. For group sends with
/// `GroupRetryMode::Individual`, secondary rows are marked one by one. Failures
/// are logged at WARN so operators can manually correct stuck rows.
pub(crate) async fn mark_sent_for_targets(
    store: &Arc<dyn NotificationStore>,
    targets: &SendTargets<'_>,
    event_type: &str,
) {
    match targets {
        SendTargets::Individual { event_id, email } => {
            if let Err(e) = store.mark_sent(*event_id, email).await {
                warn!(
                    event_id = %event_id,
                    email    = %email,
                    error    = %e,
                    "Email delivered but mark_sent DB write failed — \
                     row remains PENDING; re-delivery will attempt to re-send"
                );
                counter!("email_mark_sent_failed_total",
                    "event_type" => event_type.to_owned())
                .increment(1);
            }
        }
        SendTargets::Group {
            event_id,
            primary_email,
            secondaries,
            ..
        } => {
            if let Err(e) = store.mark_sent(*event_id, primary_email).await {
                warn!(
                    event_id = %event_id,
                    email    = %primary_email,
                    error    = %e,
                    "Group email delivered but mark_sent DB write failed for primary — \
                     row remains PENDING; re-delivery will attempt to re-send"
                );
                counter!("email_mark_sent_failed_total",
                    "event_type" => event_type.to_owned())
                .increment(1);
            }
            let mut mark_sent_failures: usize = 0;
            for email in secondaries {
                if let Err(e) = store.mark_sent(*event_id, email).await {
                    warn!(
                        event_id = %event_id,
                        email    = %email,
                        error    = %e,
                        "Group email delivered but mark_sent DB write failed for secondary \
                         recipient — row remains PENDING; re-delivery will attempt to re-send"
                    );
                    counter!("email_mark_sent_failed_total",
                        "event_type" => event_type.to_owned())
                    .increment(1);
                    mark_sent_failures += 1;
                }
            }
            if mark_sent_failures > 0 {
                counter!("email_group_mark_sent_partial_total",
                    "event_type" => event_type.to_owned())
                .increment(1);
            }
        }
    }
}

fn resolve_from_override(ov: Option<&FromOverride>) -> (Option<String>, Option<String>) {
    match ov {
        None => (None, None),
        Some(o) => (Some(o.email.clone()), o.name.clone()),
    }
}

// ── Shared rendering / serialization / message-building helpers ────────────────
//
// These pure helpers eliminate duplication between `process_recipient` and
// `process_group`.  Each is independently unit-testable without DB or network.

pub(crate) struct RenderedTemplates {
    pub(crate) subject: String,
    pub(crate) body_html: String,
    pub(crate) body_text: String,
}

/// Render all three template parts (subject, HTML body, text body).
///
/// Returns `Err` with the first render error if any part fails.  All three
/// errors are logged at WARN before returning so operators see every broken
/// part in a single log entry rather than chasing them one at a time.
pub(crate) fn render_all_templates(
    subject_tpl: &str,
    html_tpl: &str,
    text_tpl: &str,
    payload: &serde_json::Value,
) -> Result<RenderedTemplates, AppError> {
    let sr = render_template(subject_tpl, payload);
    let hr = render_html_template(html_tpl, payload);
    let tr = render_template(text_tpl, payload);

    match (sr, hr, tr) {
        (Ok(s), Ok(h), Ok(t)) => Ok(RenderedTemplates {
            subject: s,
            body_html: h,
            body_text: t,
        }),
        (sr, hr, tr) => {
            if let Err(ref e) = sr {
                warn!(component = "subject",   error = %e, "Template render failed");
            }
            if let Err(ref e) = hr {
                warn!(component = "body_html", error = %e, "Template render failed");
            }
            if let Err(ref e) = tr {
                warn!(component = "body_text", error = %e, "Template render failed");
            }
            // The arm fires only when at least one result is Err, so this
            // chain always yields Some.
            let first_err = sr
                .err()
                .or(hr.err())
                .or(tr.err())
                .expect("match arm requires at least one Err among (sr, hr, tr)");
            Err(first_err)
        }
    }
}

pub(crate) struct SerializedEmailFields {
    pub(crate) from_override: Option<serde_json::Value>,
    pub(crate) attachments: Option<serde_json::Value>,
    pub(crate) cc: Option<serde_json::Value>,
    pub(crate) bcc: Option<serde_json::Value>,
}

/// Serialize the email-level fields that are stored in the `notification_log`
/// row for idempotency and retry replay.
///
/// Returns `Err` (permanent) if any serialization fails — these are
/// well-typed structs so a failure here indicates a field-change bug, not a
/// transient condition.
pub(crate) fn serialize_email_fields(
    email_opts: &common::EmailOptions,
    effective_cc: &[Recipient],
    effective_bcc: &[Recipient],
) -> Result<SerializedEmailFields, AppError> {
    let from_override = email_opts
        .from_override
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| {
            AppError::permanent_mailer(format!("failed to serialize from_override: {e}"))
        })?;

    let attachments = if email_opts.attachments.is_empty() {
        None
    } else {
        Some(serde_json::to_value(&email_opts.attachments).map_err(|e| {
            AppError::permanent_mailer(format!("failed to serialize attachments: {e}"))
        })?)
    };

    let cc = serialize_recipient_list(effective_cc, "cc")?;
    let bcc = serialize_recipient_list(effective_bcc, "bcc")?;

    Ok(SerializedEmailFields {
        from_override,
        attachments,
        cc,
        bcc,
    })
}

/// Build an [`EmailMessage`] from resolved templates, filtered recipients, and
/// pre-fetched attachments.
///
/// `extra_recipients` is empty for individual mode and holds the secondary
/// To: addresses (beyond the primary) for group mode.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_email_message(
    event: &NotificationEvent,
    primary: &Recipient,
    extra_recipients: Vec<MailboxRef>,
    rendered: RenderedTemplates,
    from_override: (Option<String>, Option<String>),
    attachments: &[ResolvedAttachment],
    effective_cc: &[Recipient],
    effective_bcc: &[Recipient],
) -> EmailMessage {
    EmailMessage {
        event_id: event.event_id,
        to_email: primary.email.clone(),
        to_name: primary.name.clone(),
        to_extra: extra_recipients,
        subject: rendered.subject,
        body_html: rendered.body_html,
        body_text: rendered.body_text,
        from_email_override: from_override.0,
        from_name_override: from_override.1,
        attachments: attachments.to_vec(),
        cc: effective_cc
            .iter()
            .map(|r| MailboxRef {
                email: r.email.clone(),
                name: r.name.clone(),
            })
            .collect(),
        bcc: effective_bcc
            .iter()
            .map(|r| MailboxRef {
                email: r.email.clone(),
                name: r.name.clone(),
            })
            .collect(),
    }
}

pub fn is_retryable(err: &AppError) -> bool {
    match err {
        AppError::Duplicate(_)
        | AppError::NotFound(_)
        | AppError::Template(_)
        | AppError::Blocked(_) => false,
        // UnknownStatus is a data-integrity error: the DB row has a status value
        // this binary doesn't recognise.  Retrying will hit the same row and return
        // the same unrecognised status indefinitely, burning retry budget and
        // generating log noise.  Treat it as a permanent failure so it goes to DLQ
        // where an operator can investigate.
        AppError::UnknownStatus(_) => false,
        _ if err.is_permanent_mailer() => false,
        _ => true,
    }
}

fn error_reason_label(err: &AppError) -> &'static str {
    match err {
        AppError::RateLimited(_) => "rate_limited",
        AppError::Mailer {
            kind: MailerKind::Permanent,
            ..
        } => "permanent",
        AppError::Mailer { .. } => "transient",
        // `Blocked` should never reach `execute_send` — recipients are
        // filtered before that call — but if the code path ever changes
        // this ensures the metric label is accurate rather than silently
        // falling through to "other".
        AppError::Blocked(_) => "blocked",
        AppError::Database(_) => "database",
        AppError::Template(_) => "template",
        AppError::Queue(_) => "queue",
        AppError::NotFound(_) => "not_found",
        AppError::Deserialize(_) => "deserialize",
        AppError::UnknownStatus(_) => "unknown_status",
        // These variants do not represent send-path failures; they are
        // included here to make the match exhaustive so a future new
        // AppError variant triggers a compile error rather than silently
        // labelling metrics as "other".
        AppError::Duplicate(_) => "other",
    }
}
