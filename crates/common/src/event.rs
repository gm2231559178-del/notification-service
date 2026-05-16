use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// The canonical event published to the `email.requested` queue.
///
/// Business services write this (or a compatible shape) into their Outbox;
/// the Outbox worker forwards it verbatim to RabbitMQ.
///
/// One event can carry **multiple recipients** — the notification service
/// processes each recipient independently so every delivery has its own
/// `email_log` row, retry counter, and status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailEvent {
    /// Stable unique ID — used for idempotency checks per recipient.
    pub event_id: Uuid,

    /// ISO-8601 timestamp set by the publisher.
    pub timestamp: DateTime<Utc>,

    /// Logical type driving template selection (e.g. "ORDER_CONFIRMATION").
    #[serde(rename = "type")]
    pub event_type: String,

    /// One or more recipients. Each is processed independently:
    /// a blocked recipient does not prevent others from receiving the email.
    ///
    /// Deserialization is backwards-compatible: a legacy single-recipient
    /// payload `"recipient": {...}` is automatically promoted to a
    /// one-element `recipients` list via the custom deserializer below.
    #[serde(deserialize_with = "deserialize_recipients")]
    pub recipients: Vec<Recipient>,

    /// Arbitrary template variables forwarded to the renderer.
    pub payload: Value,

    /// Optional per-event sender override. When present, the notification
    /// service uses these values as the From address instead of the globally
    /// configured mailer defaults. This allows business systems to send on
    /// behalf of different accounts (e.g. "Orders <orders@acme.com>" vs
    /// "Support <support@acme.com>") without reconfiguring the service.
    #[serde(default)]
    pub from_override: Option<FromOverride>,

    #[serde(default)]
    pub metadata: Metadata,

    /// Zero or more file attachments referenced by URL.
    ///
    /// The notification service fetches each URL at send time and attaches
    /// the downloaded bytes to the email. Business systems never need to
    /// encode or embed file content — they only supply a reachable URL.
    ///
    /// See [`AttachmentRef`] for the full contract and URL guidance.
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,

    /// Optional direct body override — bypasses template lookup entirely.
    ///
    /// When present, the notification service uses `subject`, `body_html`,
    /// and `body_text` verbatim instead of looking up and rendering the
    /// template registered for `event_type`.  The `payload` field is still
    /// stored for audit purposes but is **not** used for rendering.
    ///
    /// Use this when the calling service has already rendered the email
    /// content itself (e.g. a rich transactional email built by a separate
    /// templating engine) and only needs the notification service to handle
    /// delivery, rate-limiting, retries, and logging.
    ///
    /// # Interface contract
    ///
    /// ```json
    /// {
    ///   "body_override": {
    ///     "subject":   "Your invoice #1234 is ready",
    ///     "body_html": "<h1>Hi Alice,</h1><p>Your invoice is attached.</p>",
    ///     "body_text": "Hi Alice, your invoice is attached."
    ///   }
    /// }
    /// ```
    ///
    /// All three fields are required when `body_override` is present.
    /// An `event_type` value is still required (used for logging/metrics)
    /// but does not need to correspond to a registered template.
    #[serde(default)]
    pub body_override: Option<BodyOverride>,

    /// Named sender account to use for SMTP authentication and the From address.
    ///
    /// Must match a key under `[sender_accounts]` in the service config.
    /// When absent (or the name is not configured), the service falls back to
    /// the global `[mailer]` settings.
    ///
    /// Use this when multiple business systems share one notification service
    /// but each must send from its own authenticated SMTP account:
    ///
    /// ```json
    /// { "sender_account": "system_a", ... }   // → A@example.com via system A creds
    /// { "sender_account": "system_b", ... }   // → B@example.com via system B creds
    /// { ... }                                  // → global mailer default
    /// ```
    #[serde(default)]
    pub sender_account: Option<String>,
}

// ── Body override ─────────────────────────────────────────────────────────────

/// Pre-rendered email body supplied directly by the publisher.
///
/// When set on [`EmailEvent`], the notification service skips template lookup
/// and rendering and uses these values verbatim as the email content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BodyOverride {
    /// Email subject line (plain text, no HTML).
    pub subject: String,
    /// Full HTML body of the email.
    pub body_html: String,
    /// Plain-text fallback body shown by clients that don't render HTML.
    pub body_text: String,
}

// ── Attachment reference ──────────────────────────────────────────────────────

/// A reference to a file attachment, resolved by the notification service
/// at send time via an HTTP GET.
///
/// # Interface contract for business systems
///
/// ```json
/// {
///   "attachments": [
///     {
///       "url":          "https://storage.example.com/invoices/inv-1234.pdf?token=...",
///       "filename":     "invoice-1234.pdf",
///       "content_type": "application/pdf"
///     }
///   ]
/// }
/// ```
///
/// # URL types that all work
///
/// | Origin | How to provide the URL |
/// |---|---|
/// | Object storage (S3, GCS, Azure Blob) | Generate a pre-signed URL valid for ≥ 5 minutes |
/// | File generated at runtime | Expose `GET /internal/attachments/{id}`, put that URL in the event |
/// | File in DB / filesystem | Same — serve it via an internal endpoint |
///
/// # Retry behaviour
///
/// The notification service may fetch the URL more than once (once per
/// delivery attempt). For pre-signed URLs, ensure the expiry is long enough
/// to survive the full retry window (default: up to ~5 minutes with 3 retries).
/// A `max_age_secs` hint can be set to tell the service to treat a stale
/// fetch as a permanent failure rather than retrying indefinitely.
///
/// # Security
///
/// The notification service fetches URLs with an optional Bearer token
/// (`fetch_token`). For internal URLs, use short-lived tokens or signed paths.
/// Never embed long-lived credentials in the URL itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachmentRef {
    /// Fully-qualified URL the notification service will GET.
    /// Must be reachable from the notification service's network.
    /// Supports `http://` and `https://`.
    pub url: String,

    /// File name shown to the email recipient (e.g. `"invoice-1234.pdf"`).
    /// Must not contain path separators (`/` or `\`).
    pub filename: String,

    /// MIME content-type of the file (e.g. `"application/pdf"`, `"image/png"`).
    /// Used verbatim as the MIME part Content-Type header.
    pub content_type: String,

    /// Optional Bearer token sent as `Authorization: Bearer <token>` when
    /// fetching the URL. Use this for internal service-to-service auth.
    /// Do not include the word "Bearer" — just the raw token value.
    #[serde(default)]
    pub fetch_token: Option<String>,

    /// Optional hint: how many seconds after `EmailEvent.timestamp` the URL
    /// remains valid. When the notification service attempts a fetch after
    /// this window, it marks the delivery permanently FAILED rather than
    /// retrying (avoids burning retry slots on an expired URL).
    ///
    /// If omitted, no expiry check is performed.
    #[serde(default)]
    pub max_age_secs: Option<u64>,
}

impl AttachmentRef {
    /// Validate metadata fields that can be checked without a network call.
    ///
    /// Returns an error string prefixed with `"permanent:"` so the consumer
    /// marks the delivery FAILED immediately without retrying.
    pub fn validate(&self, event_timestamp: &DateTime<Utc>) -> Result<(), String> {
        if self.url.is_empty() {
            return Err("permanent: attachment url must not be empty".into());
        }
        if !self.url.starts_with("http://") && !self.url.starts_with("https://") {
            return Err(format!(
                "permanent: attachment url '{}' must start with http:// or https://",
                self.url
            ));
        }
        if self.filename.is_empty() {
            return Err("permanent: attachment filename must not be empty".into());
        }
        if self.filename.contains('/') || self.filename.contains('\\') {
            return Err(format!(
                "permanent: attachment filename '{}' must not contain path separators",
                self.filename
            ));
        }
        if !self.content_type.contains('/') {
            return Err(format!(
                "permanent: attachment content_type '{}' is not a valid MIME type",
                self.content_type
            ));
        }
        // Expiry check: fail early if the URL window has already elapsed.
        if let Some(max_age) = self.max_age_secs {
            let age = Utc::now()
                .signed_duration_since(*event_timestamp)
                .num_seconds()
                .max(0) as u64;
            if age > max_age {
                return Err(format!(
                    "permanent: attachment '{}' URL has expired \
                     (max_age_secs={max_age}, age={age}s)",
                    self.filename
                ));
            }
        }
        Ok(())
    }
}

// ── Deserializer ──────────────────────────────────────────────────────────────

/// Deserialize `recipients` from either the new array form or the legacy
/// single-object form so the outbox worker and old publishers stay compatible.
///
/// Accepts:
/// - `"recipients": [{"email": "a@b.com"}]`  (preferred, multi-recipient)
/// - `"recipient":  {"email": "a@b.com"}`     (legacy single-recipient)
///
/// If both keys are present, `recipients` takes priority.
fn deserialize_recipients<'de, D>(de: D) -> Result<Vec<Recipient>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, MapAccess, Visitor};
    use std::fmt;

    struct RecipientsVisitor;

    impl<'de> Visitor<'de> for RecipientsVisitor {
        type Value = Vec<Recipient>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a recipients array or a single recipient object")
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut v = Vec::new();
            while let Some(r) = seq.next_element::<Recipient>()? {
                v.push(r);
            }
            Ok(v)
        }

        fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            let r = Recipient::deserialize(de::value::MapAccessDeserializer::new(map))?;
            Ok(vec![r])
        }
    }

    de.deserialize_any(RecipientsVisitor)
}

// ── Supporting types ──────────────────────────────────────────────────────────

/// Per-event From address override supplied by the business system.
///
/// Only `email` is required; `name` falls back to the global `from_name`
/// configured in `[mailer]` when absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FromOverride {
    /// From address, e.g. `"orders@acme.com"`.
    pub email: String,
    /// Optional display name, e.g. `"Acme Orders"`.
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recipient {
    pub email: String,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(default)]
    pub source: Option<String>,
}
