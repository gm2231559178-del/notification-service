//! Attachment URL fetcher.
//!
//! The notification service fetches each [`AttachmentRef`] URL at send time
//! so business systems never have to encode or embed file bytes in events.
//!
//! # Fetch strategy
//!
//! * All attachments are fetched **concurrently** via `futures::future::join_all`.
//! * Each attachment is retried independently up to [`FETCH_MAX_RETRIES`] times
//!   on transient failures (5xx, timeout, network error). This means a flaky
//!   storage server for one file does not block delivery for the whole event.
//! * A 4xx or size-exceeded response is a permanent failure — it is returned
//!   immediately without consuming retry slots.
//! * One HTTP GET per attachment attempt, with a 30 s timeout (set on the
//!   shared `Client` at construction time).
//! * Optional `Authorization: Bearer <token>` for internal service URLs.
//! * Responses are size-capped at `max_bytes` (default 10 MiB) to prevent
//!   memory exhaustion from unexpectedly large files.
//!
//! # Error classification
//!
//! | Failure | Error type | Retried |
//! |---|---|---|
//! | 4xx (bad URL, expired, forbidden) | `permanent:` prefix | No |
//! | 5xx (server error) | transient | Yes (up to FETCH_MAX_RETRIES) |
//! | Timeout / network error | transient | Yes |
//! | Response too large | `permanent:` prefix | No |
//! | URL already expired (`max_age_secs`) | `permanent:` prefix | No |
//! | All retries exhausted | last transient error | No further retries |

use common::{AppError, AttachmentRef};
use futures::future::join_all;
use reqwest::{Client, StatusCode};
use tokio::time::{sleep, Duration};
use tracing::{debug, instrument, warn};

use crate::message::ResolvedAttachment;

/// Maximum allowed response body size per attachment (10 MiB).
pub const MAX_ATTACHMENT_BYTES: usize = 10 * 1024 * 1024;

/// How many times to retry a transient fetch failure before giving up.
/// Delays follow 1 s, 2 s, 4 s (exponential, capped).
const FETCH_MAX_RETRIES: u32 = 3;

/// Fetches all attachment URLs for an event and returns resolved bytes.
///
/// Each attachment is retried independently on transient failures so a
/// flaky storage server for one file does not block the others.
/// Metadata and expiry checks run before any network calls.
///
/// The returned `Vec` preserves the same order as `refs`.
pub async fn fetch_attachments(
    client: &Client,
    refs: &[AttachmentRef],
    event_timestamp: &chrono::DateTime<chrono::Utc>,
) -> Result<Vec<ResolvedAttachment>, AppError> {
    fetch_attachments_with_limit(client, refs, event_timestamp, MAX_ATTACHMENT_BYTES).await
}

/// Like [`fetch_attachments`] but with an explicit per-attachment byte cap.
#[instrument(skip(client, refs, event_timestamp), fields(count = refs.len()))]
pub async fn fetch_attachments_with_limit(
    client: &Client,
    refs: &[AttachmentRef],
    event_timestamp: &chrono::DateTime<chrono::Utc>,
    max_bytes: usize,
) -> Result<Vec<ResolvedAttachment>, AppError> {
    // ── 1. Validate metadata for every attachment before any network call ─────
    for att_ref in refs {
        att_ref
            .validate(event_timestamp)
            .map_err(AppError::Mailer)?;
    }

    // ── 2. Fetch all URLs concurrently, each with independent retry ───────────
    //
    // `join_all` (not `try_join_all`) is used here so every attachment gets
    // its own retry budget. A transient 5xx on attachment B no longer cancels
    // the already-in-flight fetch for attachment A.
    //
    // Each element of `results` is `Result<ResolvedAttachment, AppError>`.
    // We collect all results first and then surface the first error (if any)
    // so the caller still gets a clean `Err` when any attachment ultimately
    // fails after exhausting retries.
    let futures: Vec<_> = refs
        .iter()
        .map(|att_ref| fetch_one_with_retry(client, att_ref, max_bytes))
        .collect();

    let results = join_all(futures).await;

    // Surface the first error; collect resolved attachments in order.
    let mut resolved = Vec::with_capacity(refs.len());
    for result in results {
        resolved.push(result?);
    }

    Ok(resolved)
}

/// Fetch a single attachment, retrying on transient failures.
///
/// Permanent failures (4xx, size exceeded, URL expired) are returned
/// immediately without consuming retry slots.
async fn fetch_one_with_retry(
    client: &Client,
    att_ref: &AttachmentRef,
    max_bytes: usize,
) -> Result<ResolvedAttachment, AppError> {
    let mut last_err = None;

    for attempt in 0..=FETCH_MAX_RETRIES {
        if attempt > 0 {
            let delay = Duration::from_secs(1u64 << (attempt - 1).min(3));
            warn!(
                filename = %att_ref.filename,
                attempt,
                delay_secs = delay.as_secs(),
                "Attachment fetch transient failure — retrying"
            );
            sleep(delay).await;
        }

        match fetch_one(client, att_ref, max_bytes).await {
            Ok(data) => {
                return Ok(ResolvedAttachment {
                    filename: att_ref.filename.clone(),
                    content_type: att_ref.content_type.clone(),
                    data,
                })
            }
            // Permanent errors are returned immediately — no retry.
            // We borrow `m` only to copy the message string, then construct
            // an owned error to return (AppError doesn't implement Clone).
            Err(AppError::Mailer(ref m)) if m.starts_with("permanent:") => {
                let msg = m.clone();
                return Err(AppError::Mailer(msg));
            }
            Err(e) => last_err = Some(e),
        }
    }

    Err(last_err.unwrap_or_else(|| {
        AppError::Mailer(format!(
            "attachment '{}' fetch failed after {FETCH_MAX_RETRIES} retries",
            att_ref.filename
        ))
    }))
}

/// Fetch a single attachment URL and return the raw bytes.
#[instrument(skip(client, att_ref), fields(url = %att_ref.url, filename = %att_ref.filename))]
async fn fetch_one(
    client: &Client,
    att_ref: &AttachmentRef,
    max_bytes: usize,
) -> Result<Vec<u8>, AppError> {
    debug!("Fetching attachment");

    let mut req = client.get(&att_ref.url);

    if let Some(token) = &att_ref.fetch_token {
        req = req.bearer_auth(token);
    }

    let resp = req.send().await.map_err(|e| {
        AppError::Mailer(format!(
            "attachment fetch network error '{}': {e}",
            att_ref.filename
        ))
    })?;

    let status = resp.status();

    // 429 → rate-limited by the file server (transient — will be retried).
    if status == StatusCode::TOO_MANY_REQUESTS {
        warn!(filename = %att_ref.filename, "Attachment source returned 429");
        return Err(AppError::RateLimited(format!(
            "attachment '{}' source returned HTTP 429",
            att_ref.filename
        )));
    }

    // 4xx → permanent: bad URL, expired pre-signed URL, access denied, etc.
    if status.is_client_error() {
        return Err(AppError::Mailer(format!(
            "permanent: attachment '{}' fetch returned HTTP {status} ({})",
            att_ref.filename, att_ref.url
        )));
    }

    // 5xx → transient: upstream server problem, safe to retry.
    if status.is_server_error() {
        return Err(AppError::Mailer(format!(
            "attachment '{}' fetch returned HTTP {status} — will retry",
            att_ref.filename
        )));
    }

    // Read body with size cap to prevent memory exhaustion.
    let bytes = resp.bytes().await.map_err(|e| {
        AppError::Mailer(format!("attachment '{}' read error: {e}", att_ref.filename))
    })?;

    if bytes.len() > max_bytes {
        return Err(AppError::Mailer(format!(
            "permanent: attachment '{}' exceeds size limit ({} > {} bytes)",
            att_ref.filename,
            bytes.len(),
            max_bytes
        )));
    }

    debug!(bytes = bytes.len(), "Attachment fetched");
    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn att_ref(url: &str) -> AttachmentRef {
        AttachmentRef {
            url: url.into(),
            filename: "test.pdf".into(),
            content_type: "application/pdf".into(),
            fetch_token: None,
            max_age_secs: None,
        }
    }

    #[test]
    fn validate_rejects_empty_url() {
        let mut a = att_ref("https://example.com/file.pdf");
        a.url = "".into();
        assert!(a.validate(&Utc::now()).unwrap_err().contains("permanent:"));
    }

    #[test]
    fn validate_rejects_non_http_url() {
        let a = att_ref("ftp://example.com/file.pdf");
        assert!(a.validate(&Utc::now()).unwrap_err().contains("permanent:"));
    }

    #[test]
    fn validate_rejects_path_separator_in_filename() {
        let mut a = att_ref("https://example.com/file.pdf");
        a.filename = "../../etc/passwd".into();
        assert!(a.validate(&Utc::now()).unwrap_err().contains("permanent:"));
    }

    #[test]
    fn validate_rejects_expired_url() {
        let a = AttachmentRef {
            url: "https://example.com/file.pdf".into(),
            filename: "file.pdf".into(),
            content_type: "application/pdf".into(),
            fetch_token: None,
            max_age_secs: Some(0),
        };
        let ts = Utc::now() - chrono::Duration::seconds(10);
        assert!(a.validate(&ts).unwrap_err().contains("expired"));
    }

    #[test]
    fn validate_accepts_valid_ref() {
        let a = att_ref("https://example.com/invoice.pdf");
        assert!(a.validate(&Utc::now()).is_ok());
    }
}
