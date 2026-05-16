use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::AppError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EmailStatus {
    Pending,
    Sent,
    Failed,
    Blocked,
}

impl EmailStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            EmailStatus::Pending => "PENDING",
            EmailStatus::Sent => "SENT",
            EmailStatus::Failed => "FAILED",
            EmailStatus::Blocked => "BLOCKED",
        }
    }
}

impl TryFrom<&str> for EmailStatus {
    type Error = AppError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "PENDING" => Ok(EmailStatus::Pending),
            "SENT" => Ok(EmailStatus::Sent),
            "FAILED" => Ok(EmailStatus::Failed),
            "BLOCKED" => Ok(EmailStatus::Blocked),
            other => Err(AppError::UnknownStatus(other.to_owned())),
        }
    }
}

impl std::fmt::Display for EmailStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Maps 1-to-1 with the `email_log` PostgreSQL table.
///
/// Keyed by `(event_id, recipient_email)` — one row per recipient per event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailLog {
    pub id: Uuid,
    pub event_id: Uuid,
    /// The specific recipient this row tracks.
    pub recipient_email: String,
    /// Optional display name for the recipient (e.g. "Alice Smith").
    /// Stored so it can be faithfully re-published on manual retry,
    /// preventing templates that use {{name}} from rendering the raw
    /// placeholder on retried deliveries.
    /// Nullable for rows written before migration 0011.
    pub recipient_name: Option<String>,
    pub event_type: String,
    pub status: EmailStatus,
    /// How many automatic retry attempts have been made in the current attempt
    /// window.  Reset to 0 when an operator manually retries via the HTTP API
    /// so the recipient gets a fresh set of automatic retries.
    pub retry_count: i32,
    /// Lifetime delivery attempt counter — never reset, even on manual retry.
    /// Useful for auditing and detecting persistently failing addresses.
    pub total_attempts: i32,
    pub last_error: Option<String>,
    /// Original template payload stored for retry reconstruction.
    /// Nullable for rows written before migration 0007.
    pub payload: Option<serde_json::Value>,
    /// Per-event From address override stored for retry reconstruction.
    /// Nullable for rows written before migration 0009.
    pub from_override: Option<serde_json::Value>,
    /// URL-based attachment references stored for retry reconstruction.
    /// Nullable for rows written before migration 0009.
    pub attachments: Option<serde_json::Value>,
    /// Named SMTP sender account used for the original delivery.
    /// Stored so manual retries via the HTTP API send from the same account.
    /// NULL means the global [mailer] default was used.
    /// Nullable for rows written before migration 0014.
    pub sender_account: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
