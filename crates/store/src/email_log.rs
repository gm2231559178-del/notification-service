use common::{AppError, EmailLog, EmailStatus};
use sqlx::PgPool;
use tracing::instrument;
use uuid::Uuid;

/// All database operations for the `email_log` table.
///
/// Keyed by `(event_id, recipient_email)` — one row per recipient per event.
#[derive(Clone)]
pub struct EmailLogStore {
    pool: PgPool,
}

impl EmailLogStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert a PENDING row for one recipient.
    /// Returns `AppError::Duplicate` if `(event_id, recipient_email)` already exists.
    ///
    /// `payload`, `from_override`, and `attachments` are stored for retry
    /// reconstruction so the full original event can be re-published on manual retry.
    /// `recipient_name` is stored so the display name survives retries.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, payload, from_override, attachments))]
    pub async fn insert_pending(
        &self,
        event_id: Uuid,
        event_type: &str,
        recipient_email: &str,
        recipient_name: Option<&str>,
        payload: &serde_json::Value,
        from_override: Option<&serde_json::Value>,
        attachments: Option<&serde_json::Value>,
        sender_account: Option<&str>,
    ) -> Result<Uuid, AppError> {
        let row = sqlx::query!(
            r#"
            INSERT INTO email_log
                (event_id, event_type, recipient_email, recipient_name, status, payload, from_override, attachments, sender_account)
            VALUES ($1, $2, $3, $4, 'PENDING', $5, $6, $7, $8)
            ON CONFLICT (event_id, recipient_email) DO NOTHING
            RETURNING id
            "#,
            event_id,
            event_type,
            recipient_email,
            recipient_name,
            payload,
            from_override,
            attachments,
            sender_account,
        )
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok(r.id),
            None => Err(AppError::Duplicate(format!("{event_id}/{recipient_email}"))),
        }
    }

    #[instrument(skip(self))]
    pub async fn mark_sent(&self, event_id: Uuid, recipient_email: &str) -> Result<(), AppError> {
        sqlx::query!(
            "UPDATE email_log SET status='SENT', updated_at=now() WHERE event_id=$1 AND recipient_email=$2",
            event_id, recipient_email,
        ).execute(&self.pool).await?;
        Ok(())
    }

    #[instrument(skip(self, error_msg))]
    pub async fn mark_failed(
        &self,
        event_id: Uuid,
        recipient_email: &str,
        error_msg: &str,
        exhausted: bool,
    ) -> Result<(), AppError> {
        let status = if exhausted { "FAILED" } else { "PENDING" };
        sqlx::query!(
            r#"UPDATE email_log
               SET retry_count=retry_count+1, total_attempts=total_attempts+1,
                   last_error=$3, status=$4, updated_at=now()
               WHERE event_id=$1 AND recipient_email=$2"#,
            event_id,
            recipient_email,
            error_msg,
            status,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[instrument(skip(self, reason))]
    pub async fn mark_blocked(
        &self,
        event_id: Uuid,
        recipient_email: &str,
        reason: &str,
    ) -> Result<(), AppError> {
        sqlx::query!(
            "UPDATE email_log SET status='BLOCKED', last_error=$3, updated_at=now() WHERE event_id=$1 AND recipient_email=$2",
            event_id, recipient_email, reason,
        ).execute(&self.pool).await?;
        Ok(())
    }

    /// Fetch all delivery rows for an event (one per recipient).
    #[instrument(skip(self))]
    pub async fn get_by_event_id(&self, event_id: Uuid) -> Result<Vec<EmailLog>, AppError> {
        let rows = sqlx::query!(
            r#"SELECT id, event_id, event_type, recipient_email, recipient_name, status, retry_count,
                      total_attempts, last_error, payload, from_override, attachments, sender_account, created_at, updated_at
               FROM email_log WHERE event_id=$1 ORDER BY created_at"#,
            event_id,
        )
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Err(AppError::NotFound(event_id.to_string()));
        }

        Ok(rows
            .into_iter()
            .map(|r| {
                Ok(EmailLog {
                    id: r.id,
                    event_id: r.event_id,
                    recipient_email: r.recipient_email,
                    recipient_name: r.recipient_name,
                    event_type: r.event_type,
                    status: EmailStatus::try_from(r.status.as_str())?,
                    retry_count: r.retry_count,
                    total_attempts: r.total_attempts,
                    last_error: r.last_error,
                    payload: r.payload,
                    from_override: r.from_override,
                    attachments: r.attachments,
                    sender_account: r.sender_account,
                    created_at: r.created_at,
                    updated_at: r.updated_at,
                })
            })
            .collect::<Result<Vec<_>, AppError>>()?)
    }

    /// Fetch the row for a single recipient within an event.
    #[instrument(skip(self))]
    pub async fn get_by_event_and_recipient(
        &self,
        event_id: Uuid,
        recipient_email: &str,
    ) -> Result<EmailLog, AppError> {
        let r = sqlx::query!(
            r#"SELECT id, event_id, event_type, recipient_email, recipient_name, status, retry_count,
                      total_attempts, last_error, payload, from_override, attachments, sender_account, created_at, updated_at
               FROM email_log WHERE event_id=$1 AND recipient_email=$2"#,
            event_id,
            recipient_email,
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{event_id}/{recipient_email}")))?;

        Ok(EmailLog {
            id: r.id,
            event_id: r.event_id,
            recipient_email: r.recipient_email,
            recipient_name: r.recipient_name,
            event_type: r.event_type,
            status: EmailStatus::try_from(r.status.as_str())?,
            retry_count: r.retry_count,
            total_attempts: r.total_attempts,
            last_error: r.last_error,
            payload: r.payload,
            from_override: r.from_override,
            attachments: r.attachments,
            sender_account: r.sender_account,
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
    }

    /// Reset a single FAILED recipient row to PENDING for manual replay.
    /// Uses an atomic UPDATE … WHERE status='FAILED' to avoid the TOCTOU race
    /// that existed in the old fetch-then-update approach.
    ///
    /// `retry_count` is reset to 0 so the recipient gets a full fresh set of
    /// automatic retries.  `total_attempts` is intentionally NOT reset — it is
    /// a lifetime counter used for auditing and detecting persistently failing
    /// addresses.
    #[instrument(skip(self))]
    pub async fn reset_for_retry(
        &self,
        event_id: Uuid,
        recipient_email: &str,
    ) -> Result<(), AppError> {
        let result = sqlx::query!(
            r#"UPDATE email_log SET status='PENDING', retry_count=0, last_error=NULL, updated_at=now()
               WHERE event_id=$1 AND recipient_email=$2 AND status='FAILED'"#,
            event_id, recipient_email,
        ).execute(&self.pool).await?;

        if result.rows_affected() == 0 {
            return Err(AppError::NotFound(format!(
                "No FAILED record for {event_id}/{recipient_email}"
            )));
        }
        Ok(())
    }

    /// Atomically reset ALL FAILED rows for an event to PENDING in a single
    /// UPDATE, returning the email addresses that were actually reset.
    ///
    /// `retry_count` is reset to 0 so each recipient gets a fresh set of
    /// automatic retries.  `total_attempts` is intentionally NOT reset — it
    /// is a lifetime counter preserved across manual retries for auditing.
    #[instrument(skip(self))]
    pub async fn reset_all_failed_for_event(
        &self,
        event_id: Uuid,
    ) -> Result<Vec<String>, AppError> {
        let rows = sqlx::query!(
            r#"UPDATE email_log
               SET    status='PENDING', retry_count=0, last_error=NULL, updated_at=now()
               WHERE  event_id=$1 AND status='FAILED'
               RETURNING recipient_email"#,
            event_id,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.recipient_email).collect())
    }

    /// Return the retry_count for a specific recipient row (seeds in-memory counter after restart).
    #[instrument(skip(self))]
    pub async fn get_retry_count(
        &self,
        event_id: Uuid,
        recipient_email: &str,
    ) -> Result<i32, AppError> {
        let row = sqlx::query!(
            "SELECT retry_count FROM email_log WHERE event_id=$1 AND recipient_email=$2",
            event_id,
            recipient_email,
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{event_id}/{recipient_email}")))?;
        Ok(row.retry_count)
    }

    /// Expose the underlying pool for health checks.
    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}
