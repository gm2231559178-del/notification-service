//! DB-backed template store with a moka TTL cache.
//!
//! The cache key is `"{channel}:{event_type}"`.  Entries expire automatically
//! after `cache_ttl` (default 5 minutes).  For immediate invalidation call
//! [`TemplateStore::invalidate`] or [`TemplateStore::invalidate_all`] via the
//! `DELETE /templates/{event_type}/cache` or `DELETE /templates/cache` endpoints.
//!
//! # `.sqlx` offline cache
//!
//! Every `sqlx::query!` call site in this file has a corresponding entry in
//! `.sqlx/query-<sha256>.json` at the workspace root.  The hash is the
//! SHA-256 of the **exact** query string as it appears in the macro — including
//! all whitespace and newlines.  If you reformat a query string (even just
//! re-indenting), the hash will no longer match and `SQLX_OFFLINE=true` builds
//! will fail with "query not found in offline data".  Run `cargo sqlx prepare`
//! after any query edit to regenerate the cache file.

use std::time::Duration;

use chrono::{DateTime, Utc};
use common::AppError;
use moka::future::Cache;
use sqlx::PgPool;
use tracing::{debug, info, instrument};

/// Trait for resolving templates by (event_type, channel).
///
/// The consumer only needs the `resolve` method.  Extracting it into a trait
/// allows mock implementations for unit testing without a live database.
#[async_trait::async_trait]
pub trait TemplateResolver: Send + Sync {
    async fn resolve(
        &self,
        event_type: &str,
        channel: &str,
    ) -> Result<NotificationTemplate, AppError>;
}

/// One template row from the `notification_template` table.
#[derive(Debug, Clone)]
pub struct NotificationTemplate {
    pub subject: String,
    pub body_html: String,
    pub body_text: String,
}

// Back-compat alias.
pub use NotificationTemplate as EmailTemplate;

/// A full template row returned by [`TemplateStore::list`] and [`TemplateStore::get`].
#[derive(Debug, Clone)]
pub struct TemplateRow {
    pub event_type: String,
    pub channel: String,
    pub subject: String,
    pub body_html: String,
    pub body_text: String,
    pub version: i32,
    pub active: bool,
    pub updated_at: DateTime<Utc>,
}

/// DB-backed template store with a moka TTL cache.
///
/// `Clone` is cheap — all clones share the same underlying moka cache.
#[derive(Clone)]
pub struct TemplateStore {
    pool: PgPool,
    cache: Cache<String, NotificationTemplate>,
}

impl TemplateStore {
    /// Construct with an explicit TTL.  Pass `Duration::ZERO` to disable caching.
    pub fn new_with_ttl(pool: PgPool, cache_ttl: Duration) -> Self {
        let cache = if cache_ttl.is_zero() {
            Cache::builder().max_capacity(0).build()
        } else {
            Cache::builder()
                .max_capacity(1_024) // up to 1 k distinct event-type × channel pairs
                .time_to_live(cache_ttl)
                .build()
        };
        Self { pool, cache }
    }

    /// Construct with the default TTL (5 minutes).
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_ttl(pool, Duration::from_secs(300))
    }

    /// Resolve the template for `(event_type, channel)`.
    ///
    /// Returns `AppError::Template` for unknown pairs so the consumer
    /// immediately routes to DLQ without wasting retry slots.
    #[instrument(skip(self), fields(event_type, channel))]
    pub async fn resolve(
        &self,
        event_type: &str,
        channel: &str,
    ) -> Result<NotificationTemplate, AppError> {
        let key = format!("{channel}:{event_type}");

        if let Some(tpl) = self.cache.get(&key).await {
            debug!("Template cache hit");
            return Ok(tpl);
        }

        let row = sqlx::query!(
            r#"
            SELECT subject, body_html, body_text
            FROM   notification_template
            WHERE  type = $1 AND channel = $2 AND active = TRUE
            "#,
            event_type,
            channel,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::Database)?;

        let Some(r) = row else {
            return Err(AppError::Template(format!(
                "Unknown event type '{event_type}' for channel '{channel}' \
                 — add a row to notification_template"
            )));
        };

        let tpl = NotificationTemplate {
            subject: r.subject,
            body_html: r.body_html,
            body_text: r.body_text,
        };
        self.cache.insert(key, tpl.clone()).await;
        info!("Template loaded from DB and cached");
        Ok(tpl)
    }

    /// Return all template rows ordered by type then channel.
    ///
    /// Unlike [`resolve`], this returns inactive templates too so operators
    /// can inspect the full state of the table.
    pub async fn list(&self) -> Result<Vec<TemplateRow>, AppError> {
        let rows = sqlx::query!(
            r#"
            SELECT type, channel, subject, body_html, body_text, version, active, updated_at
            FROM   notification_template
            ORDER  BY type, channel
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(AppError::Database)?;

        Ok(rows
            .into_iter()
            .map(|r| TemplateRow {
                event_type: r.r#type,
                channel: r.channel,
                subject: r.subject,
                body_html: r.body_html,
                body_text: r.body_text,
                version: r.version,
                active: r.active,
                updated_at: r.updated_at,
            })
            .collect())
    }

    /// Return all channel variants for a single event type.
    ///
    /// Returns an empty `Vec` when no rows exist for that type (caller decides
    /// whether to surface a 404).
    pub async fn get(&self, event_type: &str) -> Result<Vec<TemplateRow>, AppError> {
        let rows = sqlx::query!(
            r#"
            SELECT type, channel, subject, body_html, body_text, version, active, updated_at
            FROM   notification_template
            WHERE  type = $1
            ORDER  BY channel
            "#,
            event_type,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(AppError::Database)?;

        Ok(rows
            .into_iter()
            .map(|r| TemplateRow {
                event_type: r.r#type,
                channel: r.channel,
                subject: r.subject,
                body_html: r.body_html,
                body_text: r.body_text,
                version: r.version,
                active: r.active,
                updated_at: r.updated_at,
            })
            .collect())
    }

    /// Evict all cache entries for `event_type` across every channel.
    ///
    /// Cache keys are `"{channel}:{event_type}"` so a plain `remove(event_type)`
    /// would miss multi-channel entries.  This method iterates all keys and
    /// drops those whose suffix matches `":{event_type}"`.
    pub async fn invalidate(&self, event_type: &str) {
        let suffix = format!(":{event_type}");
        // moka doesn't expose key iteration for async cache; collect the matching
        // keys from a sync snapshot of the entry set via run_pending_tasks first.
        self.cache.run_pending_tasks().await;
        let keys: Vec<String> = self
            .cache
            .iter()
            .filter(|(k, _)| k.ends_with(&suffix))
            .map(|(k, _)| k.as_ref().clone())
            .collect();
        let count = keys.len();
        for k in keys {
            self.cache.invalidate(&k).await;
        }
        info!(
            event_type,
            removed = count,
            "Template cache entries invalidated"
        );
    }

    /// Upsert a template row for `(event_type, channel)`.
    ///
    /// Inserts a new row or, on conflict, updates subject/body/active fields
    /// and bumps `version` only when content has actually changed (mirrors the
    /// migration's `ON CONFLICT DO UPDATE` logic).
    ///
    /// `active = false` lets operators stage a disabled template without it
    /// being picked up by the consumer until explicitly enabled.
    ///
    /// Returns `(version, inserted)` where `inserted` is `true` for a new row
    /// and `false` for an update.
    pub async fn upsert(
        &self,
        event_type: &str,
        channel: &str,
        subject: &str,
        body_html: &str,
        body_text: &str,
        active: bool,
    ) -> Result<(i32, bool), AppError> {
        let row = sqlx::query!(
            r#"
            INSERT INTO notification_template (type, channel, subject, body_html, body_text, active)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (type, channel) DO UPDATE
                SET subject    = EXCLUDED.subject,
                    body_html  = EXCLUDED.body_html,
                    body_text  = EXCLUDED.body_text,
                    active     = EXCLUDED.active,
                    version    = CASE
                                     WHEN notification_template.subject   IS DISTINCT FROM EXCLUDED.subject
                                       OR notification_template.body_html IS DISTINCT FROM EXCLUDED.body_html
                                       OR notification_template.body_text IS DISTINCT FROM EXCLUDED.body_text
                                     THEN notification_template.version + 1
                                     ELSE notification_template.version
                                 END,
                    updated_at = CASE
                                     WHEN notification_template.subject   IS DISTINCT FROM EXCLUDED.subject
                                       OR notification_template.body_html IS DISTINCT FROM EXCLUDED.body_html
                                       OR notification_template.body_text IS DISTINCT FROM EXCLUDED.body_text
                                     THEN now()
                                     ELSE notification_template.updated_at
                                 END
            RETURNING version, (xmax = 0) AS "inserted!: bool"
            "#,
            event_type,
            channel,
            subject,
            body_html,
            body_text,
            active,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(AppError::Database)?;

        // Invalidate the cache entry so the next delivery picks up the new content.
        self.invalidate(event_type).await;

        Ok((row.version, row.inserted))
    }

    /// Clear the entire template cache.
    pub async fn invalidate_all(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
        info!("Template cache cleared");
    }

    /// Expose the underlying pool for use by the readiness probe.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Atomically patch a template row for `(event_type, channel)`.
    ///
    /// Only fields supplied as `Some(...)` are written; `None` leaves the
    /// existing column value unchanged.  `version` is bumped and `updated_at`
    /// advanced only when the content columns actually change.
    ///
    /// Returns `Some((version, active))` for the updated row, or `None` when
    /// no row exists for `(event_type, channel)`.
    pub async fn patch(
        &self,
        event_type: &str,
        channel: &str,
        subject: Option<&str>,
        body_html: Option<&str>,
        body_text: Option<&str>,
        active: Option<bool>,
    ) -> Result<Option<(i32, bool)>, AppError> {
        let row = sqlx::query!(
            r#"
            UPDATE notification_template
            SET subject    = COALESCE($3, subject),
                body_html  = COALESCE($4, body_html),
                body_text  = COALESCE($5, body_text),
                active     = COALESCE($6::boolean, active),
                version    = CASE
                                 WHEN subject   IS DISTINCT FROM COALESCE($3, subject)
                                   OR body_html IS DISTINCT FROM COALESCE($4, body_html)
                                   OR body_text IS DISTINCT FROM COALESCE($5, body_text)
                                 THEN version + 1
                                 ELSE version
                             END,
                updated_at = CASE
                                 WHEN subject   IS DISTINCT FROM COALESCE($3, subject)
                                   OR body_html IS DISTINCT FROM COALESCE($4, body_html)
                                   OR body_text IS DISTINCT FROM COALESCE($5, body_text)
                                 THEN now()
                                 ELSE updated_at
                             END
            WHERE type = $1 AND channel = $2
            RETURNING version, active
            "#,
            event_type,
            channel,
            subject,
            body_html,
            body_text,
            active,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::Database)?;

        if row.is_some() {
            self.invalidate(event_type).await;
        }

        Ok(row.map(|r| (r.version, r.active)))
    }
}

#[async_trait::async_trait]
impl TemplateResolver for TemplateStore {
    async fn resolve(
        &self,
        event_type: &str,
        channel: &str,
    ) -> Result<NotificationTemplate, AppError> {
        self.resolve(event_type, channel).await
    }
}
