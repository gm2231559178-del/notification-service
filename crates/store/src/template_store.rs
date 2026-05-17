use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::AppError;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument};

/// One template row from the `email_template` table.
#[derive(Debug, Clone)]
pub struct EmailTemplate {
    pub subject: String,
    pub body_html: String,
    pub body_text: String,
}

/// A cached template entry together with the instant it was populated.
#[derive(Debug, Clone)]
struct CacheEntry {
    template: EmailTemplate,
    inserted_at: Instant,
}

/// DB-backed template store with an in-memory read-through cache.
///
/// # Lookup order
///
/// 1. In-memory cache — returned immediately when the entry is younger than
///    `cache_ttl`.  A TTL of zero disables the cache entirely (always hits DB).
/// 2. Database — on cache miss or expired entry, the row is fetched, cached,
///    and returned.
/// 3. If no active row exists, `AppError::Template` is returned so the consumer
///    immediately marks the delivery FAILED (no retries, routes to DLQ).
///    Add a row to `email_template` to register a new event type.
///
/// # Cache invalidation
///
/// Entries expire automatically after `cache_ttl` (default 5 minutes).
/// For immediate invalidation without a restart, call
/// [`TemplateStore::invalidate`] / [`TemplateStore::reload_all`] via the
/// `DELETE /templates/cache` or `DELETE /templates/:event_type/cache` endpoints.
///
/// For most deployments the TTL is short enough that operator edits to the
/// `email_template` table take effect within minutes automatically.
#[derive(Clone)]
pub struct TemplateStore {
    pool: PgPool,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    cache_ttl: Duration,
}

impl TemplateStore {
    /// Construct with an explicit TTL.  Pass `Duration::ZERO` to disable caching.
    pub fn new_with_ttl(pool: PgPool, cache_ttl: Duration) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl,
        }
    }

    /// Construct with the default TTL (5 minutes).
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_ttl(pool, Duration::from_secs(300))
    }

    /// Resolve the template for `event_type`.
    ///
    /// Returns `AppError::Template` for unknown event types so the message is
    /// immediately routed to DLQ without wasting retry slots (same semantics
    /// as the old `templates_for()` function).
    #[instrument(skip(self), fields(event_type))]
    pub async fn resolve(&self, event_type: &str) -> Result<EmailTemplate, AppError> {
        // ── 1. Cache hit (only when TTL is non-zero and entry is fresh) ───────
        if !self.cache_ttl.is_zero() {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(event_type) {
                if entry.inserted_at.elapsed() < self.cache_ttl {
                    debug!("Template cache hit");
                    return Ok(entry.template.clone());
                }
                debug!("Template cache expired");
            }
        }

        // ── 2. DB lookup ──────────────────────────────────────────────────────
        let row = sqlx::query!(
            r#"
            SELECT subject, body_html, body_text
            FROM   email_template
            WHERE  type = $1 AND active = TRUE
            "#,
            event_type,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::Database)?;

        if let Some(r) = row {
            let tpl = EmailTemplate {
                subject: r.subject,
                body_html: r.body_html,
                body_text: r.body_text,
            };
            self.cache.write().await.insert(
                event_type.to_owned(),
                CacheEntry {
                    template: tpl.clone(),
                    inserted_at: Instant::now(),
                },
            );
            info!("Template loaded from DB and cached");
            return Ok(tpl);
        }

        // No DB row and no fallback — the event type is unknown.
        // This is a permanent error: the consumer will mark the delivery FAILED
        // immediately (no retries) so the message does not burn retry slots
        // or block other recipients.
        //
        // To add a new template, INSERT a row into email_template and call
        // DELETE /templates/<event_type>/cache (or wait for the TTL to expire).
        Err(AppError::Template(format!(
            "Unknown event type '{event_type}' — add a row to email_template"
        )))
    }

    /// Remove a single entry from the cache, forcing the next lookup to
    /// re-fetch from the database. Useful after an operator edits a template row.
    pub async fn invalidate(&self, event_type: &str) {
        self.cache.write().await.remove(event_type);
        info!(event_type, "Template cache entry invalidated");
    }

    /// Clear the entire cache, forcing all subsequent lookups to hit the DB.
    pub async fn reload_all(&self) {
        self.cache.write().await.clear();
        info!("Template cache cleared");
    }
}
