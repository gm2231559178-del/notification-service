//! DB-backed block/allow-list with a moka TTL cache.
//!
//! # How it works
//!
//! On every `check` call the store tries the moka cache first (O(1),
//! lock-free reads).  On a miss it loads **all** active rows from the DB,
//! rebuilds the four `HashSet`s, and writes them under the key `"block_list"`
//! with the configured TTL.  Subsequent calls within the TTL window hit the
//! cache directly.
//!
//! Cache invalidation (e.g. after a POST/DELETE to `/admin/blocklist`) calls
//! `invalidate()`, which removes the single cache entry so the next `check`
//! reloads from the DB.
//!
//! # Thread safety
//!
//! `moka::future::Cache` is `Clone + Send + Sync`.  `BlockListStore` is also
//! `Clone` (it wraps the cache in an `Arc` internally via moka) so it can be
//! passed into handlers and processor tasks like the existing `TemplateStore`.

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use common::AppError;
use moka::future::Cache;
use sqlx::PgPool;
use tracing::{debug, info};

const CACHE_KEY: &str = "block_list";

/// Trait for checking whether an email address is blocked.
///
/// Scoped to the consumer's `check()` call so that tests can substitute a
/// mock without needing a real DB pool.
#[async_trait]
pub trait BlockListChecker: Send + Sync {
    async fn check(&self, email: &str) -> Result<(), AppError>;
}

/// A snapshot of all active block/allow-list entries loaded from the DB.
#[derive(Clone, Default)]
struct BlockListSnapshot {
    blocked_emails: HashSet<String>,
    blocked_domains: HashSet<String>,
    allowed_emails: HashSet<String>,
    allowed_domains: HashSet<String>,
    /// True when at least one allowlist entry is present.
    allowlist_mode: bool,
}

/// A block_list row as returned by the DB query.
#[derive(Debug, Clone)]
pub struct BlockListEntry {
    pub id: i64,
    pub kind: String,
    pub value: String,
    pub reason: Option<String>,
    pub active: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// DB-backed block/allow-list store with a moka TTL cache.
///
/// Construct via [`BlockListStore::new`] and clone freely — all clones share
/// the same underlying moka cache.
#[derive(Clone)]
pub struct BlockListStore {
    pool: PgPool,
    cache: Cache<&'static str, BlockListSnapshot>,
}

impl BlockListStore {
    /// Construct with an explicit cache TTL.
    ///
    /// `ttl = Duration::ZERO` disables caching (every `check` hits the DB).
    pub fn new(pool: PgPool, ttl: Duration) -> Self {
        let cache = if ttl.is_zero() {
            // max_capacity(0) means nothing is ever stored — effective no-op cache.
            Cache::builder().max_capacity(0).build()
        } else {
            Cache::builder()
                .max_capacity(1) // Only one entry: the full snapshot.
                .time_to_live(ttl)
                .build()
        };
        Self { pool, cache }
    }

    /// Returns `Ok(())` if `email` may receive mail, `Err(AppError::Blocked)`
    /// if it must be dropped.  Loads from cache when fresh; hits DB on miss.
    pub async fn check(&self, email: &str) -> Result<(), AppError> {
        let snap = self.snapshot().await?;
        let email_lc = email.to_lowercase();
        let domain = email_lc.rsplit_once('@').map(|(_, d)| d.to_string());

        // Blocklist always wins.
        if snap.blocked_emails.contains(&email_lc) {
            debug!(email, "Recipient is on the DB email blocklist");
            return Err(AppError::Blocked(format!(
                "{email} is on the blocked-email list"
            )));
        }
        if let Some(ref d) = domain {
            if snap.blocked_domains.contains(d) {
                debug!(email, domain = %d, "Recipient domain is on the DB blocklist");
                return Err(AppError::Blocked(format!(
                    "{email}: domain '{d}' is on the blocked-domain list"
                )));
            }
        }

        // Allowlist mode.
        if snap.allowlist_mode {
            let ok = snap.allowed_emails.contains(&email_lc)
                || domain
                    .as_ref()
                    .map(|d| snap.allowed_domains.contains(d))
                    .unwrap_or(false);
            if !ok {
                debug!(email, "Recipient not on DB allowlist");
                return Err(AppError::Blocked(format!(
                    "{email} is not on the allowed list (allowlist mode active)"
                )));
            }
        }

        Ok(())
    }

    /// Evict the cached snapshot so the next `check` reloads from the DB.
    pub async fn invalidate(&self) {
        self.cache.invalidate(CACHE_KEY).await;
        info!("BlockListStore cache invalidated");
    }

    /// Returns `true` when no active entries exist in the DB (passthrough).
    pub async fn is_empty(&self) -> bool {
        self.snapshot()
            .await
            .map(|s| {
                s.blocked_emails.is_empty() && s.blocked_domains.is_empty() && !s.allowlist_mode
            })
            .unwrap_or(false)
    }

    // ── Admin CRUD ────────────────────────────────────────────────────────────

    /// Insert a new entry (or reactivate a soft-deleted one).
    ///
    /// `kind` must be one of: `blocked_email`, `blocked_domain`,
    /// `allowed_email`, `allowed_domain`.
    pub async fn add_entry(
        &self,
        kind: &str,
        value: &str,
        reason: Option<&str>,
    ) -> Result<BlockListEntry, AppError> {
        let value_lc = value.to_lowercase();
        let row = sqlx::query!(
            r#"
            INSERT INTO block_list (kind, value, reason, active)
            VALUES ($1, $2, $3, TRUE)
            ON CONFLICT (kind, lower(value))
            DO UPDATE SET active = TRUE, reason = EXCLUDED.reason, updated_at = now()
            RETURNING id, kind, value, reason, active, created_at
            "#,
            kind,
            value_lc,
            reason,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(AppError::Database)?;

        self.invalidate().await;
        Ok(BlockListEntry {
            id: row.id,
            kind: row.kind,
            value: row.value,
            reason: row.reason,
            active: row.active,
            created_at: row.created_at,
        })
    }

    /// Soft-delete an entry by id (sets `active = FALSE`).
    ///
    /// Returns `AppError::NotFound` when no active row has that id.
    pub async fn remove_entry(&self, id: i64) -> Result<(), AppError> {
        let result = sqlx::query!(
            "UPDATE block_list SET active = FALSE, updated_at = now() WHERE id = $1 AND active = TRUE",
            id
        )
        .execute(&self.pool)
        .await
        .map_err(AppError::Database)?;

        if result.rows_affected() == 0 {
            return Err(AppError::NotFound(format!(
                "No active block_list entry with id {id}"
            )));
        }
        self.invalidate().await;
        Ok(())
    }

    /// List all active entries.
    pub async fn list_entries(&self) -> Result<Vec<BlockListEntry>, AppError> {
        let rows = sqlx::query!(
            "SELECT id, kind, value, reason, active, created_at FROM block_list WHERE active = TRUE ORDER BY id"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(AppError::Database)?;

        Ok(rows
            .into_iter()
            .map(|r| BlockListEntry {
                id: r.id,
                kind: r.kind,
                value: r.value,
                reason: r.reason,
                active: r.active,
                created_at: r.created_at,
            })
            .collect())
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    async fn snapshot(&self) -> Result<BlockListSnapshot, AppError> {
        if let Some(snap) = self.cache.get(CACHE_KEY).await {
            return Ok(snap);
        }

        let rows = sqlx::query!("SELECT kind, value FROM block_list WHERE active = TRUE")
            .fetch_all(&self.pool)
            .await
            .map_err(AppError::Database)?;

        let mut snap = BlockListSnapshot::default();
        for row in rows {
            match row.kind.as_str() {
                "blocked_email" => {
                    snap.blocked_emails.insert(row.value);
                }
                "blocked_domain" => {
                    snap.blocked_domains.insert(row.value);
                }
                "allowed_email" => {
                    snap.allowed_emails.insert(row.value);
                    snap.allowlist_mode = true;
                }
                "allowed_domain" => {
                    snap.allowed_domains.insert(row.value);
                    snap.allowlist_mode = true;
                }
                other => tracing::warn!(kind = other, "Unknown block_list kind — skipping"),
            }
        }

        debug!(
            blocked_emails = snap.blocked_emails.len(),
            blocked_domains = snap.blocked_domains.len(),
            allowed_emails = snap.allowed_emails.len(),
            allowed_domains = snap.allowed_domains.len(),
            "BlockListStore snapshot loaded from DB"
        );

        self.cache.insert(CACHE_KEY, snap.clone()).await;
        Ok(snap)
    }
}

#[async_trait]
impl BlockListChecker for BlockListStore {
    async fn check(&self, email: &str) -> Result<(), AppError> {
        BlockListStore::check(self, email).await
    }
}
