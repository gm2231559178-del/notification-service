use rate_limiter::RateLimitConfig;
use recipient_filter::FilterConfig;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    /// Optional: URL of the business service DB to poll for outbox rows.
    pub outbox_database_url: Option<String>,
    pub amqp: AmqpConfig,
    pub mailer: MailerConfig,
    pub http: HttpConfig,
    /// Port for the Prometheus /metrics endpoint (default: 9091).
    /// Keep this separate from the public API port.
    pub metrics_port: Option<u16>,
    /// Outbound rate-limiting to the mail server.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    /// Recipient block/allow-list.
    #[serde(default)]
    pub filter: FilterConfig,
    /// How long resolved templates are cached in memory (seconds).
    #[serde(default = "default_template_cache_ttl_secs")]
    pub template_cache_ttl_secs: u64,
    /// Maximum size of a single fetched email attachment in bytes.
    #[serde(default = "default_max_attachment_bytes")]
    pub max_attachment_bytes: usize,
    /// Named SMTP sender accounts for multi-tenant / multi-brand deployments.
    ///
    /// Each entry gives a business system its own SMTP credentials and From
    /// address. The publisher selects an account by setting `sender_account`
    /// in the `EmailEvent`. When the field is absent or the name is not found,
    /// the service falls back to the global `[mailer]` config.
    ///
    /// config/default.toml example:
    /// ```toml
    /// [sender_accounts.system_a]
    /// host       = "smtp.gmail.com"
    /// port       = 587
    /// username   = "A@example.com"
    /// password   = "app-password-a"
    /// from_email = "A@example.com"
    /// from_name  = "System A"
    ///
    /// [sender_accounts.system_b]
    /// host       = "smtp.sendgrid.net"
    /// port       = 587
    /// username   = "apikey"
    /// password   = "SG.xxxx"
    /// from_email = "B@example.com"
    /// from_name  = "System B"
    /// ```
    #[serde(default)]
    pub sender_accounts: HashMap<String, SmtpAccountConfig>,
}

/// SMTP credentials for a named sender account.
/// All fields are required — there is no partial fallback to the global mailer.
#[derive(Debug, Deserialize, Clone)]
pub struct SmtpAccountConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub from_email: String,
    pub from_name: String,
}

fn default_max_rl_waits() -> u32 {
    5
}

fn default_template_cache_ttl_secs() -> u64 {
    300
}

fn default_max_attachment_bytes() -> usize {
    10 * 1024 * 1024
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AmqpConfig {
    pub url: String,
    pub queue: String,
    pub exchange: String,
    pub routing_key: String,
    pub max_retries: u32,
    pub retry_base_ms: u64,
    /// Cap on concurrent in-flight message handlers (default: 10).
    pub max_concurrency: usize,
    /// Maximum consecutive rate-limit backoff cycles per recipient (default: 5).
    #[serde(default = "default_max_rl_waits")]
    pub max_rl_waits: u32,
    /// Outbox poll interval in ms (default: 1000). Only used when outbox worker is enabled.
    pub outbox_poll_interval_ms: Option<u64>,
    /// Outbox batch size (default: 50). Only used when outbox worker is enabled.
    pub outbox_batch_size: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    pub port: u16,
    /// When set, all `/emails/*` endpoints require `Authorization: Bearer <api_key>`.
    /// Leave unset only when the API is isolated behind a private network.
    /// Override via `NS_HTTP__API_KEY` environment variable.
    pub api_key: Option<String>,
}

/// Which email backend to use.
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum MailerConfig {
    Smtp {
        host: String,
        port: u16,
        username: String,
        password: String,
        from_email: String,
        from_name: String,
    },
    Webhook {
        url: String,
        auth_token: Option<String>,
    },
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        let cfg = config::Config::builder()
            // Defaults
            .set_default("http.port", 8080)?
            .set_default("metrics_port", 9091)?
            .set_default("amqp.queue", "email.requested")?
            .set_default("amqp.exchange", "notifications")?
            .set_default("amqp.routing_key", "email.requested")?
            .set_default("amqp.max_retries", 3)?
            .set_default("amqp.retry_base_ms", 1000)?
            .set_default("amqp.max_concurrency", 10)?
            // File-based config (optional)
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(config::File::with_name("config/local").required(false))
            // Environment overrides: NS_DATABASE__URL, NS_AMQP__URL, etc.
            .add_source(config::Environment::with_prefix("NS").separator("__"))
            .build()?;

        let app: Self = cfg.try_deserialize()?;
        app.validate()?;
        Ok(app)
    }

    fn validate(&self) -> anyhow::Result<()> {
        use anyhow::bail;

        if self.database.url.is_empty() {
            bail!("database.url must not be empty");
        }
        if self.amqp.url.is_empty() {
            bail!("amqp.url must not be empty");
        }

        match &self.mailer {
            MailerConfig::Smtp {
                host,
                username,
                from_email,
                ..
            } => {
                if host.is_empty() {
                    bail!("mailer.host must not be empty");
                }
                if username.is_empty() {
                    bail!("mailer.username must not be empty");
                }
                if from_email.is_empty() {
                    bail!("mailer.from_email must not be empty");
                }
            }
            MailerConfig::Webhook { url, .. } => {
                if url.is_empty() {
                    bail!("mailer.url must not be empty");
                }
                if !url.starts_with("http://") && !url.starts_with("https://") {
                    bail!("mailer.url must start with http:// or https://");
                }
            }
        }

        // Validate every named sender account at startup so a typo in the
        // config fails fast rather than causing a runtime panic later.
        for (name, acct) in &self.sender_accounts {
            if acct.host.is_empty() {
                bail!("sender_accounts.{name}.host must not be empty");
            }
            if acct.username.is_empty() {
                bail!("sender_accounts.{name}.username must not be empty");
            }
            if acct.from_email.is_empty() {
                bail!("sender_accounts.{name}.from_email must not be empty");
            }
        }

        Ok(())
    }
}
