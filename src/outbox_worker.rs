//! Standalone outbox-worker binary.
//!
//! Connects to the **business** database (read/write on the `outbox` table)
//! and the shared RabbitMQ broker.  It has NO connection to the
//! notification-service's own PostgreSQL database — that isolation is the
//! entire point of running this as a separate container.
//!
//! Configuration is via environment variables (NS_OUTBOX__ prefix):
//!
//!   NS_OUTBOX__DATABASE_URL   — business DB  (required)
//!   NS_OUTBOX__AMQP_URL       — RabbitMQ URL (required)
//!   NS_OUTBOX__EXCHANGE       — default: notifications
//!   NS_OUTBOX__ROUTING_KEY    — default: email.requested
//!   NS_OUTBOX__POLL_INTERVAL_MS — default: 1000
//!   NS_OUTBOX__BATCH_SIZE     — default: 50

use anyhow::Context;
use outbox::{run_outbox_worker, OutboxConfig};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Debug, Deserialize)]
struct OutboxEnv {
    database_url: String,
    amqp_url: String,
    #[serde(default = "default_exchange")]
    exchange: String,
    #[serde(default = "default_routing_key")]
    routing_key: String,
    #[serde(default = "default_poll_interval_ms")]
    poll_interval_ms: u64,
    #[serde(default = "default_batch_size")]
    batch_size: i64,
    /// Max connections in the outbox DB pool (NS_OUTBOX__POOL_SIZE, default: 5).
    #[serde(default = "default_pool_size")]
    pool_size: u32,
}

fn default_exchange() -> String {
    "notifications".into()
}
fn default_routing_key() -> String {
    "email.requested".into()
}
fn default_poll_interval_ms() -> u64 {
    1_000
}
fn default_batch_size() -> i64 {
    50
}
fn default_pool_size() -> u32 {
    5
}

impl OutboxEnv {
    fn load() -> anyhow::Result<Self> {
        let cfg = config::Config::builder()
            .add_source(config::Environment::with_prefix("NS_OUTBOX").separator("__"))
            .build()?;
        let env: Self = cfg.try_deserialize()?;
        if env.database_url.is_empty() {
            anyhow::bail!("NS_OUTBOX__DATABASE_URL must not be empty");
        }
        if env.amqp_url.is_empty() {
            anyhow::bail!("NS_OUTBOX__AMQP_URL must not be empty");
        }
        Ok(env)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Tracing ───────────────────────────────────────────────────────────────
    // LOG_FORMAT=json   → structured JSON (default in Docker / production)
    // LOG_FORMAT=pretty  → human-readable coloured output (local dev)
    // LOG_FORMAT=compact → human-readable, no colours (CI / plain terminals)
    let log_format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "json".into());
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let registry = tracing_subscriber::registry().with(filter);
    match log_format.to_lowercase().as_str() {
        "pretty" => registry
            .with(tracing_subscriber::fmt::layer().pretty())
            .init(),
        "compact" => registry
            .with(tracing_subscriber::fmt::layer().compact())
            .init(),
        _ => registry
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
    }

    // ── Config ────────────────────────────────────────────────────────────────
    let env = OutboxEnv::load().context("Failed to load outbox worker config")?;
    info!(
        exchange    = %env.exchange,
        routing_key = %env.routing_key,
        poll_ms     = env.poll_interval_ms,
        batch_size  = env.batch_size,
        "Outbox worker config loaded"
    );

    let cfg = OutboxConfig {
        database_url: env.database_url,
        amqp_url: env.amqp_url,
        exchange: env.exchange,
        routing_key: env.routing_key,
        poll_interval_ms: env.poll_interval_ms,
        batch_size: env.batch_size,
        pool_size: env.pool_size,
    };

    // ── Graceful shutdown ─────────────────────────────────────────────────────
    let shutdown = CancellationToken::new();

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).context("Failed to register SIGTERM handler")?;
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("SIGINT received — shutting down outbox worker");
                }
                _ = sigterm.recv() => {
                    info!("SIGTERM received — shutting down outbox worker");
                }
            }
            shutdown_clone.cancel();
        });
    }
    #[cfg(not(unix))]
    {
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("SIGINT received — shutting down outbox worker");
            shutdown_clone.cancel();
        });
    }

    // ── Run ───────────────────────────────────────────────────────────────────
    info!("Outbox worker starting");
    run_outbox_worker(cfg, shutdown).await?;
    info!("Outbox worker stopped");

    Ok(())
}
