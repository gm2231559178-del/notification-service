mod config;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use api::{build_router, ApiState, Publisher};
use consumer::{run_consumer, ConsumerConfig};
use mailer::smtp::SmtpConfig;
use mailer::webhook::WebhookConfig;
use mailer::{EmailSender, SenderRegistry, SmtpSender, WebhookSender};
use metrics_exporter_prometheus::PrometheusBuilder;

use rate_limiter::MailRateLimiter;
use recipient_filter::RecipientFilter;
use sqlx::postgres::PgPoolOptions;
use store::{EmailLogStore, TemplateStore};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use config::{AppConfig, MailerConfig};

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
    let cfg = AppConfig::load().context("Failed to load config")?;
    info!("Config loaded");

    // ── Prometheus metrics ────────────────────────────────────────────────────
    // Installs a global metrics recorder. The exporter serves /metrics on a
    // separate port (default 9091) so it's never exposed to the public API.
    let metrics_addr: SocketAddr = format!("0.0.0.0:{}", cfg.metrics_port.unwrap_or(9091))
        .parse()
        .context("Invalid metrics port")?;
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()
        .context("Failed to install Prometheus metrics exporter")?;
    info!(addr = %metrics_addr, "Prometheus /metrics endpoint listening");

    // ── Database ──────────────────────────────────────────────────────────────
    let pool = PgPoolOptions::new()
        .max_connections(cfg.database.pool_size)
        .connect(&cfg.database.url)
        .await
        .context("Failed to connect to PostgreSQL")?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("Failed to run migrations")?;

    let store = EmailLogStore::new(pool.clone());
    let template_store = TemplateStore::new_with_ttl(
        pool,
        std::time::Duration::from_secs(cfg.template_cache_ttl_secs),
    );
    info!(ttl_secs = cfg.template_cache_ttl_secs, "Database ready");

    // ── Email sender ──────────────────────────────────────────────────────────
    let sender: Arc<dyn EmailSender> = match &cfg.mailer {
        MailerConfig::Smtp {
            host,
            port,
            username,
            password,
            from_email,
            from_name,
        } => {
            info!(host, "Using SMTP backend");
            Arc::new(
                SmtpSender::new(SmtpConfig {
                    host: host.clone(),
                    port: *port,
                    username: username.clone(),
                    password: password.clone(),
                    from_email: from_email.clone(),
                    from_name: from_name.clone(),
                })
                .context("Failed to build SMTP sender")?,
            )
        }
        MailerConfig::Webhook { url, auth_token } => {
            info!(url, "Using webhook backend");
            Arc::new(WebhookSender::new(WebhookConfig {
                url: url.clone(),
                auth_token: auth_token.clone(),
            }))
        }
    };

    // ── Named sender accounts (multi-business-system SMTP) ──────────────────
    // Each entry in sender_accounts gets its own SmtpSender instance so
    // per-account credentials are never mixed. The global `sender` above is
    // used when an event omits `sender_account` or names an unknown account.
    let mut sender_registry = SenderRegistry::new();
    for (name, acct) in &cfg.sender_accounts {
        let acct_sender = SmtpSender::new(mailer::smtp::SmtpConfig {
            host: acct.host.clone(),
            port: acct.port,
            username: acct.username.clone(),
            password: acct.password.clone(),
            from_email: acct.from_email.clone(),
            from_name: acct.from_name.clone(),
        })
        .with_context(|| format!("Failed to build SMTP sender for account '{name}'"))?;
        sender_registry.register(name.clone(), Arc::new(acct_sender));
        info!(
            account = name,
            from_email = acct.from_email,
            "Registered named sender account"
        );
    }

    // ── Rate limiter ──────────────────────────────────────────────────────────
    let rate_limiter = MailRateLimiter::new(cfg.rate_limit.clone());
    if rate_limiter.is_disabled() {
        info!("Mail rate limiting disabled (emails_per_second = 0)");
    } else {
        info!(
            emails_per_second = cfg.rate_limit.emails_per_second,
            burst_size = cfg.rate_limit.burst_size,
            "Mail rate limiter active"
        );
    }

    // ── Recipient filter ──────────────────────────────────────────────────────
    let filter = RecipientFilter::new(cfg.filter.clone());
    if filter.is_passthrough() {
        info!("Recipient filter: passthrough (no block/allow-list configured)");
    } else {
        info!("Recipient filter active");
    }

    // ── Graceful shutdown token ───────────────────────────────────────────────
    let shutdown = CancellationToken::new();

    // ── HTTP API publisher (re-enqueues events from retry endpoints) ─────────
    let publisher = Publisher::connect(&cfg.amqp.url, &cfg.amqp.exchange, &cfg.amqp.routing_key)
        .await
        .context("Failed to create API publisher")?;
    info!("API publisher connected to RabbitMQ");

    // ── HTTP API ──────────────────────────────────────────────────────────────
    let api_state = ApiState {
        store: store.clone(),
        template_store: template_store.clone(),
        publisher,
        api_key: cfg.http.api_key.clone(),
    };
    let router = build_router(api_state);
    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.http.port));
    info!(addr = %addr, "Starting HTTP API");

    let api_shutdown = shutdown.clone();
    let api_task = tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, addr = %addr, "Failed to bind HTTP listener");
                return;
            }
        };
        if let Err(e) = axum::serve(listener, router)
            .with_graceful_shutdown(async move { api_shutdown.cancelled().await })
            .await
        {
            tracing::error!(error = %e, "HTTP server exited with error");
        }
    });

    // ── AMQP consumer ─────────────────────────────────────────────────────────
    let consumer_cfg = ConsumerConfig {
        amqp_url: cfg.amqp.url.clone(),
        queue: cfg.amqp.queue.clone(),
        exchange: cfg.amqp.exchange.clone(),
        routing_key: cfg.amqp.routing_key.clone(),
        max_retries: cfg.amqp.max_retries,
        retry_base_ms: cfg.amqp.retry_base_ms,
        max_concurrency: cfg.amqp.max_concurrency,
        max_attachment_bytes: cfg.max_attachment_bytes,
        max_rl_waits: cfg.amqp.max_rl_waits,
    };

    let consumer_shutdown = shutdown.clone();
    let consumer_task = tokio::spawn(async move {
        if let Err(e) = run_consumer(
            consumer_cfg,
            store,
            template_store,
            sender,
            sender_registry,
            filter,
            rate_limiter,
            consumer_shutdown,
        )
        .await
        {
            tracing::error!(error = %e, "Consumer exited with error");
        }
    });

    info!("Notification service running");

    // ── Graceful shutdown ─────────────────────────────────────────────────────
    // Listen for SIGINT (Ctrl-C) and SIGTERM (Kubernetes / container runtimes).
    // SIGTERM is the primary signal in containerised environments; SIGINT is
    // the developer fallback. We want to react to whichever arrives first.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).context("Failed to register SIGTERM handler")?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => { info!("SIGINT received — initiating graceful shutdown"); }
            _ = sigterm.recv()          => { info!("SIGTERM received — initiating graceful shutdown"); }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        info!("SIGINT received — initiating graceful shutdown");
    }
    shutdown.cancel();

    let timeout = tokio::time::Duration::from_secs(30);
    if tokio::time::timeout(timeout, async {
        let _ = api_task.await;
        let _ = consumer_task.await;
    })
    .await
    .is_err()
    {
        tracing::warn!("Graceful shutdown timed out after 30 s — forcing exit");
    } else {
        info!("Graceful shutdown complete");
    }

    Ok(())
}
