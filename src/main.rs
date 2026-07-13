mod config;
mod syslog_layer;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use api::{build_router, ApiState, Publisher};
use consumer::{run_consumer, ConsumerConfig, ProcessorContext};
use mailer::smtp::SmtpConfig;
use mailer::webhook::WebhookConfig;
use mailer::{EmailSender, SenderRegistry, SmtpSender, WebhookSender};
use metrics_exporter_prometheus::PrometheusBuilder;

use rate_limiter::MailRateLimiter;
use recipient_filter::RecipientFilter;
use reqwest::Client;
use sqlx::postgres::PgPoolOptions;
use store::{BlockListStore, EmailNotificationStore, NotificationStore, TemplateStore};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[cfg(feature = "mcp")]
use axum::http::{header, HeaderValue, Method};
#[cfg(feature = "mcp")]
use axum::middleware;
#[cfg(feature = "mcp")]
use axum::response::Response;
#[cfg(feature = "mcp")]
use rmcp::transport::{
    streamable_http_server::session::local::LocalSessionManager, StreamableHttpServerConfig,
    StreamableHttpService,
};
#[cfg(feature = "mcp")]
use tower_http::cors::{Any, CorsLayer};

use config::{AppConfig, MailerConfig};

#[cfg(feature = "mcp")]
async fn ensure_mcp_accept(
    mut req: axum::http::Request<axum::body::Body>,
    next: middleware::Next,
) -> Response {
    if req.uri().path().starts_with("/mcp") && !req.headers().contains_key(header::ACCEPT) {
        req.headers_mut().insert(
            header::ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
    }
    next.run(req).await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Tracing ───────────────────────────────────────────────────────────────
    // LOG_FORMAT controls the log sink and format:
    //
    //   json    — structured JSON to stdout (default; best for Docker/k8s)
    //   pretty  — human-readable coloured output (local dev)
    //   compact — human-readable, no colours (CI / plain terminals)
    //   syslog  — emit to syslog (local Unix socket or remote UDP/TCP)
    //             see src/syslog_layer.rs for LOG_SYSLOG_SERVER /
    //             LOG_SYSLOG_FACILITY / LOG_SYSLOG_IDENT knobs.
    //
    // nginx equivalence (remote syslog):
    //   error_log syslog:server=10.0.0.1:514,facility=local7,tag=anvil-notify;
    //   ->  LOG_FORMAT=syslog
    //       LOG_SYSLOG_SERVER=10.0.0.1:514
    //       LOG_SYSLOG_FACILITY=local7
    //       LOG_SYSLOG_IDENT=anvil-notify
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
        "syslog" => {
            let syslog = syslog_layer::SyslogLayer::from_env()
                .context("Failed to initialise syslog transport")?;
            registry.with(syslog).init();
        }
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
        .add_global_label("service", "anvil-notify")
        .set_buckets(&[
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
        ])
        .context("Invalid bucket configuration")?
        .install()
        .context("Failed to install Prometheus metrics exporter")?;
    info!(addr = %metrics_addr, "Prometheus /metrics endpoint listening");

    // Bootstrap all metrics with zero values so they appear immediately
    // in the /metrics output rather than waiting for first use.
    api::bootstrap_metrics();
    consumer::bootstrap_metrics();
    mailer::bootstrap_metrics();
    outbox::bootstrap_metrics();
    metrics::counter!("stale_pending_reaped_total").increment(0);
    info!("All metrics bootstrapped");

    // ── Database ──────────────────────────────────────────────────────────────
    let pool = PgPoolOptions::new()
        .max_connections(cfg.database.pool_size)
        // Fail fast when the pool is saturated rather than blocking indefinitely.
        // A 5-second timeout surfaces as a retryable AppError::Database, which
        // the consumer will back off and retry — much better than a stalled task
        // holding a semaphore permit and an un-ACK'd AMQP message.
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&cfg.database.url)
        .await
        .context("Failed to connect to PostgreSQL")?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("Failed to run migrations")?;

    let store = EmailNotificationStore::new(pool.clone());
    let template_store = TemplateStore::new_with_ttl(
        pool.clone(),
        std::time::Duration::from_secs(cfg.template_cache_ttl_secs),
    );
    let block_list_store = BlockListStore::new(
        pool.clone(),
        std::time::Duration::from_secs(cfg.block_list_cache_ttl_secs),
    );
    info!(
        ttl_secs = cfg.block_list_cache_ttl_secs,
        "BlockListStore ready"
    );
    info!(ttl_secs = cfg.template_cache_ttl_secs, "Database ready");

    // ── Shared HTTP client ────────────────────────────────────────────────────
    // One reqwest::Client is shared across webhook delivery and attachment
    // fetching so both use the same connection pool. This avoids opening a
    // second set of OS-level TCP connections to the same hosts and keeps
    // keep-alive connections reusable across both code paths.
    let http = Arc::new(
        Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("Failed to build shared HTTP client")?,
    );

    // ── Email sender ──────────────────────────────────────────────────────────
    let sender: Arc<dyn EmailSender> = match &cfg.mailer {
        MailerConfig::Smtp {
            host,
            port,
            username,
            password,
            from_email,
            from_name,
            tls_mode,
            connection_timeout_ms,
            read_timeout_ms,
            write_timeout_ms,
            pool_size,
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
                    tls_mode: tls_mode.clone(),
                    connection_timeout: std::time::Duration::from_millis(*connection_timeout_ms),
                    read_timeout: std::time::Duration::from_millis(*read_timeout_ms),
                    write_timeout: std::time::Duration::from_millis(*write_timeout_ms),
                    pool_size: *pool_size,
                })
                .context("Failed to build SMTP sender")?,
            )
        }
        MailerConfig::Webhook { url, auth_token } => {
            info!(url, "Using webhook backend");
            Arc::new(WebhookSender::new(
                WebhookConfig {
                    url: url.clone(),
                    auth_token: auth_token.clone(),
                },
                Arc::clone(&http),
            ))
        }
    };

    // ── Named sender accounts (multi-business-system SMTP) ──────────────────
    // Each entry in sender_accounts gets its own SmtpSender instance so
    // per-account credentials are never mixed. The global `sender` above is
    // used when an event omits `sender_account` or names an unknown account.
    let mut sender_registry = SenderRegistry::new();
    for (name, acct) in &cfg.sender_accounts {
        // Warn loudly when credentials are absent so operators catch the
        // misconfiguration at startup rather than at first send.
        // Empty credentials are intentional for no-auth relays (e.g. Mailpit),
        // so this is a warning, not a hard failure.
        if acct.username.is_empty() || acct.password.is_empty() {
            tracing::warn!(
                account = name,
                from_email = acct.from_email,
                "Named sender account has empty username or password — \
                 this is only correct for no-auth SMTP relays (e.g. Mailpit). \
                 Set username and password in [sender_accounts.{name}] if auth is required."
            );
        }
        let acct_sender = SmtpSender::new(mailer::smtp::SmtpConfig {
            host: acct.host.clone(),
            port: acct.port,
            username: acct.username.clone(),
            password: acct.password.clone(),
            from_email: acct.from_email.clone(),
            from_name: acct.from_name.clone(),
            // Named accounts inherit the global timeout/pool defaults.
            // Override via SmtpAccountConfig if per-account tuning is needed.
            tls_mode: acct.tls_mode.clone(),
            connection_timeout: std::time::Duration::from_millis(acct.connection_timeout_ms),
            read_timeout: std::time::Duration::from_millis(acct.read_timeout_ms),
            write_timeout: std::time::Duration::from_millis(acct.write_timeout_ms),
            pool_size: acct.pool_size,
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
        // Warn operators about a misconfiguration that would silently cap
        // throughput below the intended steady-state rate.  When burst_size <
        // emails_per_second the token bucket can never hold enough tokens to
        // sustain the configured rate; sends will be throttled lower than
        // expected without any other error signal.
        if cfg.rate_limit.burst_size < cfg.rate_limit.emails_per_second {
            tracing::warn!(
                emails_per_second = cfg.rate_limit.emails_per_second,
                burst_size = cfg.rate_limit.burst_size,
                "burst_size is less than emails_per_second; \
                 steady-state throughput will be capped at burst_size, not emails_per_second. \
                 Consider setting burst_size >= emails_per_second."
            );
        }
        // Warn when burst_size is much larger than emails_per_second.  After any
        // quiet period the token bucket refills to burst_size, so the first wave
        // of sends after an idle window will hit the SMTP server at burst_size
        // messages all at once — potentially triggering provider-side throttling
        // or bans.  The threshold is 5× the steady-state rate; beyond that the
        // burst is almost certainly unintentional.
        const BURST_RATIO_WARN: u32 = 5;
        if cfg.rate_limit.emails_per_second > 0
            && cfg.rate_limit.burst_size
                > cfg
                    .rate_limit
                    .emails_per_second
                    .saturating_mul(BURST_RATIO_WARN)
        {
            tracing::warn!(
                emails_per_second = cfg.rate_limit.emails_per_second,
                burst_size = cfg.rate_limit.burst_size,
                threshold = cfg.rate_limit.emails_per_second * BURST_RATIO_WARN,
                "burst_size is more than {BURST_RATIO_WARN}× emails_per_second; after any \
                 quiet period the token bucket will release up to burst_size emails at once, \
                 which may trigger provider-side rate limiting. \
                 Consider setting burst_size closer to emails_per_second."
            );
        }
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
    match &cfg.http.api_key {
        Some(_) => {}
        None if cfg.http.allow_unauthenticated => {
            tracing::warn!(
                "HTTP API authentication is DISABLED (allow_unauthenticated = true) — \
                 all /emails/* and /templates/* endpoints are publicly accessible. \
                 Do not use this setting in production."
            );
        }
        None => {
            anyhow::bail!(
                "HTTP API has no api_key configured and allow_unauthenticated is not set. \
                 Set AN__HTTP__API_KEY to a secret bearer token, or set \
                 AN__HTTP__ALLOW_UNAUTHENTICATED=true to explicitly opt in to running \
                 without authentication (dev/test only)."
            );
        }
    }

    let api_state = ApiState {
        store: Arc::new(store.clone()) as Arc<dyn NotificationStore>,
        template_store: template_store.clone(),
        block_list_store: block_list_store.clone(),
        publisher,
        api_key: cfg.http.api_key.clone(),
        filter: filter.clone(),
        max_recipients_per_event: cfg.amqp.max_recipients_per_event,
    };
    let router = {
        #[cfg_attr(not(feature = "mcp"), allow(unused_mut))]
        let mut r = build_router(api_state);

        #[cfg(feature = "mcp")]
        {
            let mcp_cfg = mcp::config::McpConfig::new(
                cfg.database.url.clone(),
                cfg.amqp.url.clone(),
                cfg.amqp.exchange.clone(),
                cfg.amqp.routing_key.clone(),
            );
            let mcp_server = mcp::server::NotifyServer::new(pool.clone(), mcp_cfg);
            let mcp_service = StreamableHttpService::new(
                move || Ok(mcp_server.clone()),
                Arc::new(LocalSessionManager::default()),
                StreamableHttpServerConfig::default()
                    .with_stateful_mode(true)
                    .with_sse_keep_alive(Some(std::time::Duration::from_secs(15))),
            );

            let mut mcp_router = axum::Router::new()
                .fallback_service(mcp_service)
                .layer(middleware::from_fn(ensure_mcp_accept));

            if let Some(cors_origin) = &cfg.mcp_cors_origin {
                let cors = if cors_origin == "*" {
                    CorsLayer::new()
                        .allow_origin(Any)
                        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                        .allow_headers(Any)
                } else {
                    CorsLayer::new()
                        .allow_origin(
                            cors_origin
                                .parse::<HeaderValue>()
                                .expect("invalid CORS origin"),
                        )
                        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                        .allow_headers(Any)
                };
                mcp_router = mcp_router.layer(cors);
            }

            r = r.nest("/mcp", mcp_router);
            info!("MCP endpoint mounted at /mcp");
        }

        r
    };

    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.http.port));
    info!(addr = %addr, "Starting HTTP API");

    // ── Stale-PENDING reaper ──────────────────────────────────────────────────
    // Recovers rows that are stuck in PENDING with no AMQP message to drive
    // them forward (e.g. after a broker blip during a retry-endpoint call).
    // Any PENDING row older than `stale_pending_timeout_secs` is reset to
    // FAILED so an operator can see it and decide whether to retry manually.
    let reaper_store = Arc::new(store.clone()) as Arc<dyn NotificationStore>;
    let reaper_shutdown = shutdown.clone();
    let reaper_timeout = cfg.stale_pending_timeout_secs;
    let reaper_interval = cfg.stale_pending_reaper_interval_secs;

    // Sanity-check the reaper config at startup so a misconfiguration is
    // visible immediately rather than silently degrading orphan recovery.
    //
    // A timeout of 0 disables the reaper entirely (reap_stale_pending(0)
    // would match every PENDING row immediately, which is also wrong).
    // A timeout above 24 h means orphaned rows stay PENDING for over a day
    // before an operator is alerted — usually not intentional.
    const REAPER_WARN_THRESHOLD_SECS: u64 = 24 * 3600; // 24 hours
    if reaper_timeout == 0 {
        tracing::warn!(
            stale_pending_timeout_secs = reaper_timeout,
            "stale_pending_timeout_secs is 0 — the stale-PENDING reaper is effectively \
             disabled. Orphaned PENDING rows (stranded after a broker blip) will never \
             be automatically recovered. Set a positive value (default: 600) to enable \
             automatic recovery."
        );
    } else if reaper_timeout > REAPER_WARN_THRESHOLD_SECS {
        tracing::warn!(
            stale_pending_timeout_secs = reaper_timeout,
            threshold_secs = REAPER_WARN_THRESHOLD_SECS,
            "stale_pending_timeout_secs exceeds 24 hours — orphaned PENDING rows will \
             remain invisible to operators for an unusually long window. Consider \
             reducing this value (default: 600) unless you have very long retry \
             back-off windows that require a larger threshold."
        );
    } else {
        info!(
            stale_pending_timeout_secs = reaper_timeout,
            reaper_interval_secs = reaper_interval,
            "Stale-PENDING reaper configured"
        );
    }

    let reaper_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(reaper_interval));
        loop {
            tokio::select! {
                _ = reaper_shutdown.cancelled() => break,
                _ = interval.tick() => {
                    match reaper_store.reap_stale_pending(reaper_timeout).await {
                        Ok(ids) if !ids.is_empty() => {
                            metrics::counter!("stale_pending_reaped_total").increment(ids.len() as u64);
                            let event_ids: Vec<String> =
                                ids.iter().map(|id| id.to_string()).collect();
                            tracing::warn!(
                                count        = ids.len(),
                                timeout_secs = reaper_timeout,
                                event_ids    = %event_ids.join(", "),
                                "Stale-PENDING reaper: marked {} orphaned rows as FAILED — \
                                 rows were PENDING with no AMQP message to drive them (e.g. \
                                 broker blip during a retry-API call). \
                                 Wait at least {}s before retrying to ensure any in-flight \
                                 consumer task for the same event_id has completed, then use \
                                 `anctl retry --event-id <id>` to recover. \
                                 The consumer idempotency guard prevents double-sends.",
                                ids.len(),
                                reaper_timeout,
                            )
                        }
                        Ok(_) => {}
                        Err(e) => tracing::error!(error = %e, "Stale-PENDING reaper error"),
                    }
                }
            }
        }
    });

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
        // Memory note: attachment bytes are held in RAM for each in-flight
        // message from fetch through SMTP delivery.  Worst-case peak:
        //   max_concurrency × (attachments per event) × max_attachment_bytes
        // Ensure your container memory limit accounts for this.
        // See config/default.toml [amqp] max_concurrency and max_attachment_bytes.
        max_concurrency: cfg.amqp.max_concurrency,
        max_attachment_bytes: cfg.max_attachment_bytes,
        max_rl_waits: cfg.amqp.max_rl_waits,
        max_recipients_per_event: cfg.amqp.max_recipients_per_event,
    };
    consumer_cfg.validate().context("Invalid consumer config")?;

    // Warn when the product of max_concurrency and max_attachment_bytes exceeds
    // 512 MiB.  At that point a single burst of large-attachment events can OOM
    // the process before the OS's OOM killer intervenes.  The threshold is
    // generous to avoid false-positive warnings on typical deployments.
    const ATTACHMENT_MEMORY_WARN_BYTES: usize = 512 * 1024 * 1024; // 512 MiB
    let peak_attachment_bytes = cfg
        .amqp
        .max_concurrency
        .saturating_mul(cfg.max_attachment_bytes);
    if peak_attachment_bytes > ATTACHMENT_MEMORY_WARN_BYTES {
        tracing::warn!(
            max_concurrency      = cfg.amqp.max_concurrency,
            max_attachment_bytes = cfg.max_attachment_bytes,
            peak_attachment_mib  = peak_attachment_bytes / (1024 * 1024),
            threshold_mib        = ATTACHMENT_MEMORY_WARN_BYTES / (1024 * 1024),
            "Peak attachment memory (max_concurrency x max_attachment_bytes) exceeds              {} MiB. A burst of large-attachment events may exhaust container memory.              Consider reducing max_concurrency or max_attachment_bytes in your config.",
            ATTACHMENT_MEMORY_WARN_BYTES / (1024 * 1024),
        );
    } else {
        info!(
            max_concurrency = cfg.amqp.max_concurrency,
            max_attachment_bytes = cfg.max_attachment_bytes,
            peak_attachment_mib = peak_attachment_bytes / (1024 * 1024),
            "Attachment memory budget checked"
        );
    }

    let consumer_shutdown = shutdown.clone();
    let consumer_http = Arc::clone(&http);
    let consumer_task = tokio::spawn(async move {
        let ctx = ProcessorContext {
            store: Arc::new(store),
            template_store: Arc::new(template_store),
            sender,
            sender_registry,
            filter,
            block_list_store: Arc::new(block_list_store),
            rate_limiter,
        };
        if let Err(e) = run_consumer(consumer_cfg, ctx, consumer_http, consumer_shutdown).await {
            tracing::error!(error = %e, "Consumer exited with error");
        }
    });

    info!("AnvilNotify running");

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

    let timeout = tokio::time::Duration::from_secs(cfg.shutdown_timeout_secs);
    if tokio::time::timeout(timeout, async {
        let _ = api_task.await;
        let _ = consumer_task.await;
        let _ = reaper_task.await;
    })
    .await
    .is_err()
    {
        tracing::warn!(
            shutdown_timeout_secs = cfg.shutdown_timeout_secs,
            "Graceful shutdown timed out — forcing exit"
        );
    } else {
        info!("Graceful shutdown complete");
    }

    Ok(())
}
