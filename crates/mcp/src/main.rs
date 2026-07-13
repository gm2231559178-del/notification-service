mod config;
mod server;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::http::{header, HeaderValue, Method};
use axum::middleware;
use axum::response::Response;
use rmcp::transport::{
    streamable_http_server::session::local::LocalSessionManager, StreamableHttpServerConfig,
    StreamableHttpService,
};
use rmcp::{transport::stdio, ServiceExt};
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::{Any, CorsLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use config::load;
use server::NotifyServer;

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
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer().compact())
        .init();

    let cfg_path = std::env::var("MCP_CONFIG").ok();
    let cfg = load(cfg_path.as_deref()).context("Failed to load config")?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&cfg.database.url)
        .await
        .context("Failed to connect to PostgreSQL")?;

    let server = NotifyServer::new(pool, cfg);

    if let Some(port) = std::env::var("MCP_HTTP_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
    {
        let base_url = std::env::var("MCP_BASE_URL")
            .ok()
            .unwrap_or_else(|| format!("http://localhost:{}", port));

        let http_server = server.clone();
        let service = StreamableHttpService::new(
            move || Ok(http_server.clone()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_stateful_mode(true)
                .with_sse_keep_alive(Some(Duration::from_secs(15))),
        );

        let mut router = axum::Router::new().nest_service("/mcp", service);
        router = router.layer(middleware::from_fn(ensure_mcp_accept));

        if let Ok(cors_origin) = std::env::var("MCP_CORS_ORIGIN") {
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
            router = router.layer(cors);
            tracing::info!("CORS enabled with origin: {}", cors_origin);
        }

        let addr = format!("0.0.0.0:{}", port);
        tracing::info!("Starting anvil-notify MCP server over HTTP on {}", addr);
        tracing::info!("MCP endpoint: {}/mcp", base_url);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        axum::serve(listener, router).await?;
    } else {
        tracing::info!("Starting anvil-notify MCP server over stdio...");
        server.serve(stdio()).await?.waiting().await?;
    }

    Ok(())
}
