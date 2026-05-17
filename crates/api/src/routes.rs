use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde_json::json;
use subtle::ConstantTimeEq;
use tower_http::trace::TraceLayer;

use crate::{
    handlers::{
        get_email_status, get_recipient_status, health, invalidate_all_template_cache,
        invalidate_template_cache, ready, retry_event, retry_recipient,
    },
    state::ApiState,
};

pub fn build_router(state: ApiState) -> Router {
    // Probe routes are always open — no auth needed for health checks.
    let probes = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready));

    // All email status and retry routes require a bearer token when
    // `api_key` is configured. When `api_key` is `None` (network-isolated
    // deployments), the middleware passes every request through.
    let protected = Router::new()
        .route("/emails/{event_id}", get(get_email_status))
        .route("/emails/{event_id}/retry", post(retry_event))
        .route(
            "/emails/{event_id}/recipients/{email}",
            get(get_recipient_status),
        )
        .route(
            "/emails/{event_id}/recipients/{email}/retry",
            post(retry_recipient),
        )
        // Template cache invalidation — useful after editing email_template rows
        // without restarting the service.
        .route("/templates/cache", delete(invalidate_all_template_cache))
        .route(
            "/templates/{event_type}/cache",
            delete(invalidate_template_cache),
        )
        .layer(middleware::from_fn_with_state(state.clone(), bearer_auth));

    Router::new()
        .merge(probes)
        .merge(protected)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Axum middleware that enforces `Authorization: Bearer <token>` on every
/// request when `ApiState::api_key` is `Some`.
///
/// Returns 401 when the header is missing and 403 when the token is wrong,
/// so callers can distinguish "you forgot auth" from "your token is invalid".
async fn bearer_auth(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    let Some(expected) = &state.api_key else {
        // Auth disabled — pass through.
        return next.run(request).await;
    };

    let token = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    match token {
        None => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Authorization header missing or malformed" })),
        )
            .into_response(),
        // Constant-time comparison prevents timing attacks on the token.
        // `ConstantTimeEq::ct_eq` returns `subtle::Choice` (1 = equal).
        Some(t) if t.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() == 0 => (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Invalid API key" })),
        )
            .into_response(),
        Some(_) => next.run(request).await,
    }
}
