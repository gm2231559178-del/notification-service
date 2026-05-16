use std::time::Duration;

use anyhow::Context;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
};
use metrics;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

use crate::OutboxConfig;

// ── Schema ────────────────────────────────────────────────────────────────────
//
// The business service must have a table with AT LEAST these columns:
//
//   CREATE TABLE outbox (
//       id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
//       event_id     UUID        NOT NULL UNIQUE,
//       event_type   TEXT        NOT NULL,
//       payload      JSONB       NOT NULL,
//       status       TEXT        NOT NULL DEFAULT 'PENDING'
//                                CHECK (status IN ('PENDING', 'IN_PROGRESS', 'PUBLISHED', 'FAILED')),
//       fail_count   INT         NOT NULL DEFAULT 0,
//       created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
//       published_at TIMESTAMPTZ
//   );
//
// See `migrations/0002_create_outbox.sql` for the full definition.

/// Maximum consecutive publish failures before a row is permanently marked FAILED.
const MAX_PUBLISH_FAILURES: i32 = 5;

// ── Public entry point ────────────────────────────────────────────────────────

/// Poll the business outbox table and publish pending events to RabbitMQ.
///
/// Reconnects to both Postgres and RabbitMQ on failure.
/// Exits cleanly when `shutdown` is cancelled.
pub async fn run_outbox_worker(
    cfg: OutboxConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(cfg.pool_size)
        .connect(&cfg.database_url)
        .await
        .context("Outbox worker: failed to connect to business DB")?;

    info!("Outbox worker connected to business DB");

    let mut reconnect_delay = Duration::from_secs(2);

    loop {
        if shutdown.is_cancelled() {
            info!("Outbox worker: shutdown requested");
            return Ok(());
        }

        match connect_amqp_and_poll(&cfg, &pool, shutdown.clone()).await {
            Ok(()) => {
                info!("Outbox worker: exiting cleanly");
                return Ok(());
            }
            Err(e) if shutdown.is_cancelled() => {
                info!(error = %e, "Outbox worker: exited after shutdown");
                return Ok(());
            }
            Err(e) => {
                error!(
                    error = %e,
                    delay_secs = reconnect_delay.as_secs(),
                    "Outbox worker error — reconnecting"
                );
                tokio::select! {
                    _ = sleep(reconnect_delay) => {}
                    _ = shutdown.cancelled()   => return Ok(()),
                }
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(60));
            }
        }
    }
}

// ── One AMQP connection lifetime ──────────────────────────────────────────────

async fn connect_amqp_and_poll(
    cfg: &OutboxConfig,
    pool: &PgPool,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let conn = Connection::connect(&cfg.amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    channel
        .exchange_declare(
            &cfg.exchange,
            lapin::ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    info!(exchange = %cfg.exchange, "Outbox worker AMQP ready");
    // Reset backoff on successful connect.

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Outbox worker: shutdown — stopping poll loop");
                return Ok(());
            }
            result = poll_once(cfg, pool, &channel) => {
                if let Err(e) = result {
                    // AMQP channel is dead — surface the error so the outer
                    // loop reconnects rather than continuing with a broken channel.
                    error!(error = %e, "Outbox worker: Failed poll_once");
                    return Err(e);
                }
            }
        }

        // Wait before next poll cycle.
        tokio::select! {
            _ = sleep(Duration::from_millis(cfg.poll_interval_ms)) => {}
            _ = shutdown.cancelled() => return Ok(()),
        }
    }
}

// ── Single poll cycle ─────────────────────────────────────────────────────────

/// Poll one batch of PENDING outbox rows and publish them to RabbitMQ.
///
/// Returns `Err` if the AMQP channel is broken so `connect_amqp_and_poll`
/// can reconnect immediately rather than continuing to hammer a dead channel
/// for every remaining row in the batch.  Database errors are logged and
/// treated as transient (the next poll cycle will retry them).
async fn poll_once(cfg: &OutboxConfig, pool: &PgPool, channel: &Channel) -> anyhow::Result<()> {
    match fetch_pending_batch(pool, cfg.batch_size).await {
        Ok(rows) if rows.is_empty() => {
            // Nothing to do this cycle.
        }
        Ok(rows) => {
            let count = rows.len();
            info!(count, "Outbox: processing batch");
            for row in rows {
                match publish_and_mark(pool, channel, cfg, &row).await {
                    Ok(()) => {
                        metrics::counter!("outbox_published_total").increment(1);
                    }
                    Err(e) => {
                        error!(event_id = %row.event_id, error = %e, "Failed to publish outbox row");
                        metrics::counter!("outbox_publish_failed_total").increment(1);
                        if let Err(e2) =
                            record_publish_failure(pool, row.id, MAX_PUBLISH_FAILURES).await
                        {
                            error!(event_id = %row.event_id, error = %e2, "Could not record publish failure");
                        }
                        // If the channel is no longer connected, propagate the
                        // error so connect_amqp_and_poll reconnects.  Otherwise
                        // keep going — the failure was row-specific (e.g. a
                        // serialization error) and the channel is still healthy.
                        if !channel.status().connected() {
                            return Err(e);
                        }
                    }
                }
            }
        }
        Err(e) => {
            error!(error = %e, "Outbox: failed to fetch batch");
            // DB errors are transient — don't abort the AMQP connection.
        }
    }
    Ok(())
}

// ── DB helpers ────────────────────────────────────────────────────────────────

struct OutboxRow {
    id: Uuid,
    event_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
}

async fn fetch_pending_batch(pool: &PgPool, limit: i64) -> anyhow::Result<Vec<OutboxRow>> {
    // SELECT … FOR UPDATE SKIP LOCKED must run inside an explicit transaction.
    // Without a transaction the row-level locks are released immediately after
    // the SELECT, defeating the purpose of SKIP LOCKED and allowing two workers
    // racing on the same batch to pick up the same rows.
    //
    // We also immediately flip matched rows to IN_PROGRESS inside the same txn
    // so that even after the transaction commits no other worker can re-select them.
    let mut tx = pool.begin().await?;
    let rows = sqlx::query!(
        r#"
        SELECT id, event_id, event_type, payload
        FROM   outbox
        WHERE  status = 'PENDING'
        ORDER  BY created_at ASC
        LIMIT  $1
        FOR    UPDATE SKIP LOCKED
        "#,
        limit,
    )
    .fetch_all(&mut *tx)
    .await?;

    if !rows.is_empty() {
        let ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.id).collect();
        sqlx::query!(
            "UPDATE outbox SET status = 'IN_PROGRESS' WHERE id = ANY($1) AND status = 'PENDING'",
            &ids,
        )
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(rows
        .into_iter()
        .map(|r| OutboxRow {
            id: r.id,
            event_id: r.event_id,
            event_type: r.event_type,
            payload: r.payload,
        })
        .collect())
}

async fn publish_and_mark(
    pool: &PgPool,
    channel: &Channel,
    cfg: &OutboxConfig,
    row: &OutboxRow,
) -> anyhow::Result<()> {
    // Build the canonical EmailEvent JSON.
    //
    // Backwards-compatible promotion:
    //   Legacy payload: { "recipient": {...}, "payload": {...} }
    //   New payload:    { "recipients": [...], "payload": {...} }
    //
    // If the outbox row was written with a singular "recipient" key (old
    // business service), we wrap it in a one-element array so the consumer's
    // deserializer always receives the array form.  If "recipients" is already
    // present (new business service), we forward it verbatim.
    let recipients = if row.payload.get("recipients").is_some() {
        row.payload["recipients"].clone()
    } else if let Some(r) = row.payload.get("recipient") {
        serde_json::Value::Array(vec![r.clone()])
    } else {
        serde_json::Value::Array(vec![])
    };

    let event = serde_json::json!({
        "event_id":      row.event_id,
        "timestamp":     chrono::Utc::now().to_rfc3339(),
        "type":          row.event_type,
        "recipients":    recipients,
        "payload":       row.payload.get("payload").cloned().unwrap_or(serde_json::Value::Object(Default::default())),
        // Forward optional per-event sender override from the outbox payload.
        // Business service writes: { "from_override": { "email": "...", "name": "..." } }
        "from_override": row.payload.get("from_override").cloned().unwrap_or(serde_json::Value::Null),
        "metadata":      row.payload.get("metadata").cloned().unwrap_or(serde_json::Value::Null),
        // Forward attachment URL references so the consumer can fetch and attach
        // files at send time.  Business service writes:
        //   { "attachments": [{ "url": "...", "filename": "...", "content_type": "..." }] }
        // An absent key is promoted to an empty array — the consumer treats both
        // identically (no attachments).
        "attachments":   row.payload.get("attachments").cloned().unwrap_or(serde_json::Value::Array(vec![])),
    });

    let body = serde_json::to_vec(&event)?;

    channel
        .basic_publish(
            &cfg.exchange,
            &cfg.routing_key,
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2), // persistent
        )
        .await?
        .await?; // wait for broker confirm

    // Mark as PUBLISHED — uses IN_PROGRESS guard so only this worker can flip it.
    sqlx::query!(
        r#"
        UPDATE outbox
        SET    status = 'PUBLISHED', published_at = now()
        WHERE  id = $1 AND status = 'IN_PROGRESS'
        "#,
        row.id,
    )
    .execute(pool)
    .await?;

    info!(event_id = %row.event_id, event_type = %row.event_type, "Published outbox event");
    Ok(())
}

// ── Publish failure tracking ──────────────────────────────────────────────────

/// Record a publish failure for an outbox row and reset it back to PENDING so
/// it is retried on the next poll cycle.
///
/// When `fail_count` reaches `max_failures` the row is permanently marked
/// FAILED instead, preventing a broken event from blocking the outbox forever.
///
/// Transitions from IN_PROGRESS → PENDING (or FAILED), which is necessary
/// because `fetch_pending_batch` now sets rows to IN_PROGRESS atomically;
/// leaving them IN_PROGRESS after a failure would strand them forever.
async fn record_publish_failure(pool: &PgPool, id: Uuid, max_failures: i32) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        UPDATE outbox
        SET    fail_count = fail_count + 1,
               status     = CASE WHEN fail_count + 1 >= $2 THEN 'FAILED' ELSE 'PENDING' END
        WHERE  id = $1
        "#,
        id,
        max_failures,
    )
    .execute(pool)
    .await?;
    Ok(())
}
