use std::sync::Arc;
use std::time::Duration;

use common::{AppError, EmailEvent, Recipient};
use futures_lite::StreamExt;
use lapin::{options::*, types::FieldTable, Channel, Connection, ConnectionProperties};
use mailer::message::ResolvedAttachment;
use mailer::{fetch_attachments_with_limit, EmailSender, SenderRegistry};
use rate_limiter::MailRateLimiter;
use recipient_filter::RecipientFilter;
use reqwest::Client;
use store::{EmailLogStore, TemplateStore};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    config::ConsumerConfig,
    processor::{is_retryable, process_recipient, ProcessorContext, RecipientOutcome},
};

// ── Public entry point ────────────────────────────────────────────────────────

pub async fn run_consumer(
    cfg: ConsumerConfig,
    store: EmailLogStore,
    template_store: TemplateStore,
    sender: Arc<dyn EmailSender>,
    sender_registry: SenderRegistry,
    filter: RecipientFilter,
    rate_limiter: MailRateLimiter,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let http = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("failed to build HTTP client");
    let http = Arc::new(http);
    let semaphore = Arc::new(Semaphore::new(cfg.max_concurrency));
    let mut reconnect_delay = Duration::from_secs(2);

    // Build the shared context once; all spawned tasks clone it cheaply.
    let ctx = ProcessorContext {
        store,
        template_store,
        sender,
        sender_registry,
        filter,
        rate_limiter,
    };

    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        info!(url = %cfg.amqp_url, "Connecting to RabbitMQ");

        match connect_and_consume(
            &cfg,
            ctx.clone(),
            Arc::clone(&semaphore),
            Arc::clone(&http),
            shutdown.clone(),
        )
        .await
        {
            Ok(()) => {
                info!("Consumer loop exited cleanly");
                return Ok(());
            }
            Err(e) if shutdown.is_cancelled() => {
                info!(error = %e, "Consumer exited after shutdown");
                return Ok(());
            }
            Err(e) => {
                error!(error = %e, delay_secs = reconnect_delay.as_secs(), "Consumer error — reconnecting");
                tokio::select! {
                    _ = sleep(reconnect_delay) => {}
                    _ = shutdown.cancelled() => return Ok(()),
                }
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(60));
            }
        }
    }
}

// ── One connection lifetime ───────────────────────────────────────────────────

async fn connect_and_consume(
    cfg: &ConsumerConfig,
    ctx: ProcessorContext,
    semaphore: Arc<Semaphore>,
    http: Arc<Client>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let conn = Connection::connect(&cfg.amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    declare_topology(&channel, cfg).await?;

    let mut consumer = channel
        .basic_consume(
            &cfg.queue,
            "notification-service",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!(queue = %cfg.queue, "Listening for messages");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Shutdown: draining in-flight tasks");
                let _ = semaphore.acquire_many(cfg.max_concurrency as u32).await;
                return Ok(());
            }
            delivery = consumer.next() => {
                let delivery = match delivery {
                    Some(Ok(d)) => d,
                    Some(Err(e)) => { error!(error = %e, "AMQP error"); return Err(e.into()); }
                    None => { warn!("Consumer stream ended"); return Err(anyhow::anyhow!("stream closed")); }
                };

                let permit = Arc::clone(&semaphore).acquire_owned().await.expect("semaphore closed");
                let ctx    = ctx.clone();
                let cfg    = cfg.clone();
                let http   = Arc::clone(&http);
                let shutdown = shutdown.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    handle_delivery(delivery, ctx, http, cfg, shutdown).await;
                });
            }
        }
    }
}

// ── Per-delivery handler ──────────────────────────────────────────────────────

/// Handle one delivery.
///
/// Attachments are fetched ONCE here at the event level, then the resolved
/// bytes are passed to every recipient.  This prevents N×M HTTP GETs and
/// pre-signed URL expiry for recipients processed later in the loop.
///
/// The AMQP message is ACK'd once ALL recipients are resolved.
/// It is NACK'd (→ DLQ) only if the message itself cannot be deserialized
/// or if the event-level attachment fetch fails permanently.
async fn handle_delivery(
    delivery: lapin::message::Delivery,
    ctx: ProcessorContext,
    http: Arc<Client>,
    cfg: ConsumerConfig,
    shutdown: CancellationToken,
) {
    let event: EmailEvent = match serde_json::from_slice(&delivery.data) {
        Ok(e) => e,
        Err(e) => {
            error!(error = %e, "Cannot deserialize event — sending to DLQ");
            let _ = delivery
                .nack(BasicNackOptions {
                    requeue: false,
                    ..Default::default()
                })
                .await;
            return;
        }
    };

    if event.recipients.is_empty() {
        warn!(event_id = %event.event_id, "Event has no recipients — ACKing and skipping");
        let _ = delivery.ack(BasicAckOptions::default()).await;
        return;
    }

    // ── Fetch attachments once for the whole event ───────────────────────────
    let resolved_attachments: Vec<ResolvedAttachment> = if event.attachments.is_empty() {
        vec![]
    } else {
        match fetch_attachments_with_limit(
            &http,
            &event.attachments,
            &event.timestamp,
            cfg.max_attachment_bytes,
        )
        .await
        {
            Ok(atts) => atts,
            Err(ref e) => {
                let permanent = matches!(e, AppError::Mailer(m) if m.starts_with("permanent:"));
                error!(
                    event_id  = %event.event_id,
                    error     = %e,
                    permanent,
                    "Attachment fetch failed — NACKing message"
                );
                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: !permanent,
                        ..Default::default()
                    })
                    .await;
                return;
            }
        }
    };

    // Process every recipient independently.
    for recipient in &event.recipients {
        process_one_recipient(
            &ctx,
            &event,
            recipient,
            &resolved_attachments,
            &cfg,
            &shutdown,
        )
        .await;
    }

    let _ = delivery.ack(BasicAckOptions::default()).await;
}

/// Drive one recipient through the send loop with per-recipient retry.
async fn process_one_recipient(
    ctx: &ProcessorContext,
    event: &EmailEvent,
    recipient: &Recipient,
    attachments: &[ResolvedAttachment],
    cfg: &ConsumerConfig,
    shutdown: &CancellationToken,
) {
    // Seed attempt counter from DB so restarts don't reset the count.
    let initial = ctx
        .store
        .get_retry_count(event.event_id, &recipient.email)
        .await
        .unwrap_or(0) as u32;

    let mut attempt = initial;
    let mut rl_count: u32 = 0;

    loop {
        match process_recipient(ctx, event, recipient, attachments).await {
            RecipientOutcome::Sent | RecipientOutcome::Blocked(_) | RecipientOutcome::Skipped => {
                return;
            }

            RecipientOutcome::Failed(ref e) if !is_retryable(e) => {
                error!(
                    event_id = %event.event_id,
                    email    = %recipient.email,
                    error    = %e,
                    "Permanent failure for recipient — marking FAILED"
                );
                let _ = ctx
                    .store
                    .mark_failed(event.event_id, &recipient.email, &e.to_string(), true)
                    .await;
                return;
            }

            RecipientOutcome::Failed(ref e) if attempt >= cfg.max_retries => {
                error!(
                    event_id = %event.event_id,
                    email    = %recipient.email,
                    attempt,
                    "Max retries exhausted for recipient"
                );
                let _ = ctx
                    .store
                    .mark_failed(event.event_id, &recipient.email, &e.to_string(), true)
                    .await;
                return;
            }

            // Rate-limited — back off without consuming a retry slot,
            // but cap consecutive rate-limit waits to prevent infinite loops.
            RecipientOutcome::Failed(AppError::RateLimited(ref msg)) => {
                rl_count += 1;
                if rl_count > cfg.max_rl_waits {
                    error!(
                        event_id   = %event.event_id,
                        email      = %recipient.email,
                        rl_count,
                        max_rl_waits = cfg.max_rl_waits,
                        "Rate-limit backoff limit reached — marking FAILED"
                    );
                    let _ = ctx
                        .store
                        .mark_failed(event.event_id, &recipient.email, msg, true)
                        .await;
                    return;
                }
                let delay = Duration::from_secs(30 * (1u64 << attempt.min(3)));
                warn!(
                    event_id   = %event.event_id,
                    email      = %recipient.email,
                    rl_count,
                    delay_secs = delay.as_secs(),
                    "Rate-limited — backing off without consuming retry slot"
                );
                let _ = ctx
                    .store
                    .mark_failed(event.event_id, &recipient.email, msg, false)
                    .await;
                tokio::select! {
                    _ = sleep(delay) => {}
                    _ = shutdown.cancelled() => {
                        // Shutdown arrived during backoff. The row is already
                        // PENDING (mark_failed with exhausted=false above), which
                        // would leave it stuck with no queue message to pick it up.
                        // Flip it to FAILED so the operator can see it and use the
                        // retry API after the service restarts.
                        warn!(
                            event_id = %event.event_id,
                            email    = %recipient.email,
                            "Shutdown during rate-limit backoff — marking FAILED for manual retry"
                        );
                        let _ = ctx
                            .store
                            .mark_failed(event.event_id, &recipient.email,
                                "service shutdown during rate-limit backoff", true)
                            .await;
                        return;
                    }
                }
            }

            // Transient failure — normal exponential backoff
            RecipientOutcome::Failed(ref e) => {
                attempt += 1;
                rl_count = 0;
                let delay = Duration::from_millis(cfg.retry_base_ms * (1 << attempt.min(10)));
                warn!(
                    event_id = %event.event_id,
                    email    = %recipient.email,
                    attempt,
                    delay_ms = delay.as_millis(),
                    error    = %e,
                    "Transient failure — retrying"
                );
                let _ = ctx
                    .store
                    .mark_failed(event.event_id, &recipient.email, &e.to_string(), false)
                    .await;
                tokio::select! {
                    _ = sleep(delay) => {}
                    _ = shutdown.cancelled() => {
                        // Shutdown arrived during retry backoff. The row is already
                        // PENDING; flip it to FAILED so it is visible and recoverable
                        // via the retry API after restart.
                        warn!(
                            event_id = %event.event_id,
                            email    = %recipient.email,
                            attempt,
                            "Shutdown during retry backoff — marking FAILED for manual retry"
                        );
                        let _ = ctx
                            .store
                            .mark_failed(event.event_id, &recipient.email,
                                "service shutdown during retry backoff", true)
                            .await;
                        return;
                    }
                }
            }
        }
    }
}

// ── Topology ──────────────────────────────────────────────────────────────────

async fn declare_topology(channel: &Channel, cfg: &ConsumerConfig) -> anyhow::Result<()> {
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

    let dlx_name = format!("{}.dlx", cfg.exchange);
    channel
        .exchange_declare(
            &dlx_name,
            lapin::ExchangeKind::Fanout,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    let dlq_name = format!("{}.dlq", cfg.queue);
    channel
        .queue_declare(
            &dlq_name,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            &dlq_name,
            &dlx_name,
            "",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut args = FieldTable::default();
    args.insert(
        "x-dead-letter-exchange".into(),
        lapin::types::AMQPValue::LongString(dlx_name.into()),
    );
    channel
        .queue_declare(
            &cfg.queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            args,
        )
        .await?;
    channel
        .queue_bind(
            &cfg.queue,
            &cfg.exchange,
            &cfg.routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!("AMQP topology declared");
    Ok(())
}
