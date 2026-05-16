//! `ns send` — publish a new email event directly to RabbitMQ.
//!
//! Bypasses the outbox table; useful for one-off sends, testing templates,
//! or re-triggering a delivery from the command line.

use anyhow::{bail, Context, Result};
use chrono::Utc;
use common::{AttachmentRef, BodyOverride, EmailEvent, FromOverride, Recipient};
use dialoguer::Confirm;
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};
use uuid::Uuid;

use crate::{cli::SendArgs, config::CliConfig};

pub async fn run(args: SendArgs, cfg: CliConfig) -> Result<()> {
    // ── 1. Parse payload ──────────────────────────────────────────────────────
    let payload: serde_json::Value = if args.payload.starts_with('@') {
        let path = &args.payload[1..];
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("Cannot read payload file: {path}"))?;
        serde_json::from_str(&text).with_context(|| format!("Invalid JSON in {path}"))?
    } else {
        serde_json::from_str(&args.payload)
            .context("--payload must be valid JSON or @<path> to a JSON file")?
    };

    // ── 2. Build recipients ───────────────────────────────────────────────────
    if args.to.is_empty() {
        bail!("At least one --to address is required");
    }
    let recipients: Vec<Recipient> = args
        .to
        .iter()
        .enumerate()
        .map(|(i, email)| Recipient {
            email: email.clone(),
            name: args.name.get(i).cloned(),
        })
        .collect();

    // ── 3. Validate recipient emails ──────────────────────────────────────────
    for r in &recipients {
        if !common::is_valid_email(&r.email) {
            bail!("Invalid email address: {}", r.email);
        }
    }

    // ── 4. Parse attachments ──────────────────────────────────────────────────
    let attachments: Vec<AttachmentRef> = args
        .attachment
        .iter()
        .map(|raw| {
            serde_json::from_str::<AttachmentRef>(raw)
                .with_context(|| format!("Invalid attachment JSON: {raw}"))
        })
        .collect::<Result<Vec<_>>>()?;

    for att in &attachments {
        att.validate(&Utc::now())
            .map_err(|e| anyhow::anyhow!("Attachment validation failed: {e}"))?;
    }

    // ── 5. Build from_override ────────────────────────────────────────────────
    let from_override = match &args.from_email {
        Some(email) => {
            if !common::is_valid_email(email) {
                bail!("Invalid --from-email address: {email}");
            }
            Some(FromOverride {
                email: email.clone(),
                name: args.from_name.clone(),
            })
        }
        None => None,
    };

    // ── 6. Build event ────────────────────────────────────────────────────────

    // Construct body_override when all three direct-body flags are present.
    // clap's `requires` constraints ensure either all three are Some or all None.
    let body_override = match (&args.subject, &args.body_html, &args.body_text) {
        (Some(subject), Some(body_html), Some(body_text)) => Some(BodyOverride {
            subject: subject.clone(),
            body_html: body_html.clone(),
            body_text: body_text.clone(),
        }),
        _ => None,
    };

    let event_id = args.event_id.unwrap_or_else(Uuid::new_v4);
    let event = EmailEvent {
        event_id,
        timestamp: Utc::now(),
        event_type: args.event_type.clone(),
        recipients: recipients.clone(),
        payload,
        from_override,
        metadata: common::event::Metadata {
            source: Some(args.source.clone()),
        },
        attachments,
        body_override,
        sender_account: None,
    };

    // ── 7. Confirm ────────────────────────────────────────────────────────────
    if !args.yes {
        let to_str: Vec<&str> = recipients.iter().map(|r| r.email.as_str()).collect();
        println!("About to publish:");
        println!("  Event type   : {}", args.event_type);
        println!("  Event ID     : {event_id}");
        println!("  Recipients   : {}", to_str.join(", "));
        println!("  Attachments  : {}", event.attachments.len());
        if event.body_override.is_some() {
            println!("  Body mode    : direct (body_override — template bypassed)");
            println!("  Subject      : {}", args.subject.as_deref().unwrap_or(""));
        } else {
            println!("  Body mode    : template");
        }

        let ok = Confirm::new()
            .with_prompt("Publish this event?")
            .default(true)
            .interact()
            .context("Prompt failed")?;

        if !ok {
            println!("Aborted.");
            return Ok(());
        }
    }

    // ── 8. Publish to RabbitMQ ────────────────────────────────────────────────
    let body = serde_json::to_vec(&event).context("Failed to serialize event")?;

    let conn = Connection::connect(&cfg.amqp.url, ConnectionProperties::default())
        .await
        .context("Failed to connect to RabbitMQ")?;
    let channel = conn
        .create_channel()
        .await
        .context("Failed to create AMQP channel")?;

    // Declare exchange (idempotent — safe if consumer already declared it).
    channel
        .exchange_declare(
            &cfg.amqp.exchange,
            lapin::ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("exchange_declare failed")?;

    channel
        .basic_publish(
            &cfg.amqp.exchange,
            &cfg.amqp.routing_key,
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2), // persistent
        )
        .await
        .context("basic_publish failed")?
        .await
        .context("Publisher confirm failed")?;

    println!("✓ Published event {event_id}");
    println!("  Run `ns status {event_id}` to track delivery.");
    Ok(())
}
