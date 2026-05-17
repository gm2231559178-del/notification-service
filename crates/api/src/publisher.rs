use anyhow::Context;
use common::AppError;
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Thin wrapper around an AMQP channel used by the HTTP API to re-enqueue
/// events whose DB rows have been reset to PENDING via the retry endpoints.
///
/// The inner channel is held behind an `Arc<Mutex<…>>` so that a dropped
/// connection can be transparently re-established on the next publish call
/// without restarting the process. Callers that receive an error may simply
/// retry the HTTP request — the next call will reconnect automatically.
#[derive(Clone)]
pub struct Publisher {
    inner: Arc<Mutex<Option<Channel>>>,
    amqp_url: String,
    exchange: String,
    routing_key: String,
}

impl Publisher {
    pub async fn connect(
        amqp_url: &str,
        exchange: &str,
        routing_key: &str,
    ) -> anyhow::Result<Self> {
        let channel = open_channel(amqp_url, exchange).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(channel))),
            amqp_url: amqp_url.to_owned(),
            exchange: exchange.to_owned(),
            routing_key: routing_key.to_owned(),
        })
    }

    /// Publish a raw JSON body to the configured exchange + routing key.
    ///
    /// Uses publisher confirms — waits until the broker acknowledges the message.
    /// If the channel is broken (connection dropped), one reconnect attempt is
    /// made automatically before returning an error.
    pub async fn publish(&self, body: Vec<u8>) -> Result<(), AppError> {
        let mut guard = self.inner.lock().await;

        // Try once with the current channel; on failure, reconnect and retry once.
        for attempt in 0..2u8 {
            let channel = match guard.as_ref() {
                Some(ch) if ch.status().connected() => ch,
                _ => {
                    // Channel is gone — reconnect.
                    warn!("Publisher: channel not connected — reconnecting");
                    match open_channel(&self.amqp_url, &self.exchange).await {
                        Ok(ch) => {
                            info!("Publisher: reconnected to RabbitMQ");
                            *guard = Some(ch);
                            guard.as_ref().unwrap()
                        }
                        Err(e) => {
                            return Err(AppError::Queue(format!(
                                "Publisher reconnect failed: {e}"
                            )));
                        }
                    }
                }
            };

            let result = channel
                .basic_publish(
                    &self.exchange,
                    &self.routing_key,
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default()
                        .with_content_type("application/json".into())
                        .with_delivery_mode(2), // persistent
                )
                .await;

            match result {
                Ok(confirm) => {
                    return confirm
                        .await
                        .map(|_| ())
                        .map_err(|e| AppError::Queue(e.to_string()));
                }
                Err(e) if attempt == 0 => {
                    // First attempt failed — clear channel and let loop reconnect.
                    warn!(error = %e, "Publisher: publish failed, will reconnect");
                    *guard = None;
                }
                Err(e) => {
                    return Err(AppError::Queue(e.to_string()));
                }
            }
        }

        Err(AppError::Queue(
            "Publisher: publish failed after reconnect".into(),
        ))
    }
}

/// Open a fresh AMQP connection + channel, declare the exchange, and enable
/// publisher confirms so `basic_publish().await?.await` is meaningful.
///
/// Without `confirm_select` the broker never sends Ack/Nack frames and the
/// second `.await` on the confirm future blocks indefinitely.
async fn open_channel(amqp_url: &str, exchange: &str) -> anyhow::Result<Channel> {
    let conn = Connection::connect(amqp_url, ConnectionProperties::default())
        .await
        .context("Publisher: failed to connect to RabbitMQ")?;
    let channel = conn
        .create_channel()
        .await
        .context("Publisher: create channel")?;

    // Put the channel into confirm mode — required for publisher confirms.
    channel
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .context("Publisher: confirm_select")?;

    // Declare the exchange (idempotent — safe if consumer already declared it).
    channel
        .exchange_declare(
            exchange,
            lapin::ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("Publisher: exchange_declare")?;

    Ok(channel)
}
