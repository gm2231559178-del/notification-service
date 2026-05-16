/// Configuration for the outbox poller.
#[derive(Debug, Clone)]
pub struct OutboxConfig {
    /// Connection URL for the **business** database (may differ from the
    /// notification-service's own DB).
    pub database_url: String,

    /// AMQP broker URL.
    pub amqp_url: String,

    /// Exchange to publish events to.
    pub exchange: String,

    /// Routing key used when publishing.
    pub routing_key: String,

    /// How many milliseconds to wait between polling cycles when the
    /// outbox is empty.
    pub poll_interval_ms: u64,

    /// Maximum rows to process per polling cycle.
    pub batch_size: i64,

    /// Maximum number of connections in the outbox DB pool (default: 5).
    pub pool_size: u32,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://postgres:postgres@localhost:5432/business".into(),
            amqp_url: "amqp://guest:guest@localhost:5672".into(),
            exchange: "notifications".into(),
            routing_key: "email.requested".into(),
            poll_interval_ms: 1_000,
            batch_size: 50,
            pool_size: 5,
        }
    }
}
