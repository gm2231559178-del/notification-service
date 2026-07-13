use std::fmt;
use std::time::Duration;

use crate::config::McpConfig;
use chrono::Utc;
use common::{
    ChannelOverrides, EmailOptions, FromOverride, GroupRetryMode, Metadata, NotificationEvent,
    Recipient, RetryPolicy, SendMode,
};
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};
use rmcp::{
    handler::server::router::tool::ToolRouter, handler::server::tool::ToolCallContext,
    handler::server::wrapper::Parameters, model::*, schemars, service::RequestContext, tool,
    tool_router, ErrorData as McpError, RoleServer, ServerHandler,
};
use sqlx::PgPool;
use store::{cli_queries, BlockListStore, TemplateStore};
use uuid::Uuid;

#[derive(Clone)]
pub struct NotifyServer {
    pool: PgPool,
    cfg: McpConfig,
    tool_router: ToolRouter<Self>,
    template_store: TemplateStore,
    block_list_store: BlockListStore,
}

impl fmt::Debug for NotifyServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotifyServer")
            .field("pool", &self.pool)
            .field("cfg", &self.cfg)
            .field("tool_router", &self.tool_router)
            .field("template_store", &"...")
            .field("block_list_store", &"...")
            .finish()
    }
}

#[tool_router]
impl NotifyServer {
    pub fn new(pool: PgPool, cfg: McpConfig) -> Self {
        Self {
            template_store: TemplateStore::new(pool.clone()),
            block_list_store: BlockListStore::new(pool.clone(), Duration::from_secs(300)),
            pool,
            tool_router: Self::tool_router(),
            cfg,
        }
    }

    #[tool(description = "Send a test email event via RabbitMQ")]
    async fn send_email(
        &self,
        Parameters(args): Parameters<SendEmailArgs>,
    ) -> Result<CallToolResult, McpError> {
        if !common::is_valid_email(&args.to) {
            return Err(McpError::invalid_params(
                format!("invalid email address: {}", args.to),
                None::<serde_json::Value>,
            ));
        }

        let recipient = Recipient {
            email: args.to.clone(),
            name: args.name.clone(),
        };

        let payload: serde_json::Value =
            if args.subject.is_some() && args.body_html.is_some() && args.body_text.is_some() {
                serde_json::json!({
                    "subject": args.subject.as_deref().unwrap_or_default(),
                    "body_html": args.body_html.as_deref().unwrap_or_default(),
                    "body_text": args.body_text.as_deref().unwrap_or_default(),
                })
            } else {
                serde_json::from_str(&args.payload).unwrap_or(serde_json::Value::Null)
            };

        let event_id: Uuid = args
            .event_id
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(uuid::Uuid::new_v4);
        let event_type =
            if args.subject.is_some() && args.body_html.is_some() && args.body_text.is_some() {
                "GENERIC_HTML".to_string()
            } else {
                args.event_type.clone()
            };

        let event = NotificationEvent {
            event_id,
            timestamp: Utc::now(),
            event_type,
            payload,
            metadata: Metadata {
                source: Some("opencode-mcp".to_string()),
            },
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    recipients: vec![recipient],
                    cc: vec![],
                    bcc: vec![],
                    from_override: args.from_email.map(|email| FromOverride {
                        email,
                        name: args.from_name.clone(),
                    }),
                    attachments: vec![],
                    sender_account: None,
                    send_mode: SendMode::Individual,
                    group_retry_mode: GroupRetryMode::Whole,
                    retry_policy: RetryPolicy::Retry,
                    send_at: None,
                    priority: None,
                }),
            },
        };

        let body = serde_json::to_vec(&event)
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        let conn = Connection::connect(&self.cfg.amqp.url, ConnectionProperties::default())
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        channel
            .confirm_select(lapin::options::ConfirmSelectOptions::default())
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        channel
            .exchange_declare(
                &self.cfg.amqp.exchange,
                lapin::ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        channel
            .basic_publish(
                &self.cfg.amqp.exchange,
                &self.cfg.amqp.routing_key,
                BasicPublishOptions::default(),
                &body,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2),
            )
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        let result = serde_json::json!({
            "status": "published",
            "event_id": event.event_id.to_string(),
            "event_type": event.event_type,
            "recipient": args.to,
            "message": format!("Email event {} published to {}", event.event_id, self.cfg.amqp.routing_key),
        });

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(description = "Check delivery status for an email event by event ID")]
    async fn check_delivery_status(
        &self,
        Parameters(args): Parameters<StatusArgs>,
    ) -> Result<CallToolResult, McpError> {
        let event_id: Uuid = args.event_id.parse().map_err(|e| {
            McpError::invalid_params(
                format!("invalid event_id UUID: {}", e),
                None::<serde_json::Value>,
            )
        })?;
        let rows = cli_queries::get_status_for_event(&self.pool, event_id)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        if rows.is_empty() {
            return Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "No delivery records found for event {}",
                args.event_id
            ))]));
        }

        let result: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|r| {
                serde_json::json!({
                    "recipient": r.recipient_email,
                    "status": r.status,
                    "retry_count": r.retry_count,
                    "last_error": r.last_error,
                    "updated_at": r.updated_at.to_rfc3339(),
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&serde_json::json!({
                "event_id": args.event_id,
                "recipients": result,
            }))
            .unwrap_or_default(),
        )]))
    }

    #[tool(description = "List all available email notification templates")]
    async fn list_templates(&self) -> Result<CallToolResult, McpError> {
        let templates = cli_queries::list_templates(&self.pool)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        let result: Vec<serde_json::Value> = templates
            .into_iter()
            .map(|t| {
                serde_json::json!({
                    "event_type": t.event_type,
                    "channel": t.channel,
                    "subject": t.subject,
                    "version": t.version,
                    "active": t.active,
                    "updated_at": t.updated_at.to_rfc3339(),
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(
        description = "Show full template content for an event type including body HTML and body text"
    )]
    async fn get_template(
        &self,
        Parameters(args): Parameters<GetTemplateArgs>,
    ) -> Result<CallToolResult, McpError> {
        let templates = self
            .template_store
            .get(&args.event_type)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        if templates.is_empty() {
            return Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "No template found for event type '{}'",
                args.event_type
            ))]));
        }

        let result: Vec<serde_json::Value> = templates
            .into_iter()
            .map(|t| {
                serde_json::json!({
                    "event_type": t.event_type,
                    "channel": t.channel,
                    "subject": t.subject,
                    "body_html": t.body_html,
                    "body_text": t.body_text,
                    "version": t.version,
                    "active": t.active,
                    "updated_at": t.updated_at.to_rfc3339(),
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(description = "Create or update an email notification template")]
    async fn upsert_template(
        &self,
        Parameters(args): Parameters<UpsertTemplateArgs>,
    ) -> Result<CallToolResult, McpError> {
        if args.event_type.trim().is_empty() || args.subject.trim().is_empty() {
            return Err(McpError::invalid_params(
                "event_type and subject must not be empty".to_string(),
                None::<serde_json::Value>,
            ));
        }

        let (version, inserted) = self
            .template_store
            .upsert(
                &args.event_type,
                &args.channel,
                &args.subject,
                &args.body_html,
                &args.body_text,
                args.active,
            )
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        let result = serde_json::json!({
            "event_type": args.event_type,
            "channel": args.channel,
            "version": version,
            "active": args.active,
            "inserted": inserted,
        });

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(description = "Check delivery status for a specific recipient within an event")]
    async fn get_recipient_status(
        &self,
        Parameters(args): Parameters<RecipientStatusArgs>,
    ) -> Result<CallToolResult, McpError> {
        let event_id: Uuid = args.event_id.parse().map_err(|e| {
            McpError::invalid_params(
                format!("invalid event_id UUID: {}", e),
                None::<serde_json::Value>,
            )
        })?;

        let row = cli_queries::get_status_for_recipient(&self.pool, event_id, &args.email)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        match row {
            None => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "No delivery record found for event {} / recipient {}",
                args.event_id, args.email
            ))])),
            Some(r) => {
                let result = serde_json::json!({
                    "event_id": args.event_id,
                    "recipient": r.recipient_email,
                    "status": r.status,
                    "retry_count": r.retry_count,
                    "last_error": r.last_error,
                    "updated_at": r.updated_at.to_rfc3339(),
                });
                Ok(CallToolResult::success(vec![ContentBlock::text(
                    serde_json::to_string_pretty(&result).unwrap_or_default(),
                )]))
            }
        }
    }

    #[tool(description = "List all active block/allow-list entries")]
    async fn list_blocklist(&self) -> Result<CallToolResult, McpError> {
        let entries = self
            .block_list_store
            .list_entries()
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        let result: Vec<serde_json::Value> = entries
            .into_iter()
            .map(|e| {
                serde_json::json!({
                    "id": e.id,
                    "kind": e.kind,
                    "value": e.value,
                    "reason": e.reason,
                    "created_at": e.created_at.to_rfc3339(),
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(
        description = "Add a block/allow-list entry (kind: blocked_email, blocked_domain, allowed_email, allowed_domain)"
    )]
    async fn add_blocklist_entry(
        &self,
        Parameters(args): Parameters<AddBlocklistEntryArgs>,
    ) -> Result<CallToolResult, McpError> {
        let valid_kinds = [
            "blocked_email",
            "blocked_domain",
            "allowed_email",
            "allowed_domain",
        ];
        if !valid_kinds.contains(&args.kind.as_str()) {
            return Err(McpError::invalid_params(
                format!(
                    "invalid kind '{}' — must be one of: {}",
                    args.kind,
                    valid_kinds.join(", ")
                ),
                None::<serde_json::Value>,
            ));
        }

        let entry = self
            .block_list_store
            .add_entry(&args.kind, &args.value, args.reason.as_deref())
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        let result = serde_json::json!({
            "id": entry.id,
            "kind": entry.kind,
            "value": entry.value,
            "reason": entry.reason,
            "created_at": entry.created_at.to_rfc3339(),
        });

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(description = "Remove (soft-delete) a block/allow-list entry by ID")]
    async fn remove_blocklist_entry(
        &self,
        Parameters(args): Parameters<RemoveBlocklistEntryArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.block_list_store
            .remove_entry(args.id)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None::<serde_json::Value>))?;

        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Blocklist entry {} removed",
            args.id
        ))]))
    }

    #[tool(description = "Check the health of the notification service database")]
    async fn health_check(&self) -> Result<CallToolResult, McpError> {
        let db_ok = sqlx::query("SELECT 1").execute(&self.pool).await.is_ok();

        let result = serde_json::json!({
            "status": if db_ok { "healthy" } else { "unhealthy" },
            "database": if db_ok { "connected" } else { "disconnected" },
            "service": "anvil-notify",
        });

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    #[tool(description = "Get information about the anvil-notify MCP server")]
    async fn get_server_info(&self) -> Result<CallToolResult, McpError> {
        let result = serde_json::json!({
            "service": "anvil-notify",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "MCP server for anvil-notify — transactional email delivery service",
            "tools": [
                "send_email",
                "check_delivery_status",
                "list_templates",
                "get_template",
                "upsert_template",
                "get_recipient_status",
                "list_blocklist",
                "add_blocklist_entry",
                "remove_blocklist_entry",
                "health_check",
                "get_server_info",
            ],
            "amqp_exchange": self.cfg.amqp.exchange,
            "amqp_routing_key": self.cfg.amqp.routing_key,
        });

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }
}

impl ServerHandler for NotifyServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::V_2024_11_05)
            .with_server_info(Implementation::from_build_env())
            .with_instructions(
                "AnvilNotify MCP Server. Provides email sending, delivery status checks, \
                template management, blocklist management, and health checks for the anvil-notify \
                transactional email service. Use send_email to publish an email event via RabbitMQ, \
                check_delivery_status to query delivery results, list_templates to see available \
                notification templates, and list_blocklist to view block/allow-list entries.",
            )
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_router.get(name).cloned()
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let tools = self.tool_router.list_all();
        Ok(ListToolsResult {
            tools,
            meta: None,
            next_cursor: None,
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if self.get_tool(&request.name).is_none() {
            return Err(McpError::invalid_params(
                "tool not found",
                None::<serde_json::Value>,
            ));
        }
        let tcc = ToolCallContext::new(self, request, context);
        self.tool_router.call(tcc).await
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SendEmailArgs {
    /// Event type (e.g. ORDER_CONFIRMATION). Uses GENERIC_HTML when --subject/--body-html/--body-text are supplied.
    pub event_type: String,
    /// Recipient email address
    pub to: String,
    /// Optional display name for the recipient
    pub name: Option<String>,
    /// Template payload as JSON string (default: {})
    #[serde(default = "default_payload")]
    pub payload: String,
    /// Override From email address
    pub from_email: Option<String>,
    /// Override From display name
    pub from_name: Option<String>,
    /// Subject line (requires body-html and body-text together)
    pub subject: Option<String>,
    /// HTML body (requires subject and body-text)
    pub body_html: Option<String>,
    /// Plain-text body (requires subject and body-html)
    pub body_text: Option<String>,
    /// Explicit event UUID for idempotency (auto-generated if omitted)
    pub event_id: Option<String>,
}

fn default_payload() -> String {
    "{}".to_string()
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct StatusArgs {
    /// Event UUID to check delivery status for
    pub event_id: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetTemplateArgs {
    /// Event type to look up (e.g. ORDER_CONFIRMATION)
    pub event_type: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct UpsertTemplateArgs {
    /// Event type (e.g. ORDER_CONFIRMATION)
    pub event_type: String,
    /// Email channel (defaults to "email")
    #[serde(default = "default_channel")]
    pub channel: String,
    /// Subject line (supports Handlebars template variables)
    pub subject: String,
    /// HTML body content
    pub body_html: String,
    /// Plain-text body content
    pub body_text: String,
    /// Whether the template is active (defaults to true)
    #[serde(default = "default_true")]
    pub active: bool,
}

fn default_channel() -> String {
    "email".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct RecipientStatusArgs {
    /// Event UUID
    pub event_id: String,
    /// Recipient email address
    pub email: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct AddBlocklistEntryArgs {
    /// Entry kind: blocked_email, blocked_domain, allowed_email, allowed_domain
    pub kind: String,
    /// The value to block/allow (email or domain)
    pub value: String,
    /// Optional reason for the entry
    pub reason: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct RemoveBlocklistEntryArgs {
    /// Entry ID to remove
    pub id: i64,
}
