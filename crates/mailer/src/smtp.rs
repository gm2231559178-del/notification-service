use async_trait::async_trait;
use common::AppError;
use lettre::{
    message::{header::ContentType, Attachment as LettreAttachment, MultiPart, SinglePart},
    transport::smtp::authentication::Credentials,
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};
use tracing::{info, instrument, warn};

use crate::{EmailMessage, EmailSender};

pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub from_email: String,
    pub from_name: String,
}

pub struct SmtpSender {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from_email: String,
    from_name: String,
}

impl SmtpSender {
    pub fn new(cfg: SmtpConfig) -> Result<Self, AppError> {
        // TLS mode is selected automatically by port:
        //   465          → implicit TLS (SMTPS)
        //   587 / 25     → STARTTLS
        //   anything else → plain (e.g. Mailpit / MailHog on port 1025)
        //
        // Credentials are only attached when a username is configured.
        // Dev catch-all servers (Mailpit, MailHog) advertise no auth mechanisms
        // and return "No compatible authentication mechanism was found" if the
        // client attempts a credential handshake even with valid credentials.
        let with_creds = !cfg.username.is_empty();

        macro_rules! build_transport {
            ($builder:expr) => {{
                let b = $builder
                    .map_err(|e| AppError::Mailer(e.to_string()))?
                    .port(cfg.port);
                if with_creds {
                    b.credentials(Credentials::new(cfg.username, cfg.password))
                        .build()
                } else {
                    b.build()
                }
            }};
        }

        let transport = match cfg.port {
            465 => build_transport!(AsyncSmtpTransport::<Tokio1Executor>::relay(&cfg.host)),
            587 | 25 => {
                build_transport!(AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(
                    &cfg.host
                ))
            }
            _ => {
                // builder_dangerous does not return a Result, so handle separately.
                let b = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&cfg.host)
                    .port(cfg.port);
                if with_creds {
                    b.credentials(Credentials::new(cfg.username, cfg.password))
                        .build()
                } else {
                    b.build()
                }
            }
        };

        Ok(Self {
            transport,
            from_email: cfg.from_email,
            from_name: cfg.from_name,
        })
    }
}

#[async_trait]
impl EmailSender for SmtpSender {
    #[instrument(skip(self, msg), fields(event_id = %msg.event_id, to = %msg.to_email))]
    async fn send(&self, msg: &EmailMessage) -> Result<(), AppError> {
        let from_email = msg
            .from_email_override
            .as_deref()
            .unwrap_or(&self.from_email);
        let from_name = msg.from_name_override.as_deref().unwrap_or(&self.from_name);
        let from = format!("{from_name} <{from_email}>")
            .parse()
            .map_err(|e: lettre::address::AddressError| AppError::Mailer(e.to_string()))?;

        let to = match &msg.to_name {
            Some(name) => format!("{name} <{}>", msg.to_email),
            None => msg.to_email.clone(),
        }
        .parse()
        .map_err(|e: lettre::address::AddressError| AppError::Mailer(e.to_string()))?;

        // ── Build text+html alternative ───────────────────────────────────────
        let alternative = MultiPart::alternative()
            .singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_PLAIN)
                    .body(msg.body_text.clone()),
            )
            .singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_HTML)
                    .body(msg.body_html.clone()),
            );

        // ── Wrap in multipart/mixed when attachments are present ──────────────
        //
        // MIME structure:
        //   multipart/mixed          (only when attachments present)
        //     multipart/alternative  (always: text + html)
        //     <attachment parts>…
        //
        // Without attachments we keep the simpler multipart/alternative so
        // mail clients don't show a spurious empty attachments section.
        let body = if msg.attachments.is_empty() {
            alternative
        } else {
            let mut mixed = MultiPart::mixed().multipart(alternative);
            for att in &msg.attachments {
                // Bytes are already fetched and validated by AttachmentFetcher.
                let content_type = att.content_type.parse::<ContentType>().map_err(|e| {
                    AppError::Mailer(format!(
                        "permanent: attachment '{}' has invalid content-type '{}': {e}",
                        att.filename, att.content_type
                    ))
                })?;

                mixed = mixed.singlepart(
                    LettreAttachment::new(att.filename.clone())
                        .body(att.data.clone(), content_type),
                );
            }
            mixed
        };

        let email = Message::builder()
            .from(from)
            .to(to)
            .subject(&msg.subject)
            .multipart(body)
            .map_err(|e| AppError::Mailer(e.to_string()))?;

        self.transport
            .send(email)
            .await
            .map_err(classify_smtp_error)?;

        info!(
            event_id    = %msg.event_id,
            attachments = msg.attachments.len(),
            "Email sent via SMTP"
        );
        Ok(())
    }
}

fn classify_smtp_error(err: lettre::transport::smtp::Error) -> AppError {
    // lettre 0.11 exposes boolean predicate methods on Error; the internal
    // ErrorKind enum and kind() accessor are private.
    if err.is_permanent() {
        // 5xx response: bad recipient, auth failure, policy rejection, etc.
        // Will never succeed on retry — route straight to DLQ.
        AppError::Mailer(format!("permanent: SMTP {err}"))
    } else if err.is_transient() {
        // 4xx response: server busy, quota, greylisting — retry later.
        warn!(smtp_error = %err, "SMTP 4xx transient — treating as rate-limited");
        AppError::RateLimited(format!("SMTP transient: {err}"))
    } else if err.is_timeout() || err.is_tls() {
        // Network-level failures — transient, worth retrying.
        AppError::Mailer(err.to_string())
    } else if err.is_client() {
        // Client-side error (invalid address format, builder error) — permanent.
        AppError::Mailer(format!("permanent: SMTP client error: {err}"))
    } else {
        // Unknown / transport shutdown — treat as transient.
        AppError::Mailer(err.to_string())
    }
}

pub fn is_permanent_smtp_error(err: &AppError) -> bool {
    matches!(err, AppError::Mailer(m) if m.starts_with("permanent:"))
}
