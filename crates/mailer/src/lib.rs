pub mod fetcher;
pub mod message;
pub mod registry;
pub mod sender;
pub mod smtp;
pub mod template;
pub mod webhook;

pub use fetcher::{fetch_attachments, fetch_attachments_with_limit};
pub use message::{EmailMessage, ResolvedAttachment};
pub use registry::SenderRegistry;
pub use sender::EmailSender;
pub use smtp::SmtpSender;
pub use template::{render_html_template, render_template, templates_for};
pub use webhook::WebhookSender;
