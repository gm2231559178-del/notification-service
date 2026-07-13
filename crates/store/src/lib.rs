pub mod cli_queries;
pub mod notification_log;
pub mod template_store;

// Core multi-channel store — primary public API.
pub use notification_log::{
    EmailInsertPendingArgs, EmailNotificationStore, EventDeliveryDetail, InsertResult,
    NotificationStore, CHANNEL_EMAIL,
};
pub use template_store::{
    EmailTemplate, NotificationTemplate, TemplateResolver, TemplateRow, TemplateStore,
};

pub mod block_list_store;
pub use block_list_store::{BlockListChecker, BlockListEntry, BlockListStore};
