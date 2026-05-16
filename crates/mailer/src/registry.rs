//! Registry of named SMTP sender accounts.
//!
//! At startup, `main.rs` builds a `SenderRegistry` from the
//! `[sender_accounts]` section of the config. The processor calls
//! `registry.resolve(name)` to get the right `EmailSender` for each event.
//!
//! Unknown account names fall back to the global default sender so that a
//! typo in a publisher doesn't silently drop the email — it logs a warning
//! and sends from the default account instead.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::warn;

use crate::EmailSender;

/// A cheaply-cloneable map from account name → sender instance.
#[derive(Clone, Default)]
pub struct SenderRegistry {
    accounts: HashMap<String, Arc<dyn EmailSender>>,
}

impl SenderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a named account. Called once at startup per configured account.
    pub fn register(&mut self, name: impl Into<String>, sender: Arc<dyn EmailSender>) {
        self.accounts.insert(name.into(), sender);
    }

    /// Return the sender for `account_name`, or `None` when the name is absent.
    ///
    /// The caller is responsible for falling back to the global default:
    /// ```rust,ignore
    /// let sender = registry
    ///     .resolve(event.sender_account.as_deref())
    ///     .unwrap_or_else(|| global_sender.clone());
    /// ```
    pub fn resolve(&self, account_name: Option<&str>) -> Option<Arc<dyn EmailSender>> {
        let name = account_name?;
        match self.accounts.get(name) {
            Some(s) => Some(Arc::clone(s)),
            None => {
                warn!(
                    account = name,
                    "sender_account not found in registry — falling back to global mailer"
                );
                None
            }
        }
    }
}
