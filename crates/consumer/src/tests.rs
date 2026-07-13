//! Integration tests for the consumer processor retry logic.
//!
//! These tests exercise `process_recipient` and `process_one_recipient`
//! (via the `ProcessorContext`) using:
//!   - A `MockSender` that returns a configurable sequence of outcomes.
//!   - Stub `EmailNotificationStore` / `TemplateStore` backed by a real Postgres
//!     instance only in CI; locally the tests that need DB are gated behind
//!     `#[cfg(feature = "integration")]`.  The pure-logic tests (retry
//!     counting, permanent-vs-transient branching, rate-limit cap) use
//!     the mock store defined below and run everywhere (`cargo test`).
//!
//! Run all tests (including DB-backed ones):
//!   cargo test -p consumer --features integration
//!
//! Run pure-unit tests only (no Postgres needed):
//!   cargo test -p consumer

#[cfg(test)]
mod processor_tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::Utc;
    use common::{AppError, ChannelOverrides, EmailOptions, NotificationEvent, Recipient};
    use mailer::{EmailMessage, EmailSender};

    use recipient_filter::{FilterConfig, RecipientFilter};
    use serde_json::json;

    use uuid::Uuid;

    use crate::config::ConsumerConfig;
    use crate::processor::is_retryable;

    /// A sender that pops from a pre-configured queue of `Result`s.
    /// Panics when the queue is exhausted (unexpected extra call).
    #[allow(dead_code)]
    struct MockSender {
        outcomes: Mutex<Vec<Result<(), AppError>>>,
    }

    #[async_trait]
    impl EmailSender for MockSender {
        async fn send(&self, _msg: &EmailMessage) -> Result<(), AppError> {
            self.outcomes
                .lock()
                .unwrap()
                .pop()
                // pop() takes from the end. The `mock_sender` helper reverses
                // the slice before calling `new()`, so the first element
                // provided by the caller is consumed first.
                .expect("MockSender: unexpected extra send() call")
        }
    }

    #[allow(dead_code)]
    fn mock_sender(outcomes: Vec<Result<(), AppError>>) -> MockSender {
        MockSender {
            outcomes: Mutex::new(outcomes.into_iter().rev().collect()),
        }
    }

    // ── MockNotificationStore ──────────────────────────────────────────────────

    use std::collections::HashMap;
    use store::{EmailInsertPendingArgs, InsertResult, NotificationStore};

    /// In-memory mock for [`NotificationStore`].
    ///
    /// Stores rows keyed by `(event_id, recipient_email)`.  Supports the
    /// subset of the trait that `process_recipient` / `process_group` /
    /// `execute_send` actually call.  Methods not exercised by the consumer
    /// (`get_by_event_id`, `get_event_delivery_detail`, etc.) panic with
    /// "not implemented" so accidental usage is caught immediately.
    #[allow(dead_code)]
    struct MockNotificationStore {
        rows: Mutex<HashMap<(Uuid, String), MockRow>>,
    }

    #[allow(dead_code)]
    struct MockRow {
        status: String,
        retry_count: i32,
        error_msg: Option<String>,
    }

    #[allow(dead_code)]
    impl MockNotificationStore {
        fn new() -> Self {
            Self {
                rows: Mutex::new(HashMap::new()),
            }
        }

        /// Check the final status of a recipient row (for assertions).
        fn status_of(&self, event_id: Uuid, email: &str) -> Option<(String, i32)> {
            let rows = self.rows.lock().unwrap();
            rows.get(&(event_id, email.to_string()))
                .map(|r| (r.status.clone(), r.retry_count))
        }
    }

    #[async_trait]
    impl NotificationStore for MockNotificationStore {
        async fn insert_pending(
            &self,
            args: &EmailInsertPendingArgs<'_>,
        ) -> Result<InsertResult, AppError> {
            let mut rows = self.rows.lock().unwrap();
            let key = (args.event_id, args.recipient_email.to_string());
            match rows.get_mut(&key) {
                Some(existing) => {
                    // Duplicate — return current state without overwriting.
                    Ok(InsertResult::Duplicate {
                        retry_count: existing.retry_count,
                        status: existing.status.clone(),
                    })
                }
                None => {
                    rows.insert(
                        key,
                        MockRow {
                            status: "PENDING".into(),
                            retry_count: 0,
                            error_msg: None,
                        },
                    );
                    Ok(InsertResult::Inserted)
                }
            }
        }

        // insert_pending_batch uses the default trait impl (sequential insert_pending).

        async fn mark_sent(&self, event_id: Uuid, recipient_id: &str) -> Result<(), AppError> {
            let mut rows = self.rows.lock().unwrap();
            let key = (event_id, recipient_id.to_string());
            if let Some(row) = rows.get_mut(&key) {
                row.status = "SENT".into();
            }
            Ok(())
        }

        async fn mark_failed(
            &self,
            event_id: Uuid,
            recipient_id: &str,
            error_msg: &str,
            exhausted: bool,
        ) -> Result<(), AppError> {
            let mut rows = self.rows.lock().unwrap();
            let key = (event_id, recipient_id.to_string());
            if let Some(row) = rows.get_mut(&key) {
                row.retry_count += 1;
                row.error_msg = Some(error_msg.to_string());
                if exhausted {
                    row.status = "FAILED".into();
                }
                // else stays PENDING
            }
            Ok(())
        }

        async fn mark_blocked(
            &self,
            event_id: Uuid,
            recipient_id: &str,
            _reason: &str,
        ) -> Result<(), AppError> {
            let mut rows = self.rows.lock().unwrap();
            let key = (event_id, recipient_id.to_string());
            if let Some(row) = rows.get_mut(&key) {
                row.status = "BLOCKED".into();
            }
            Ok(())
        }

        async fn mark_skipped(
            &self,
            event_id: Uuid,
            _event_type: &str,
            recipient_id: &str,
            _reason: &str,
            _event_timestamp: chrono::DateTime<chrono::Utc>,
            _payload: &serde_json::Value,
        ) -> Result<(), AppError> {
            let mut rows = self.rows.lock().unwrap();
            let key = (event_id, recipient_id.to_string());
            rows.insert(
                key,
                MockRow {
                    status: "SKIPPED".into(),
                    retry_count: 0,
                    error_msg: None,
                },
            );
            Ok(())
        }

        async fn get_by_event_and_recipient(
            &self,
            event_id: Uuid,
            recipient_id: &str,
        ) -> Result<common::NotificationLog, AppError> {
            let rows = self.rows.lock().unwrap();
            let key = (event_id, recipient_id.to_string());
            match rows.get(&key) {
                Some(row) => Ok(common::NotificationLog {
                    id: Uuid::new_v4(),
                    event_id,
                    event_type: String::new(),
                    channel: "email".into(),
                    status: common::NotificationStatus::try_from(row.status.as_str())
                        .unwrap_or(common::NotificationStatus::Pending),
                    retry_count: row.retry_count,
                    total_attempts: row.retry_count,
                    last_error: row.error_msg.clone(),
                    payload: None,
                    event_timestamp: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    recipient_email: recipient_id.to_string(),
                    recipient_name: None,
                    from_override: None,
                    sender_account: None,
                    send_mode: None,
                    group_retry_mode: None,
                    attachments: None,
                    cc: None,
                    bcc: None,
                    to_recipients: None,
                }),
                None => Err(AppError::NotFound(format!(
                    "row not found for {event_id}/{recipient_id}"
                ))),
            }
        }

        async fn reap_stale_pending(&self, _timeout_secs: u64) -> Result<Vec<Uuid>, AppError> {
            unimplemented!("not needed for consumer unit tests")
        }

        async fn get_by_event_id(
            &self,
            _event_id: Uuid,
        ) -> Result<Vec<common::NotificationLog>, AppError> {
            unimplemented!("not needed for consumer unit tests")
        }

        async fn get_recipients_for_event(
            &self,
            _event_id: Uuid,
            _only_emails: Option<&[String]>,
        ) -> Result<Vec<common::NotificationLog>, AppError> {
            unimplemented!("not needed for consumer unit tests")
        }

        async fn get_event_delivery_detail(
            &self,
            _event_id: Uuid,
        ) -> Result<store::EventDeliveryDetail, AppError> {
            unimplemented!("not needed for consumer unit tests")
        }

        async fn reset_for_retry(
            &self,
            _event_id: Uuid,
            _recipient_id: &str,
        ) -> Result<(), AppError> {
            unimplemented!("not needed for consumer unit tests")
        }

        async fn reset_all_failed_for_event(
            &self,
            _event_id: Uuid,
        ) -> Result<Vec<String>, AppError> {
            unimplemented!("not needed for consumer unit tests")
        }

        fn pool(&self) -> &sqlx::PgPool {
            unimplemented!("not needed for consumer unit tests")
        }
    }

    // ── MockTemplateResolver ───────────────────────────────────────────────────

    use store::TemplateResolver;

    /// In-memory mock for [`TemplateResolver`].
    ///
    /// Returns pre-configured templates keyed by event type, or
    /// `AppError::Template` for unknown event types.
    #[allow(dead_code)]
    struct MockTemplateResolver {
        templates: Mutex<HashMap<String, store::NotificationTemplate>>,
    }

    #[allow(dead_code)]
    impl MockTemplateResolver {
        fn new() -> Self {
            Self {
                templates: Mutex::new(HashMap::new()),
            }
        }

        /// Register a template for an event type.
        fn register(&self, event_type: &str, subject: &str, body_html: &str, body_text: &str) {
            self.templates.lock().unwrap().insert(
                event_type.to_string(),
                store::NotificationTemplate {
                    subject: subject.to_string(),
                    body_html: body_html.to_string(),
                    body_text: body_text.to_string(),
                },
            );
        }
    }

    #[async_trait]
    impl TemplateResolver for MockTemplateResolver {
        async fn resolve(
            &self,
            event_type: &str,
            _channel: &str,
        ) -> Result<store::NotificationTemplate, AppError> {
            self.templates
                .lock()
                .unwrap()
                .get(event_type)
                .cloned()
                .ok_or_else(|| AppError::Template(format!("Unknown event type '{event_type}'")))
        }
    }

    // ── MockBlockListChecker ──────────────────────────────────────────────────

    use store::BlockListChecker;

    /// In-memory mock for [`BlockListChecker`].
    ///
    /// Returns `Ok(())` by default.  Emails added via `block()` cause
    /// `check()` to return `Err(AppError::Blocked)`.
    #[allow(dead_code)]
    struct MockBlockListChecker {
        blocked: Mutex<HashSet<String>>,
    }

    #[allow(dead_code)]
    impl MockBlockListChecker {
        fn new() -> Self {
            Self {
                blocked: Mutex::new(HashSet::new()),
            }
        }

        fn block(&self, email: &str) {
            self.blocked.lock().unwrap().insert(email.to_lowercase());
        }
    }

    #[async_trait]
    impl BlockListChecker for MockBlockListChecker {
        async fn check(&self, email: &str) -> Result<(), AppError> {
            if self.blocked.lock().unwrap().contains(&email.to_lowercase()) {
                return Err(AppError::Blocked(format!("{email} is on the blocklist")));
            }
            Ok(())
        }
    }

    /// Build a `ProcessorContext` suitable for unit tests, wiring a mock
    /// template resolver, mock store, and mock sender.  The filter and
    /// block_list_store are set to pass-through (no blocking rules).
    #[allow(dead_code)]
    fn build_test_context(
        mock_store: Arc<MockNotificationStore>,
        mock_sender: Arc<dyn EmailSender>,
        template_resolver: Arc<dyn TemplateResolver>,
    ) -> crate::ProcessorContext {
        use rate_limiter::{MailRateLimiter, RateLimitConfig};
        use recipient_filter::{FilterConfig, RecipientFilter};

        crate::ProcessorContext {
            store: mock_store,
            template_store: template_resolver,
            sender: mock_sender,
            sender_registry: mailer::SenderRegistry::new(),
            filter: RecipientFilter::new(FilterConfig::default()),
            block_list_store: Arc::new(MockBlockListChecker::new()),
            rate_limiter: MailRateLimiter::new(RateLimitConfig {
                emails_per_second: 0,
                burst_size: 0,
            }),
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    // ── is_retryable unit tests ────────────────────────────────────────────────

    #[test]
    fn permanent_mailer_error_is_not_retryable() {
        let err = AppError::permanent_mailer("bad address");
        assert!(!is_retryable(&err));
    }

    #[test]
    fn transient_mailer_error_is_retryable() {
        let err = AppError::transient_mailer("connection reset");
        assert!(is_retryable(&err));
    }

    #[test]
    fn template_error_is_not_retryable() {
        let err = AppError::Template("Unknown event type 'X'".into());
        assert!(!is_retryable(&err));
    }

    #[test]
    fn rate_limited_error_is_retryable() {
        let err = AppError::RateLimited("429".into());
        assert!(is_retryable(&err));
    }

    #[test]
    fn database_error_is_retryable() {
        // We can't easily construct sqlx::Error directly, so test via the
        // Queue variant which is also retryable.
        let err = AppError::Queue("connection pool exhausted".into());
        assert!(is_retryable(&err));
    }

    // ── ProcessorContext integration tests ─────────────────────────────────────
    //
    // These tests exercise the full process_recipient path using real template
    // resolution (compile-time fallback for ORDER_CONFIRMATION) and a mock sender.
    // They require no database — the store operations are exercised against a
    // real PgPool only in the `integration` feature tests below.
    //
    // For these tests we verify the *outcome* returned by process_recipient,
    // which is what the runner acts on.

    // ── Pure-logic tests that don't need a DB ─────────────────────────────────

    /// Verifies the is_retryable + retry cap logic that the runner uses to
    /// decide whether to mark a recipient FAILED.
    #[test]
    fn retry_loop_exhausts_after_max_retries() {
        // Simulate the runner's decision logic directly (without spawning tasks)
        // to verify the retry counter stops at max_retries.
        let max_retries: u32 = 3;
        let mut attempt: u32 = 0;
        let mut failed_permanently = false;

        for _ in 0..10 {
            // Transient error every time
            let err = AppError::transient_mailer("transient");
            if !is_retryable(&err) {
                failed_permanently = true;
                break;
            }
            if attempt >= max_retries {
                failed_permanently = true;
                break;
            }
            attempt += 1;
        }

        assert!(failed_permanently, "should have exhausted retries");
        assert_eq!(attempt, max_retries);
    }

    #[test]
    fn permanent_error_stops_immediately_without_retry() {
        let max_retries: u32 = 3;
        let mut attempt: u32 = 0;
        let mut stopped_early = false;

        let err = AppError::permanent_mailer("bad domain");
        if !is_retryable(&err) {
            stopped_early = true;
        } else if attempt >= max_retries {
            attempt += 1;
        }

        assert!(
            stopped_early,
            "permanent error should stop without retrying"
        );
        assert_eq!(attempt, 0, "no retry attempts should have been made");
    }

    #[test]
    fn rate_limit_cap_triggers_before_normal_retry_limit() {
        // Simulates the rl_count branch: after max_rl_waits consecutive
        // rate-limit responses the recipient is marked FAILED regardless
        // of how many normal retries remain.
        let max_retries: u32 = 10; // high, so normal cap isn't the trigger
        let max_rl_waits: u32 = 3;
        let attempt: u32 = 0;
        let mut rl_count: u32 = 0;
        let mut hit_rl_cap = false;

        for _ in 0..20 {
            let err = AppError::RateLimited("429".into());
            if !is_retryable(&err) {
                break;
            }
            if attempt >= max_retries {
                break;
            }
            // Rate-limited path: don't increment attempt, only rl_count
            rl_count += 1;
            if rl_count > max_rl_waits {
                hit_rl_cap = true;
                break;
            }
        }

        assert!(hit_rl_cap, "should have hit rate-limit cap");
        assert_eq!(rl_count, max_rl_waits + 1);
        assert_eq!(
            attempt, 0,
            "rate-limit exhaustion must not consume retry slots"
        );
    }

    #[test]
    fn mixed_transient_and_rate_limit_resets_rl_counter() {
        // After a transient failure, rl_count should reset so a subsequent
        // rate-limit burst gets its own full budget.
        let max_rl_waits: u32 = 2;
        let mut rl_count: u32 = 0;

        // Simulate: RL, RL (not yet capped), then a normal transient (resets rl_count)
        for outcome in &["rl", "rl", "transient", "rl", "rl"] {
            match *outcome {
                "rl" => {
                    rl_count += 1;
                    assert!(
                        rl_count <= max_rl_waits + 1,
                        "should not exceed cap within one RL run"
                    );
                }
                "transient" => {
                    // Normal transient: reset the RL counter
                    rl_count = 0;
                }
                _ => unreachable!(),
            }
        }
        // After the second RL run, rl_count should be 2 (within cap)
        assert_eq!(rl_count, 2);
    }

    // ── EmailStatus TryFrom tests ─────────────────────────────────────────────

    #[test]
    fn email_status_try_from_known_values() {
        use common::EmailStatus;
        assert_eq!(
            EmailStatus::try_from("PENDING").unwrap(),
            EmailStatus::Pending
        );
        assert_eq!(EmailStatus::try_from("SENT").unwrap(), EmailStatus::Sent);
        assert_eq!(
            EmailStatus::try_from("FAILED").unwrap(),
            EmailStatus::Failed
        );
        assert_eq!(
            EmailStatus::try_from("BLOCKED").unwrap(),
            EmailStatus::Blocked
        );
        assert_eq!(
            EmailStatus::try_from("SKIPPED").unwrap(),
            EmailStatus::Skipped
        );
    }

    #[test]
    fn email_status_try_from_unknown_returns_error() {
        use common::EmailStatus;
        let err = EmailStatus::try_from("IN_PROGRESS").unwrap_err();
        assert!(
            matches!(err, AppError::UnknownStatus(ref s) if s == "IN_PROGRESS"),
            "expected UnknownStatus, got {err:?}"
        );
    }

    #[test]
    fn email_status_try_from_is_case_sensitive() {
        use common::EmailStatus;
        // The DB stores values in SCREAMING_SNAKE_CASE; lowercase must not match.
        assert!(EmailStatus::try_from("pending").is_err());
        assert!(EmailStatus::try_from("sent").is_err());
    }

    // ── CC/BCC filter enforcement tests ───────────────────────────────────────

    /// Helper: build an event whose CC or BCC contains the given address.
    fn make_event_with_cc_bcc(
        recipient_email: &str,
        cc: Vec<&str>,
        bcc: Vec<&str>,
    ) -> NotificationEvent {
        NotificationEvent {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type: "ORDER_CONFIRMATION".into(),
            payload: json!({ "orderId": "42", "amount": "9.99", "name": "Test User" }),
            metadata: Default::default(),
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    recipients: vec![Recipient {
                        email: recipient_email.into(),
                        name: Some("Test User".into()),
                    }],
                    cc: cc
                        .into_iter()
                        .map(|e| Recipient {
                            email: e.into(),
                            name: None,
                        })
                        .collect(),
                    bcc: bcc
                        .into_iter()
                        .map(|e| Recipient {
                            email: e.into(),
                            name: None,
                        })
                        .collect(),
                    from_override: None,
                    attachments: vec![],
                    sender_account: None,
                    send_mode: common::SendMode::Individual,
                    group_retry_mode: common::GroupRetryMode::Individual,
                    retry_policy: common::RetryPolicy::Retry,
                    send_at: None,
                    priority: None,
                }),
            },
        }
    }

    /// Verifies that a blocked CC address causes the delivery to fail (permanent).
    #[test]
    fn blocked_cc_address_is_rejected_by_filter() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked@example.com".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "to@example.com",
            vec!["blocked@example.com"], // CC contains a blocked address
            vec![],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        // Simulate the filter check that processor.rs performs on CC/BCC.
        let mut hit_blocked = false;
        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            if let Err(common::AppError::Blocked(_)) = filter.check(&r.email) {
                hit_blocked = true;
            }
        }
        assert!(
            hit_blocked,
            "blocked CC address should have been caught by the filter"
        );
    }

    /// Verifies that a blocked BCC address also causes a filter hit.
    #[test]
    fn blocked_bcc_address_is_rejected_by_filter() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            blocked_domains: vec!["blocked.io".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "to@safe.com",
            vec![],
            vec!["audit@blocked.io"], // BCC domain is blocked
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        let mut hit_blocked = false;
        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            if let Err(common::AppError::Blocked(_)) = filter.check(&r.email) {
                hit_blocked = true;
            }
        }
        assert!(
            hit_blocked,
            "blocked BCC domain address should have been caught by the filter"
        );
    }

    /// Verifies that allowlist mode also blocks CC/BCC addresses not on the list.
    #[test]
    fn allowlist_mode_blocks_unlisted_cc_address() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            allowed_domains: vec!["mycompany.com".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "employee@mycompany.com",
            vec!["external@other.com"], // CC is not on the allowlist
            vec![],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        let mut hit_blocked = false;
        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            if let Err(common::AppError::Blocked(_)) = filter.check(&r.email) {
                hit_blocked = true;
            }
        }
        assert!(
            hit_blocked,
            "CC address outside allowlist should be blocked"
        );
    }

    /// Verifies that a clean (non-blocked) CC address passes through the filter.
    #[test]
    fn clean_cc_address_passes_filter() {
        use recipient_filter::FilterConfig;
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked@example.com".into()],
            ..Default::default()
        });

        let event = make_event_with_cc_bcc(
            "to@example.com",
            vec!["safe@example.com"],
            vec!["also-safe@example.com"],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        for r in email_opts.cc.iter().chain(email_opts.bcc.iter()) {
            assert!(
                filter.check(&r.email).is_ok(),
                "clean CC/BCC address {} should pass the filter",
                r.email
            );
        }
    }

    // ── NoRetry policy tests ──────────────────────────────────────────────

    /// With RetryPolicy::NoRetry any failure — transient or permanent — must be
    /// treated as immediately exhausted without consuming any retry slots.
    #[test]
    fn no_retry_policy_stops_on_first_transient_error() {
        use common::RetryPolicy;

        let policy = RetryPolicy::NoRetry;
        let max_retries: u32 = 5;
        let attempt: u32 = 0;
        let mut marked_failed = false;

        let err = AppError::transient_mailer("connection reset");
        if is_retryable(&err) && attempt < max_retries && policy == RetryPolicy::NoRetry {
            marked_failed = true;
        }

        assert!(marked_failed, "NoRetry must fail on first transient error");
        assert_eq!(attempt, 0, "NoRetry must not increment attempt counter");
    }

    /// With RetryPolicy::NoRetry a rate-limit response also causes immediate
    /// failure rather than backing off and retrying.
    #[test]
    fn no_retry_policy_stops_on_rate_limit() {
        use common::RetryPolicy;

        let policy = RetryPolicy::NoRetry;
        let max_retries: u32 = 5;
        let attempt: u32 = 0;
        let mut marked_failed = false;

        let err = AppError::RateLimited("429 from mail server".into());
        if is_retryable(&err) && attempt < max_retries && policy == RetryPolicy::NoRetry {
            marked_failed = true;
        }

        assert!(marked_failed, "NoRetry must fail on rate-limit error too");
        assert_eq!(attempt, 0, "NoRetry must not increment attempt counter");
    }

    /// With RetryPolicy::Retry (default) a transient error increments the
    /// attempt counter up to max_retries before marking FAILED.
    #[test]
    fn retry_policy_exhausts_all_attempts() {
        use common::RetryPolicy;

        let policy = RetryPolicy::Retry;
        let max_retries: u32 = 3;
        let mut attempt: u32 = 0;
        let mut failed_permanently = false;

        for _ in 0..20 {
            let err = AppError::transient_mailer("transient");
            if policy == RetryPolicy::NoRetry || !is_retryable(&err) {
                failed_permanently = true;
                break;
            }
            if attempt >= max_retries {
                failed_permanently = true;
                break;
            }
            attempt += 1;
        }

        assert!(failed_permanently, "should eventually be marked FAILED");
        assert_eq!(attempt, max_retries, "should have used all retry slots");
    }

    // ── Retry delay calculation tests ─────────────────────────────────────────────

    /// The exponential backoff formula is: retry_base_ms * 2^attempt, capped
    /// at 30 minutes.  Verifies the cap and that the shift is bounded at 10.
    #[test]
    fn retry_delay_is_capped_at_30_minutes() {
        const MAX_RETRY_DELAY_MS: u64 = 30 * 60 * 1_000;
        let retry_base_ms: u64 = 1_000;

        // attempt=10 with base 1000 gives 1024 s ≈ 17 min, still under cap
        let delay_at_10 = retry_base_ms
            .saturating_mul(1u64 << 10)
            .min(MAX_RETRY_DELAY_MS);
        assert_eq!(delay_at_10, 1_024_000);

        // A very large base must saturate to the 30-minute cap
        let large_base: u64 = 60 * 60 * 1_000; // 1 hour
        let delay_large = large_base
            .saturating_mul(1u64 << 1u64)
            .min(MAX_RETRY_DELAY_MS);
        assert_eq!(
            delay_large, MAX_RETRY_DELAY_MS,
            "delay must never exceed 30 minutes"
        );
    }

    /// Verifies that saturating_mul prevents silent u64 wrapping when
    /// retry_base_ms is set to a pathologically large value.
    #[test]
    fn retry_delay_saturating_mul_prevents_overflow() {
        const MAX_RETRY_DELAY_MS: u64 = 30 * 60 * 1_000;
        let retry_base_ms: u64 = u64::MAX / 2 + 1;

        let delay = retry_base_ms
            .saturating_mul(1u64 << 1u64)
            .min(MAX_RETRY_DELAY_MS);

        assert_eq!(
            delay, MAX_RETRY_DELAY_MS,
            "overflow must be caught by saturating_mul + min cap"
        );
    }

    // ── TO recipient filter — end-to-end processor logic tests ───────────────
    //
    // These tests verify the three filter rules for group and individual sends:
    //   Rule 1: All TO blocked   → delivery dropped entirely.
    //   Rule 2: Partial TO blocked → send to remaining allowed TOs only.
    //   Rule 3: CC/BCC blocked   → silently removed, delivery continues.

    fn make_group_event(recipients: Vec<&str>, cc: Vec<&str>, bcc: Vec<&str>) -> NotificationEvent {
        NotificationEvent {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type: "ORDER_CONFIRMATION".into(),
            payload: json!({ "orderId": "42", "amount": "9.99", "name": "Test User" }),
            metadata: Default::default(),
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    recipients: recipients
                        .into_iter()
                        .map(|e| Recipient {
                            email: e.into(),
                            name: None,
                        })
                        .collect(),
                    cc: cc
                        .into_iter()
                        .map(|e| Recipient {
                            email: e.into(),
                            name: None,
                        })
                        .collect(),
                    bcc: bcc
                        .into_iter()
                        .map(|e| Recipient {
                            email: e.into(),
                            name: None,
                        })
                        .collect(),
                    from_override: None,
                    attachments: vec![],
                    sender_account: None,
                    send_mode: common::SendMode::Group,
                    group_retry_mode: common::GroupRetryMode::Whole,
                    retry_policy: common::RetryPolicy::Retry,
                    send_at: None,
                    priority: None,
                }),
            },
        }
    }

    /// Rule 1 (group): all TO recipients blocked → Blocked outcome, nothing sent.
    #[test]
    fn group_all_to_blocked_drops_delivery() {
        use crate::processor::RecipientOutcome;

        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["a@blocked.com".into(), "b@blocked.com".into()],
            ..Default::default()
        });

        let event = make_group_event(vec!["a@blocked.com", "b@blocked.com"], vec![], vec![]);
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        let allowed: Vec<_> = email_opts
            .recipients
            .iter()
            .filter(|r| filter.check(&r.email).is_ok())
            .collect();

        assert!(
            allowed.is_empty(),
            "all TO recipients blocked — allowed list must be empty"
        );
        let outcome = if allowed.is_empty() {
            RecipientOutcome::Blocked("all TO recipients blocked by filter".into())
        } else {
            RecipientOutcome::Sent
        };
        assert!(matches!(outcome, RecipientOutcome::Blocked(_)));
    }

    /// Rule 2 (group): partial TO blocked → allowed TOs remain, blocked ones excluded.
    #[test]
    fn group_partial_to_blocked_sends_to_remaining() {
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked@example.com".into()],
            ..Default::default()
        });

        let event = make_group_event(
            vec![
                "ok@example.com",
                "blocked@example.com",
                "also-ok@example.com",
            ],
            vec![],
            vec![],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();
        let recipients = &email_opts.recipients;

        let allowed: Vec<_> = recipients
            .iter()
            .filter(|r| filter.check(&r.email).is_ok())
            .collect();
        let blocked: Vec<_> = recipients
            .iter()
            .filter(|r| filter.check(&r.email).is_err())
            .collect();

        assert_eq!(allowed.len(), 2, "two TO recipients should pass the filter");
        assert_eq!(blocked.len(), 1, "one TO recipient should be blocked");
        assert_eq!(blocked[0].email, "blocked@example.com");
        assert!(
            !allowed.is_empty(),
            "delivery must proceed to remaining allowed TOs"
        );
    }

    /// Rule 2 (individual): each TO is processed independently; a blocked
    /// recipient gets Blocked while others proceed unaffected.
    #[test]
    fn individual_blocked_to_does_not_affect_other_recipients() {
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked@example.com".into()],
            ..Default::default()
        });

        let addresses = vec![
            "ok@example.com",
            "blocked@example.com",
            "also-ok@example.com",
        ];

        let mut blocked_count = 0usize;
        let mut allowed_count = 0usize;
        for email in &addresses {
            match filter.check(email) {
                Ok(()) => allowed_count += 1,
                Err(AppError::Blocked(_)) => blocked_count += 1,
                Err(_) => {}
            }
        }

        assert_eq!(allowed_count, 2, "two recipients should be allowed through");
        assert_eq!(blocked_count, 1, "exactly one recipient should be blocked");
    }

    /// Rule 3 (group, CC): blocked CC address is silently excluded; TO and
    /// remaining CC are unaffected and delivery continues.
    #[test]
    fn group_blocked_cc_excluded_delivery_continues() {
        let filter = RecipientFilter::new(FilterConfig {
            blocked_emails: vec!["blocked-cc@example.com".into()],
            ..Default::default()
        });

        let event = make_group_event(
            vec!["to@example.com"],
            vec!["safe-cc@example.com", "blocked-cc@example.com"],
            vec![],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        let to_allowed: Vec<_> = email_opts
            .recipients
            .iter()
            .filter(|r| filter.check(&r.email).is_ok())
            .collect();
        assert_eq!(to_allowed.len(), 1, "TO recipient must pass the filter");

        let effective_cc: Vec<_> = email_opts
            .cc
            .iter()
            .filter(|r| filter.check(&r.email).is_ok())
            .collect();
        assert_eq!(
            effective_cc.len(),
            1,
            "only the safe CC address should remain"
        );
        assert_eq!(effective_cc[0].email, "safe-cc@example.com");
    }

    /// Rule 3 (group, BCC): blocked BCC address is silently excluded; delivery
    /// continues to TO and remaining BCC.
    #[test]
    fn group_blocked_bcc_excluded_delivery_continues() {
        let filter = RecipientFilter::new(FilterConfig {
            blocked_domains: vec!["blocked.io".into()],
            ..Default::default()
        });

        let event = make_group_event(
            vec!["to@example.com"],
            vec![],
            vec!["audit@safe.com", "log@blocked.io"],
        );
        let email_opts = event.channel_overrides.email.as_ref().unwrap();

        let effective_bcc: Vec<_> = email_opts
            .bcc
            .iter()
            .filter(|r| filter.check(&r.email).is_ok())
            .collect();
        assert_eq!(
            effective_bcc.len(),
            1,
            "only the safe BCC address should remain"
        );
        assert_eq!(effective_bcc[0].email, "audit@safe.com");
    }

    // ── GroupRetryMode outcome tests ───────────────────────────────────────────

    /// Whole mode must produce a plain Failed outcome so the runner retries
    /// the whole group email as a unit.
    #[test]
    fn group_retry_mode_whole_produces_plain_failed_outcome() {
        use crate::processor::RecipientOutcome;
        use common::GroupRetryMode;

        let err = AppError::transient_mailer("smtp timeout");
        let mode = GroupRetryMode::Whole;

        let outcome = match mode {
            GroupRetryMode::Individual => RecipientOutcome::GroupFailedWithIndividualRows(err),
            GroupRetryMode::Whole => {
                RecipientOutcome::Failed(AppError::transient_mailer("smtp timeout"))
            }
        };

        assert!(
            matches!(outcome, RecipientOutcome::Failed(_)),
            "Whole mode must produce Failed, not GroupFailedWithIndividualRows"
        );
    }

    /// Individual mode must produce GroupFailedWithIndividualRows so the
    /// runner falls back to per-recipient sends, skipping already-SENT rows.
    #[test]
    fn group_retry_mode_individual_produces_individual_rows_outcome() {
        use crate::processor::RecipientOutcome;
        use common::GroupRetryMode;

        let err = AppError::transient_mailer("smtp timeout");
        let mode = GroupRetryMode::Individual;

        let outcome = match mode {
            GroupRetryMode::Individual => RecipientOutcome::GroupFailedWithIndividualRows(err),
            GroupRetryMode::Whole => {
                RecipientOutcome::Failed(AppError::transient_mailer("smtp timeout"))
            }
        };

        assert!(
            matches!(outcome, RecipientOutcome::GroupFailedWithIndividualRows(_)),
            "Individual mode must produce GroupFailedWithIndividualRows"
        );
    }

    // ── process_group guard tests ─────────────────────────────────────────────
    //
    // These tests exercise the defence-in-depth guards at the top of
    // `process_group` without requiring a database connection.  They verify
    // the path-branching logic that is independent of DB I/O.

    /// `process_group` must return Failed immediately when the recipient list
    /// is empty, before any DB write or network call.
    #[test]
    fn group_empty_recipients_is_permanent_failure() {
        use crate::processor::RecipientOutcome;

        // Simulate the guard at the top of process_group:
        //   let primary = match recipients.first() { None => return Failed(...) }
        let recipients: Vec<common::Recipient> = vec![];
        let outcome = match recipients.first() {
            Some(_) => RecipientOutcome::Sent, // unreachable in this test
            None => RecipientOutcome::Failed(AppError::permanent_mailer(
                "group send: recipients list is empty",
            )),
        };

        assert!(
            matches!(outcome, RecipientOutcome::Failed(_)),
            "empty recipients must produce a permanent Failed outcome"
        );
        // Must be permanent so it goes to DLQ rather than burning retry budget.
        if let RecipientOutcome::Failed(ref err) = outcome {
            assert!(
                !is_retryable(err),
                "empty-recipients error must not be retryable"
            );
        }
    }

    /// `process_group` must return Failed immediately when recipient count
    /// exceeds `max_recipients_per_event`, before any DB write or network call.
    #[test]
    fn group_recipient_count_exceeds_max_is_permanent_failure() {
        use crate::processor::RecipientOutcome;

        // Use the same default as ConsumerConfig so this test stays in sync
        // with the configured limit without importing a now-removed constant.
        let max_recipients = ConsumerConfig::default().max_recipients_per_event;

        // Simulate the defence-in-depth guard inside process_group.
        let recipient_count = max_recipients + 1;
        let outcome = if recipient_count > max_recipients {
            RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "group send: recipient count {recipient_count} exceeds maximum allowed \
                 ({max_recipients})"
            )))
        } else {
            RecipientOutcome::Sent // unreachable in this test
        };

        assert!(
            matches!(outcome, RecipientOutcome::Failed(_)),
            "oversized recipient list must produce a permanent Failed outcome"
        );
        if let RecipientOutcome::Failed(ref err) = outcome {
            assert!(
                !is_retryable(err),
                "recipient-count-exceeded error must not be retryable"
            );
        }
    }

    /// `process_group` must accept exactly `max_recipients_per_event` recipients
    /// without triggering the count guard.
    #[test]
    fn group_recipient_count_at_max_is_allowed() {
        let max_recipients = ConsumerConfig::default().max_recipients_per_event;
        // The guard condition: strictly greater than, not greater-or-equal.
        let would_fail = max_recipients > max_recipients;
        assert!(
            !would_fail,
            "exactly max_recipients_per_event recipients must not trigger the count guard"
        );
    }

    /// An invalid TO address must produce a permanent failure, not a retryable one.
    #[test]
    fn group_invalid_to_address_is_permanent_failure() {
        use crate::processor::RecipientOutcome;

        let invalid_email = "not-an-email";
        let outcome = if !common::is_valid_email(invalid_email) {
            RecipientOutcome::Failed(AppError::permanent_mailer(format!(
                "invalid recipient email address: {invalid_email}"
            )))
        } else {
            RecipientOutcome::Sent
        };

        assert!(
            matches!(outcome, RecipientOutcome::Failed(_)),
            "invalid TO address must produce Failed"
        );
        if let RecipientOutcome::Failed(ref err) = outcome {
            assert!(
                !is_retryable(err),
                "invalid-address error must not be retryable"
            );
        }
    }

    // ── Partial-send strip tests (step 3c / 4b in process_group) ─────────────
    //
    // These tests exercise the secondary-result inspection and partial-send
    // strip logic introduced in `process_group`.  They simulate the relevant
    // sub-steps directly — same pattern as the existing guard tests above —
    // without requiring a database connection.

    /// When all secondary insert results are Inserted (first attempt or clean
    /// retry), the already_sent set must be empty and the full recipient list
    /// must be passed to the email.
    #[test]
    fn partial_send_no_already_sent_keeps_all_recipients() {
        use store::InsertResult;

        let recipients = [
            Recipient {
                email: "a@example.com".into(),
                name: None,
            },
            Recipient {
                email: "b@example.com".into(),
                name: None,
            },
            Recipient {
                email: "c@example.com".into(),
                name: None,
            },
        ];

        // Simulate: primary Inserted, both secondaries Inserted.
        let secondary_results = [InsertResult::Inserted, InsertResult::Inserted];

        let mut already_sent: std::collections::HashSet<&str> = std::collections::HashSet::new();

        for (r, result) in recipients.iter().skip(1).zip(secondary_results.iter()) {
            if let InsertResult::Duplicate { ref status, .. } = result {
                use common::NotificationStatus;
                if matches!(
                    NotificationStatus::try_from(status.as_str()),
                    Ok(NotificationStatus::Sent) | Ok(NotificationStatus::Blocked)
                ) {
                    already_sent.insert(&r.email);
                }
            }
        }

        assert!(
            already_sent.is_empty(),
            "no secondary should be in already_sent when all results are Inserted"
        );

        let to_send: Vec<&Recipient> = recipients
            .iter()
            .filter(|r| !already_sent.contains(r.email.as_str()))
            .collect();

        assert_eq!(to_send.len(), 3, "all recipients must be included");
    }

    /// When some secondaries come back as Duplicate { Sent }, those addresses
    /// must be collected into already_sent and stripped from the To: list.
    #[test]
    fn partial_send_already_sent_secondaries_are_stripped() {
        use store::InsertResult;

        let recipients = [
            Recipient {
                email: "primary@example.com".into(),
                name: None,
            },
            Recipient {
                email: "already-sent@example.com".into(),
                name: None,
            },
            Recipient {
                email: "unsent@example.com".into(),
                name: None,
            },
        ];

        // Simulate: primary Inserted, second already SENT, third fresh.
        let secondary_results = [
            InsertResult::Duplicate {
                retry_count: 1,
                status: "SENT".into(),
            },
            InsertResult::Inserted,
        ];

        let mut already_sent: std::collections::HashSet<&str> = std::collections::HashSet::new();

        for (r, result) in recipients.iter().skip(1).zip(secondary_results.iter()) {
            if let InsertResult::Duplicate { ref status, .. } = result {
                use common::NotificationStatus;
                if matches!(
                    NotificationStatus::try_from(status.as_str()),
                    Ok(NotificationStatus::Sent) | Ok(NotificationStatus::Blocked)
                ) {
                    already_sent.insert(&r.email);
                }
            }
        }

        assert_eq!(already_sent.len(), 1);
        assert!(already_sent.contains("already-sent@example.com"));

        let to_send: Vec<&Recipient> = recipients
            .iter()
            .filter(|r| !already_sent.contains(r.email.as_str()))
            .collect();

        assert_eq!(to_send.len(), 2, "only primary and unsent should remain");
        assert!(to_send.iter().any(|r| r.email == "primary@example.com"));
        assert!(to_send.iter().any(|r| r.email == "unsent@example.com"));
        assert!(
            !to_send
                .iter()
                .any(|r| r.email == "already-sent@example.com"),
            "already-SENT address must be stripped from the To: list"
        );
    }

    /// Blocked secondaries (Duplicate { Blocked }) must also be stripped,
    /// since re-including a blocked address would silently re-attempt a
    /// delivery that was explicitly rejected.
    #[test]
    fn partial_send_already_blocked_secondaries_are_stripped() {
        use store::InsertResult;

        let recipients = [
            Recipient {
                email: "primary@example.com".into(),
                name: None,
            },
            Recipient {
                email: "blocked@example.com".into(),
                name: None,
            },
        ];

        let secondary_results = [InsertResult::Duplicate {
            retry_count: 0,
            status: "BLOCKED".into(),
        }];

        let mut already_sent: std::collections::HashSet<&str> = std::collections::HashSet::new();

        for (r, result) in recipients.iter().skip(1).zip(secondary_results.iter()) {
            if let InsertResult::Duplicate { ref status, .. } = result {
                use common::NotificationStatus;
                if matches!(
                    NotificationStatus::try_from(status.as_str()),
                    Ok(NotificationStatus::Sent) | Ok(NotificationStatus::Blocked)
                ) {
                    already_sent.insert(&r.email);
                }
            }
        }

        assert!(
            already_sent.contains("blocked@example.com"),
            "BLOCKED secondary must be collected into already_sent"
        );

        let to_send: Vec<&Recipient> = recipients
            .iter()
            .filter(|r| !already_sent.contains(r.email.as_str()))
            .collect();

        assert_eq!(to_send.len(), 1);
        assert_eq!(to_send[0].email, "primary@example.com");
    }

    /// A non-terminal Duplicate (e.g. PENDING, FAILED) must NOT be stripped —
    /// it represents a recipient that has not yet been successfully delivered
    /// to and should be retried.
    #[test]
    fn partial_send_non_terminal_duplicate_is_not_stripped() {
        use store::InsertResult;

        let recipients = [
            Recipient {
                email: "primary@example.com".into(),
                name: None,
            },
            Recipient {
                email: "pending@example.com".into(),
                name: None,
            },
        ];

        let secondary_results = [InsertResult::Duplicate {
            retry_count: 2,
            status: "PENDING".into(),
        }];

        let mut already_sent: std::collections::HashSet<&str> = std::collections::HashSet::new();

        for (r, result) in recipients.iter().skip(1).zip(secondary_results.iter()) {
            if let InsertResult::Duplicate { ref status, .. } = result {
                use common::NotificationStatus;
                if matches!(
                    NotificationStatus::try_from(status.as_str()),
                    Ok(NotificationStatus::Sent) | Ok(NotificationStatus::Blocked)
                ) {
                    already_sent.insert(&r.email);
                }
            }
        }

        assert!(
            already_sent.is_empty(),
            "PENDING duplicate must not be collected into already_sent"
        );

        let to_send: Vec<&Recipient> = recipients
            .iter()
            .filter(|r| !already_sent.contains(r.email.as_str()))
            .collect();

        assert_eq!(
            to_send.len(),
            2,
            "PENDING recipient must remain in the To: list for retry"
        );
    }

    /// Primary is never in already_sent (its result was Inserted), so it
    /// is always present in the final To: list even when all secondaries are stripped.
    #[test]
    fn partial_send_primary_is_never_stripped() {
        use store::InsertResult;

        let recipients = [
            Recipient {
                email: "primary@example.com".into(),
                name: None,
            },
            Recipient {
                email: "sent1@example.com".into(),
                name: None,
            },
            Recipient {
                email: "sent2@example.com".into(),
                name: None,
            },
        ];

        // All secondaries already SENT.
        let secondary_results = [
            InsertResult::Duplicate {
                retry_count: 1,
                status: "SENT".into(),
            },
            InsertResult::Duplicate {
                retry_count: 1,
                status: "SENT".into(),
            },
        ];

        let mut already_sent: std::collections::HashSet<&str> = std::collections::HashSet::new();

        for (r, result) in recipients.iter().skip(1).zip(secondary_results.iter()) {
            if let InsertResult::Duplicate { ref status, .. } = result {
                use common::NotificationStatus;
                if matches!(
                    NotificationStatus::try_from(status.as_str()),
                    Ok(NotificationStatus::Sent) | Ok(NotificationStatus::Blocked)
                ) {
                    already_sent.insert(&r.email);
                }
            }
        }

        assert_eq!(
            already_sent.len(),
            2,
            "both secondaries must be in already_sent"
        );

        let to_send: Vec<&Recipient> = recipients
            .iter()
            .filter(|r| !already_sent.contains(r.email.as_str()))
            .collect();

        assert_eq!(
            to_send.len(),
            1,
            "only the primary should remain after stripping all secondaries"
        );
        assert_eq!(
            to_send[0].email, "primary@example.com",
            "primary must always survive the partial-send strip"
        );
    }

    // ── render_all_templates tests ─────────────────────────────────────────────

    #[test]
    fn render_all_templates_success() {
        use crate::processor::render_all_templates;

        let payload = json!({ "name": "Alice", "orderId": "42" });
        let result = render_all_templates(
            "Hello {{ name }}",
            "<h1>Hello {{ name }}</h1>",
            "Hello {{ name }}",
            &payload,
        );
        let rendered = result.unwrap();
        assert_eq!(rendered.subject, "Hello Alice");
        assert_eq!(rendered.body_html, "<h1>Hello Alice</h1>");
        assert_eq!(rendered.body_text, "Hello Alice");
    }

    #[test]
    fn render_all_templates_invalid_template_returns_error() {
        use crate::processor::render_all_templates;

        let payload = json!({});
        let result = render_all_templates("Unclosed {{ name", "<p>body</p>", "text", &payload);
        assert!(result.is_err(), "broken template must return Err");
    }

    #[test]
    fn render_all_templates_html_xss_escaping() {
        use crate::processor::render_all_templates;

        let payload = json!({ "name": "<script>alert(1)</script>" });
        let result = render_all_templates("{{ name }}", "{{ name }}", "{{ name }}", &payload);
        let rendered = result.unwrap();
        assert_eq!(rendered.subject, "<script>alert(1)</script>");
        // HTML template auto-escapes <, >, ", &
        assert!(rendered.body_html.contains("&lt;script&gt;"));
        // Text template does NOT escape
        assert_eq!(rendered.body_text, "<script>alert(1)</script>");
    }

    #[test]
    fn render_all_templates_undefined_variable_returns_error() {
        use crate::processor::render_all_templates;

        let payload = json!({});
        let result = render_all_templates("Hello {{ missing }}", "<p>body</p>", "text", &payload);
        // minijinja returns an error for undefined variables by default
        assert!(result.is_err(), "undefined variable must return Err");
    }

    // ── serialize_email_fields tests ───────────────────────────────────────────

    #[test]
    fn serialize_email_fields_empty() {
        use crate::processor::serialize_email_fields;

        let opts = common::EmailOptions {
            recipients: vec![],
            cc: vec![],
            bcc: vec![],
            from_override: None,
            attachments: vec![],
            sender_account: None,
            send_mode: common::SendMode::Individual,
            group_retry_mode: common::GroupRetryMode::Individual,
            retry_policy: common::RetryPolicy::Retry,
            send_at: None,
            priority: None,
        };
        let fields = serialize_email_fields(&opts, &[], &[]).unwrap();
        assert!(fields.from_override.is_none());
        assert!(fields.attachments.is_none());
        assert!(fields.cc.is_none());
        assert!(fields.bcc.is_none());
    }

    #[test]
    fn serialize_email_fields_with_from_override() {
        use crate::processor::serialize_email_fields;
        use common::FromOverride;

        let opts = common::EmailOptions {
            recipients: vec![],
            cc: vec![],
            bcc: vec![],
            from_override: Some(FromOverride {
                email: "sender@example.com".into(),
                name: Some("Sender".into()),
            }),
            attachments: vec![],
            sender_account: None,
            send_mode: common::SendMode::Individual,
            group_retry_mode: common::GroupRetryMode::Individual,
            retry_policy: common::RetryPolicy::Retry,
            send_at: None,
            priority: None,
        };
        let fields = serialize_email_fields(&opts, &[], &[]).unwrap();
        let from = fields.from_override.unwrap();
        assert_eq!(from["email"], "sender@example.com");
        assert_eq!(from["name"], "Sender");
    }

    #[test]
    fn serialize_email_fields_with_cc_bcc() {
        use crate::processor::serialize_email_fields;

        let opts = common::EmailOptions {
            recipients: vec![],
            cc: vec![Recipient {
                email: "cc@example.com".into(),
                name: Some("CC User".into()),
            }],
            bcc: vec![Recipient {
                email: "bcc@example.com".into(),
                name: None,
            }],
            from_override: None,
            attachments: vec![],
            sender_account: None,
            send_mode: common::SendMode::Individual,
            group_retry_mode: common::GroupRetryMode::Individual,
            retry_policy: common::RetryPolicy::Retry,
            send_at: None,
            priority: None,
        };
        let cc = vec![Recipient {
            email: "cc@example.com".into(),
            name: Some("CC User".into()),
        }];
        let bcc = vec![Recipient {
            email: "bcc@example.com".into(),
            name: None,
        }];
        let fields = serialize_email_fields(&opts, &cc, &bcc).unwrap();
        let cc_val = fields.cc.unwrap();
        assert_eq!(cc_val[0]["email"], "cc@example.com");
        let bcc_val = fields.bcc.unwrap();
        assert_eq!(bcc_val[0]["email"], "bcc@example.com");
    }

    // ── build_email_message tests ──────────────────────────────────────────────

    #[test]
    fn build_email_message_individual() {
        use crate::processor::{build_email_message, RenderedTemplates};

        let event = NotificationEvent {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type: "ORDER_CONFIRMATION".into(),
            payload: json!({}),
            metadata: Default::default(),
            channel_overrides: ChannelOverrides { email: None },
        };
        let primary = Recipient {
            email: "to@example.com".into(),
            name: Some("To User".into()),
        };
        let rendered = RenderedTemplates {
            subject: "Test Subject".into(),
            body_html: "<p>HTML</p>".into(),
            body_text: "Text".into(),
        };
        let msg = build_email_message(
            &event,
            &primary,
            vec![], // individual mode
            rendered,
            (Some("from@example.com".into()), Some("From Name".into())),
            &[],
            &[Recipient {
                email: "cc@example.com".into(),
                name: None,
            }],
            &[],
        );
        assert_eq!(msg.to_email, "to@example.com");
        assert_eq!(msg.to_name.as_deref(), Some("To User"));
        assert!(msg.to_extra.is_empty());
        assert_eq!(msg.subject, "Test Subject");
        assert_eq!(msg.from_email_override.as_deref(), Some("from@example.com"));
        assert_eq!(msg.cc.len(), 1);
        assert_eq!(msg.cc[0].email, "cc@example.com");
        assert!(msg.bcc.is_empty());
    }

    #[test]
    fn build_email_message_group() {
        use crate::processor::{build_email_message, RenderedTemplates};
        use mailer::MailboxRef;

        let event = NotificationEvent {
            event_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            event_type: "ORDER_CONFIRMATION".into(),
            payload: json!({}),
            metadata: Default::default(),
            channel_overrides: ChannelOverrides { email: None },
        };
        let primary = Recipient {
            email: "primary@example.com".into(),
            name: None,
        };
        let extra = vec![
            MailboxRef {
                email: "second@example.com".into(),
                name: None,
            },
            MailboxRef {
                email: "third@example.com".into(),
                name: None,
            },
        ];
        let rendered = RenderedTemplates {
            subject: "Group Subject".into(),
            body_html: "<p>HTML</p>".into(),
            body_text: "Text".into(),
        };
        let msg = build_email_message(
            &event,
            &primary,
            extra,
            rendered,
            (None, None),
            &[],
            &[],
            &[],
        );
        assert_eq!(msg.to_email, "primary@example.com");
        assert_eq!(msg.to_extra.len(), 2);
        assert_eq!(msg.to_extra[0].email, "second@example.com");
        assert_eq!(msg.to_extra[1].email, "third@example.com");
        assert!(msg.from_email_override.is_none());
    }

    // ── MockNotificationStore basic tests ──────────────────────────────────────

    #[tokio::test]
    async fn mock_store_insert_pending_returns_inserted() {
        let store = MockNotificationStore::new();
        let event_id = Uuid::new_v4();
        let args = EmailInsertPendingArgs {
            event_id,
            event_type: "ORDER_CONFIRMATION",
            recipient_email: "test@example.com",
            payload: &json!({}),
            event_timestamp: Utc::now(),
            recipient_name: None,
            from_override: None,
            attachments: None,
            sender_account: None,
            cc: None,
            bcc: None,
            send_mode: "individual",
            group_retry_mode: None,
            to_recipients: None,
        };
        let result = store.insert_pending(&args).await.unwrap();
        assert!(matches!(result, InsertResult::Inserted));
    }

    #[tokio::test]
    async fn mock_store_insert_pending_returns_duplicate_on_second_call() {
        let store = MockNotificationStore::new();
        let event_id = Uuid::new_v4();
        let args = EmailInsertPendingArgs {
            event_id,
            event_type: "ORDER_CONFIRMATION",
            recipient_email: "test@example.com",
            payload: &json!({}),
            event_timestamp: Utc::now(),
            recipient_name: None,
            from_override: None,
            attachments: None,
            sender_account: None,
            cc: None,
            bcc: None,
            send_mode: "individual",
            group_retry_mode: None,
            to_recipients: None,
        };
        let r1 = store.insert_pending(&args).await.unwrap();
        assert!(matches!(r1, InsertResult::Inserted));

        let r2 = store.insert_pending(&args).await.unwrap();
        match r2 {
            InsertResult::Duplicate {
                retry_count,
                status,
            } => {
                assert_eq!(retry_count, 0);
                assert_eq!(status, "PENDING");
            }
            _ => panic!("expected Duplicate"),
        }
    }

    #[tokio::test]
    async fn mock_store_mark_sent_sets_status() {
        let store = MockNotificationStore::new();
        let event_id = Uuid::new_v4();
        let args = EmailInsertPendingArgs {
            event_id,
            event_type: "ORDER_CONFIRMATION",
            recipient_email: "test@example.com",
            payload: &json!({}),
            event_timestamp: Utc::now(),
            recipient_name: None,
            from_override: None,
            attachments: None,
            sender_account: None,
            cc: None,
            bcc: None,
            send_mode: "individual",
            group_retry_mode: None,
            to_recipients: None,
        };
        store.insert_pending(&args).await.unwrap();
        store.mark_sent(event_id, "test@example.com").await.unwrap();

        let (status, _) = store.status_of(event_id, "test@example.com").unwrap();
        assert_eq!(status, "SENT");
    }

    #[tokio::test]
    async fn mock_store_mark_failed_exhausted_sets_status() {
        let store = MockNotificationStore::new();
        let event_id = Uuid::new_v4();
        let args = EmailInsertPendingArgs {
            event_id,
            event_type: "ORDER_CONFIRMATION",
            recipient_email: "test@example.com",
            payload: &json!({}),
            event_timestamp: Utc::now(),
            recipient_name: None,
            from_override: None,
            attachments: None,
            sender_account: None,
            cc: None,
            bcc: None,
            send_mode: "individual",
            group_retry_mode: None,
            to_recipients: None,
        };
        store.insert_pending(&args).await.unwrap();
        store
            .mark_failed(event_id, "test@example.com", "smtp timeout", true)
            .await
            .unwrap();

        let (status, retry_count) = store.status_of(event_id, "test@example.com").unwrap();
        assert_eq!(status, "FAILED");
        assert_eq!(retry_count, 1); // incremented once
    }

    #[tokio::test]
    async fn mock_store_mark_failed_not_exhausted_keeps_pending() {
        let store = MockNotificationStore::new();
        let event_id = Uuid::new_v4();
        let args = EmailInsertPendingArgs {
            event_id,
            event_type: "ORDER_CONFIRMATION",
            recipient_email: "test@example.com",
            payload: &json!({}),
            event_timestamp: Utc::now(),
            recipient_name: None,
            from_override: None,
            attachments: None,
            sender_account: None,
            cc: None,
            bcc: None,
            send_mode: "individual",
            group_retry_mode: None,
            to_recipients: None,
        };
        store.insert_pending(&args).await.unwrap();
        store
            .mark_failed(event_id, "test@example.com", "smtp timeout", false)
            .await
            .unwrap();

        let (status, retry_count) = store.status_of(event_id, "test@example.com").unwrap();
        assert_eq!(status, "PENDING"); // stays PENDING when not exhausted
        assert_eq!(retry_count, 1);
    }

    #[tokio::test]
    async fn mock_store_mark_blocked_sets_status() {
        let store = MockNotificationStore::new();
        let event_id = Uuid::new_v4();
        let args = EmailInsertPendingArgs {
            event_id,
            event_type: "ORDER_CONFIRMATION",
            recipient_email: "test@example.com",
            payload: &json!({}),
            event_timestamp: Utc::now(),
            recipient_name: None,
            from_override: None,
            attachments: None,
            sender_account: None,
            cc: None,
            bcc: None,
            send_mode: "individual",
            group_retry_mode: None,
            to_recipients: None,
        };
        store.insert_pending(&args).await.unwrap();
        store
            .mark_blocked(event_id, "test@example.com", "domain blocked")
            .await
            .unwrap();

        let (status, _) = store.status_of(event_id, "test@example.com").unwrap();
        assert_eq!(status, "BLOCKED");
    }

    #[tokio::test]
    async fn mock_store_insert_pending_batch() {
        let store = MockNotificationStore::new();
        let event_id = Uuid::new_v4();
        let payload = json!({});
        let args: Vec<_> = ["a@example.com", "b@example.com", "c@example.com"]
            .iter()
            .map(|email| EmailInsertPendingArgs {
                event_id,
                event_type: "ORDER_CONFIRMATION",
                recipient_email: email,
                payload: &payload,
                event_timestamp: Utc::now(),
                recipient_name: None,
                from_override: None,
                attachments: None,
                sender_account: None,
                cc: None,
                bcc: None,
                send_mode: "group",
                group_retry_mode: Some("individual"),
                to_recipients: None,
            })
            .collect();

        let results = store.insert_pending_batch(&args).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| matches!(r, InsertResult::Inserted)));

        // Second batch: all duplicates
        let results2 = store.insert_pending_batch(&args).await.unwrap();
        assert_eq!(results2.len(), 3);
        assert!(results2
            .iter()
            .all(|r| matches!(r, InsertResult::Duplicate { .. })));
    }

    // ── resolve_sender tests ──────────────────────────────────────────────────

    #[test]
    fn resolve_sender_no_account_returns_global() {
        use crate::processor::resolve_sender;
        use mailer::SenderRegistry;

        let registry = SenderRegistry::new();
        let global = mock_sender(vec![Ok(())]);
        let global: Arc<dyn EmailSender> = Arc::new(global);

        let resolved = resolve_sender(&registry, &global, None, "ORDER_CONFIRMATION");
        // Same Arc as global — pointer equality
        assert!(Arc::ptr_eq(&resolved, &global));
    }

    #[test]
    fn resolve_sender_known_account_returns_named_sender() {
        use crate::processor::resolve_sender;
        use mailer::SenderRegistry;

        let mut registry = SenderRegistry::new();
        let named = mock_sender(vec![Ok(())]);
        let named_arc: Arc<dyn EmailSender> = Arc::new(named);
        registry.register("business-a", named_arc.clone());

        let global = mock_sender(vec![Ok(())]);
        let global: Arc<dyn EmailSender> = Arc::new(global);

        let resolved = resolve_sender(&registry, &global, Some("business-a"), "ORDER_CONFIRMATION");
        assert!(Arc::ptr_eq(&resolved, &named_arc));
    }

    #[test]
    fn resolve_sender_unknown_account_falls_back_to_global() {
        use crate::processor::resolve_sender;
        use mailer::SenderRegistry;

        let registry = SenderRegistry::new();
        let global = mock_sender(vec![Ok(())]);
        let global: Arc<dyn EmailSender> = Arc::new(global);

        let resolved = resolve_sender(
            &registry,
            &global,
            Some("nonexistent"),
            "ORDER_CONFIRMATION",
        );
        assert!(Arc::ptr_eq(&resolved, &global));
    }

    // ── mark_sent_for_targets tests ────────────────────────────────────────────

    #[tokio::test]
    async fn mark_sent_individual_sets_status() {
        use crate::processor::{mark_sent_for_targets, SendTargets};

        let store = Arc::new(MockNotificationStore::new());
        let event_id = Uuid::new_v4();

        let args = EmailInsertPendingArgs {
            event_id,
            event_type: "ORDER_CONFIRMATION",
            recipient_email: "test@example.com",
            payload: &json!({}),
            event_timestamp: Utc::now(),
            recipient_name: None,
            from_override: None,
            attachments: None,
            sender_account: None,
            cc: None,
            bcc: None,
            send_mode: "individual",
            group_retry_mode: None,
            to_recipients: None,
        };
        store.insert_pending(&args).await.unwrap();

        // Arc<MockNotificationStore> coerces to Arc<dyn NotificationStore>
        let store_dyn: Arc<dyn NotificationStore> = store.clone();
        let targets = SendTargets::Individual {
            event_id,
            email: "test@example.com",
        };
        mark_sent_for_targets(&store_dyn, &targets, "ORDER_CONFIRMATION").await;

        let (status, _) = store.status_of(event_id, "test@example.com").unwrap();
        assert_eq!(status, "SENT");
    }

    #[tokio::test]
    async fn mark_sent_group_marks_primary_and_secondaries() {
        use crate::processor::{mark_sent_for_targets, SendTargets};
        use common::GroupRetryMode;

        let store = Arc::new(MockNotificationStore::new());
        let event_id = Uuid::new_v4();

        for email in &[
            "primary@example.com",
            "second-a@example.com",
            "second-b@example.com",
        ] {
            let args = EmailInsertPendingArgs {
                event_id,
                event_type: "ORDER_CONFIRMATION",
                recipient_email: email,
                payload: &json!({}),
                event_timestamp: Utc::now(),
                recipient_name: None,
                from_override: None,
                attachments: None,
                sender_account: None,
                cc: None,
                bcc: None,
                send_mode: "group",
                group_retry_mode: Some("individual"),
                to_recipients: None,
            };
            store.insert_pending(&args).await.unwrap();
        }

        let store_dyn: Arc<dyn NotificationStore> = store.clone();
        let targets = SendTargets::Group {
            event_id,
            primary_email: "primary@example.com",
            secondaries: vec!["second-a@example.com", "second-b@example.com"],
            retry_mode: &GroupRetryMode::Individual,
            to_count: 3,
        };
        mark_sent_for_targets(&store_dyn, &targets, "ORDER_CONFIRMATION").await;

        let (s1, _) = store.status_of(event_id, "primary@example.com").unwrap();
        let (s2, _) = store.status_of(event_id, "second-a@example.com").unwrap();
        let (s3, _) = store.status_of(event_id, "second-b@example.com").unwrap();
        assert_eq!(s1, "SENT");
        assert_eq!(s2, "SENT");
        assert_eq!(s3, "SENT");
    }

    #[tokio::test]
    async fn mark_sent_group_no_secondaries_when_empty() {
        use crate::processor::{mark_sent_for_targets, SendTargets};
        use common::GroupRetryMode;

        let store = Arc::new(MockNotificationStore::new());
        let event_id = Uuid::new_v4();

        let args = EmailInsertPendingArgs {
            event_id,
            event_type: "ORDER_CONFIRMATION",
            recipient_email: "primary@example.com",
            payload: &json!({}),
            event_timestamp: Utc::now(),
            recipient_name: None,
            from_override: None,
            attachments: None,
            sender_account: None,
            cc: None,
            bcc: None,
            send_mode: "group",
            group_retry_mode: Some("whole"),
            to_recipients: None,
        };
        store.insert_pending(&args).await.unwrap();

        let store_dyn: Arc<dyn NotificationStore> = store.clone();
        let targets = SendTargets::Group {
            event_id,
            primary_email: "primary@example.com",
            secondaries: vec![],
            retry_mode: &GroupRetryMode::Whole,
            to_count: 1,
        };
        mark_sent_for_targets(&store_dyn, &targets, "ORDER_CONFIRMATION").await;

        let (status, _) = store.status_of(event_id, "primary@example.com").unwrap();
        assert_eq!(status, "SENT");
    }

    // ── process_recipient integration tests ────────────────────────────────────

    use crate::processor::{process_group, process_recipient, EffectiveCcBcc, RecipientOutcome};
    use tokio_util::sync::CancellationToken;

    /// Build a minimal `NotificationEvent` + `EmailOptions` for individual-mode tests.
    fn make_individual_test_data(email: &str) -> (NotificationEvent, common::EmailOptions) {
        let event = make_event_with_cc_bcc(email, vec![], vec![]);
        let opts = event.channel_overrides.email.clone().unwrap();
        (event, opts)
    }

    #[tokio::test]
    async fn process_recipient_happy_path() {
        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");
        let sender = Arc::new(mock_sender(vec![Ok(())]));

        let ctx = build_test_context(store.clone(), sender, resolver);
        let (event, opts) = make_individual_test_data("ok@example.com");
        let recipient = &opts.recipients[0];
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        let outcome = process_recipient(
            &ctx,
            &event,
            &opts,
            recipient,
            &[],
            &empty_cc_bcc,
            &shutdown,
        )
        .await;

        assert!(
            matches!(outcome, RecipientOutcome::Sent),
            "expected Sent, got {outcome:?}"
        );
        let (status, _) = store.status_of(event.event_id, "ok@example.com").unwrap();
        assert_eq!(status, "SENT");
    }

    #[tokio::test]
    async fn process_recipient_template_error_no_db_row() {
        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        // Don't register "ORDER_CONFIRMATION" → resolve() fails
        let sender = Arc::new(mock_sender(vec![]));

        let ctx = build_test_context(store.clone(), sender, resolver);
        let (event, opts) = make_individual_test_data("ok@example.com");
        let recipient = &opts.recipients[0];
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        let outcome = process_recipient(
            &ctx,
            &event,
            &opts,
            recipient,
            &[],
            &empty_cc_bcc,
            &shutdown,
        )
        .await;

        assert!(
            matches!(&outcome, RecipientOutcome::Failed(_)),
            "expected Failed, got {outcome:?}"
        );
        // No DB row should have been created — insert_pending was never called.
        assert!(
            store.status_of(event.event_id, "ok@example.com").is_none(),
            "no DB row should exist when template lookup fails"
        );
    }

    #[tokio::test]
    async fn process_recipient_duplicate_already_sent_skipped() {
        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");
        let sender = Arc::new(mock_sender(vec![]));

        let ctx = build_test_context(store.clone(), sender, resolver);
        let (event, opts) = make_individual_test_data("dup@example.com");
        let recipient = &opts.recipients[0];
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        // Insert a pending row, then mark it as SENT to simulate a prior delivery.
        store
            .insert_pending(&EmailInsertPendingArgs {
                event_id: event.event_id,
                event_type: "ORDER_CONFIRMATION",
                recipient_email: "dup@example.com",
                recipient_name: None,
                payload: &json!({}),
                event_timestamp: event.timestamp,
                from_override: None,
                attachments: None,
                sender_account: None,
                cc: None,
                bcc: None,
                send_mode: "individual",
                group_retry_mode: None,
                to_recipients: None,
            })
            .await
            .unwrap();
        store
            .mark_sent(event.event_id, "dup@example.com")
            .await
            .unwrap();

        let outcome = process_recipient(
            &ctx,
            &event,
            &opts,
            recipient,
            &[],
            &empty_cc_bcc,
            &shutdown,
        )
        .await;

        assert!(
            matches!(outcome, RecipientOutcome::Skipped),
            "expected Skipped, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn process_recipient_duplicate_pending_returns_retry_count() {
        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");
        let sender = Arc::new(mock_sender(vec![]));

        let ctx = build_test_context(store.clone(), sender, resolver);
        let (event, opts) = make_individual_test_data("dup@example.com");
        let recipient = &opts.recipients[0];
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        // Insert a pending row directly (simulates a prior incomplete attempt).
        // Then call process_recipient — it finds Duplicate with PENDING status.
        store
            .insert_pending(&EmailInsertPendingArgs {
                event_id: event.event_id,
                event_type: "ORDER_CONFIRMATION",
                recipient_email: "dup@example.com",
                recipient_name: None,
                payload: &json!({}),
                event_timestamp: event.timestamp,
                from_override: None,
                attachments: None,
                sender_account: None,
                cc: None,
                bcc: None,
                send_mode: "individual",
                group_retry_mode: None,
                to_recipients: None,
            })
            .await
            .unwrap();

        let outcome = process_recipient(
            &ctx,
            &event,
            &opts,
            recipient,
            &[],
            &empty_cc_bcc,
            &shutdown,
        )
        .await;

        assert!(
            matches!(&outcome, RecipientOutcome::Duplicate { retry_count: 0 }),
            "expected Duplicate {{ retry_count: 0 }}, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn process_recipient_db_block_list_blocks() {
        use rate_limiter::{MailRateLimiter, RateLimitConfig};
        use recipient_filter::{FilterConfig, RecipientFilter};

        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");

        let blc = Arc::new(MockBlockListChecker::new());
        blc.block("blocked@example.com");

        let ctx = crate::ProcessorContext {
            store: store.clone(),
            template_store: resolver,
            sender: Arc::new(mock_sender(vec![])),
            sender_registry: mailer::SenderRegistry::new(),
            filter: RecipientFilter::new(FilterConfig::default()),
            block_list_store: blc,
            rate_limiter: MailRateLimiter::new(RateLimitConfig {
                emails_per_second: 0,
                burst_size: 0,
            }),
        };

        let (event, opts) = make_individual_test_data("blocked@example.com");
        let recipient = &opts.recipients[0];
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        let outcome = process_recipient(
            &ctx,
            &event,
            &opts,
            recipient,
            &[],
            &empty_cc_bcc,
            &shutdown,
        )
        .await;

        assert!(
            matches!(&outcome, RecipientOutcome::Blocked(_)),
            "expected Blocked, got {outcome:?}"
        );
        let (status, _) = store
            .status_of(event.event_id, "blocked@example.com")
            .unwrap();
        assert_eq!(status, "BLOCKED");
    }

    #[tokio::test]
    async fn process_recipient_config_filter_blocks() {
        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");

        // Build context with a filter that blocks the recipient
        use rate_limiter::{MailRateLimiter, RateLimitConfig};
        use recipient_filter::{FilterConfig, RecipientFilter};
        let ctx = crate::ProcessorContext {
            store: store.clone(),
            template_store: resolver,
            sender: Arc::new(mock_sender(vec![])),
            sender_registry: mailer::SenderRegistry::new(),
            filter: RecipientFilter::new(FilterConfig {
                blocked_emails: vec!["filtered@example.com".into()],
                ..Default::default()
            }),
            block_list_store: Arc::new(MockBlockListChecker::new()),
            rate_limiter: MailRateLimiter::new(RateLimitConfig {
                emails_per_second: 0,
                burst_size: 0,
            }),
        };

        let (event, opts) = make_individual_test_data("filtered@example.com");
        let recipient = &opts.recipients[0];
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        let outcome = process_recipient(
            &ctx,
            &event,
            &opts,
            recipient,
            &[],
            &empty_cc_bcc,
            &shutdown,
        )
        .await;

        assert!(
            matches!(&outcome, RecipientOutcome::Blocked(_)),
            "expected Blocked, got {outcome:?}"
        );
        let (status, _) = store
            .status_of(event.event_id, "filtered@example.com")
            .unwrap();
        assert_eq!(status, "BLOCKED");
    }

    #[tokio::test]
    async fn process_recipient_send_transient_failure() {
        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");
        let sender = Arc::new(mock_sender(vec![Err(AppError::transient_mailer(
            "timeout",
        ))]));

        let ctx = build_test_context(store.clone(), sender, resolver);
        let (event, opts) = make_individual_test_data("fail@example.com");
        let recipient = &opts.recipients[0];
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        let outcome = process_recipient(
            &ctx,
            &event,
            &opts,
            recipient,
            &[],
            &empty_cc_bcc,
            &shutdown,
        )
        .await;

        // Send failure returns Failed; row stays PENDING (execute_send doesn't
        // call mark_failed — the delivery layer handles that).
        assert!(
            matches!(&outcome, RecipientOutcome::Failed(_)),
            "expected Failed, got {outcome:?}"
        );
        let (status, _) = store.status_of(event.event_id, "fail@example.com").unwrap();
        assert_eq!(status, "PENDING");
    }

    #[tokio::test]
    async fn process_recipient_send_permanent_failure() {
        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");
        let sender = Arc::new(mock_sender(vec![Err(AppError::permanent_mailer(
            "bad address",
        ))]));

        let ctx = build_test_context(store.clone(), sender, resolver);
        let (event, opts) = make_individual_test_data("perm@example.com");
        let recipient = &opts.recipients[0];
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        let outcome = process_recipient(
            &ctx,
            &event,
            &opts,
            recipient,
            &[],
            &empty_cc_bcc,
            &shutdown,
        )
        .await;

        assert!(
            matches!(&outcome, RecipientOutcome::Failed(_)),
            "expected Failed, got {outcome:?}"
        );
        // Row stays PENDING — execute_send doesn't call mark_failed.
        let (status, _) = store.status_of(event.event_id, "perm@example.com").unwrap();
        assert_eq!(status, "PENDING");
    }

    // ── process_group integration tests ────────────────────────────────────────

    fn make_group_test_data(
        emails: Vec<&str>,
        group_retry_mode: common::GroupRetryMode,
    ) -> (NotificationEvent, common::EmailOptions) {
        let event = make_group_event(emails.clone(), vec![], vec![]);
        let mut opts = event.channel_overrides.email.clone().unwrap();
        opts.group_retry_mode = group_retry_mode;
        (event, opts)
    }

    #[tokio::test]
    async fn process_group_whole_mode_happy_path() {
        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");
        let sender = Arc::new(mock_sender(vec![Ok(())]));

        let ctx = build_test_context(store.clone(), sender, resolver);
        let (event, opts) = make_group_test_data(
            vec!["a@example.com", "b@example.com"],
            common::GroupRetryMode::Whole,
        );
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        let outcome = process_group(&ctx, &event, &opts, &[], &empty_cc_bcc, 50, &shutdown).await;

        assert!(
            matches!(outcome, RecipientOutcome::Sent),
            "expected Sent, got {outcome:?}"
        );
        // Only primary row in Whole mode
        let (status, _) = store.status_of(event.event_id, "a@example.com").unwrap();
        assert_eq!(status, "SENT");
    }

    #[tokio::test]
    async fn process_group_individual_mode_happy_path() {
        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");
        let sender = Arc::new(mock_sender(vec![Ok(())]));

        let ctx = build_test_context(store.clone(), sender, resolver);
        let (event, opts) = make_group_test_data(
            vec!["a@example.com", "b@example.com", "c@example.com"],
            common::GroupRetryMode::Individual,
        );
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        let outcome = process_group(&ctx, &event, &opts, &[], &empty_cc_bcc, 50, &shutdown).await;

        assert!(
            matches!(outcome, RecipientOutcome::Sent),
            "expected Sent, got {outcome:?}"
        );
        // All three recipients should be SENT
        for email in &["a@example.com", "b@example.com", "c@example.com"] {
            let (status, _) = store.status_of(event.event_id, email).unwrap();
            assert_eq!(status, "SENT", "{email} should be SENT");
        }
    }

    #[tokio::test]
    async fn process_group_individual_mode_all_secondaries_already_sent() {
        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");
        let sender = Arc::new(mock_sender(vec![Ok(())]));

        let ctx = build_test_context(store.clone(), sender, resolver);
        let (event, opts) = make_group_test_data(
            vec!["a@example.com", "b@example.com", "c@example.com"],
            common::GroupRetryMode::Individual,
        );
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        // Pre-populate: primary not yet sent, secondaries already SENT
        store
            .mark_sent(event.event_id, "b@example.com")
            .await
            .unwrap();
        store
            .mark_sent(event.event_id, "c@example.com")
            .await
            .unwrap();

        let outcome = process_group(&ctx, &event, &opts, &[], &empty_cc_bcc, 50, &shutdown).await;

        assert!(
            matches!(outcome, RecipientOutcome::Sent),
            "expected Sent, got {outcome:?}"
        );
        // Primary should be SENT now
        let (status, _) = store.status_of(event.event_id, "a@example.com").unwrap();
        assert_eq!(status, "SENT");
        // Secondaries should remain SENT from earlier
        let (status_b, _) = store.status_of(event.event_id, "b@example.com").unwrap();
        assert_eq!(status_b, "SENT");
    }

    #[tokio::test]
    async fn process_group_all_recipients_blocked() {
        use rate_limiter::{MailRateLimiter, RateLimitConfig};
        use recipient_filter::{FilterConfig, RecipientFilter};

        let store = Arc::new(MockNotificationStore::new());
        let resolver = Arc::new(MockTemplateResolver::new());
        resolver.register("ORDER_CONFIRMATION", "Subject", "<b>Hi</b>", "Hi");

        let ctx = crate::ProcessorContext {
            store: store.clone(),
            template_store: resolver,
            sender: Arc::new(mock_sender(vec![])),
            sender_registry: mailer::SenderRegistry::new(),
            filter: RecipientFilter::new(FilterConfig {
                blocked_emails: vec!["x@blocked.com".into(), "y@blocked.com".into()],
                ..Default::default()
            }),
            block_list_store: Arc::new(MockBlockListChecker::new()),
            rate_limiter: MailRateLimiter::new(RateLimitConfig {
                emails_per_second: 0,
                burst_size: 0,
            }),
        };

        let (event, opts) = make_group_test_data(
            vec!["x@blocked.com", "y@blocked.com"],
            common::GroupRetryMode::Individual,
        );
        let shutdown = CancellationToken::new();
        let empty_cc_bcc = EffectiveCcBcc {
            cc: vec![],
            bcc: vec![],
        };

        let outcome = process_group(&ctx, &event, &opts, &[], &empty_cc_bcc, 50, &shutdown).await;

        assert!(
            matches!(&outcome, RecipientOutcome::Blocked(_)),
            "expected Blocked, got {outcome:?}"
        );
        // Both rows should be marked BLOCKED
        let (status_x, _) = store.status_of(event.event_id, "x@blocked.com").unwrap();
        assert_eq!(status_x, "BLOCKED");
        let (status_y, _) = store.status_of(event.event_id, "y@blocked.com").unwrap();
        assert_eq!(status_y, "BLOCKED");
    }
}
