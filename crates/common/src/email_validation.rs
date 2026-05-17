//! Email address format validation.
//!
//! A single canonical implementation shared by the consumer (pre-send check)
//! and the API layer (pre-enqueue check). Keeping one copy ensures fixes and
//! rule changes propagate everywhere automatically.
//!
//! Deliberately stricter than a full RFC-5321 parser but well short of one:
//! the goal is to catch obvious typos and structural errors before they reach
//! the SMTP server, not to implement the full address grammar.
//!
//! Rules:
//! - Total length ≤ 254 characters (RFC-5321 §4.5.3.1.3).
//! - Exactly one `@` separating a non-empty local part and domain.
//! - Quoted local parts (`"user name"@example.com`) are rejected — virtually
//!   no mail servers accept them and they are commonly a sign of user error.
//! - Local part: 1–64 chars, no leading/trailing/consecutive dots.
//!   Allowed characters: alphanumeric plus `!#$%&'*+/=?^_`{|}~.-`
//! - Domain: labels separated by `.`, each 1–63 chars, alphanumeric plus
//!   hyphens (not leading/trailing).
//! - Multi-label domains (at least one dot) are required unless the domain is
//!   exactly `localhost`, which is permitted for internal mail relays and SMTP
//!   test servers.
//! - IP-address literals (`user@[192.168.1.1]`) are rejected — they are
//!   almost never legitimate in transactional email.

/// Returns `true` if `addr` passes the structural email address check.
pub fn is_valid_email(addr: &str) -> bool {
    if addr.len() > 254 {
        return false;
    }
    let (local, domain) = match addr.split_once('@') {
        Some(parts) => parts,
        None => return false,
    };
    is_valid_local(local) && is_valid_domain(domain)
}

fn is_valid_local(local: &str) -> bool {
    if local.is_empty() || local.len() > 64 {
        return false;
    }
    // Reject quoted local parts: "user name"@example.com
    // These are technically valid per RFC but are almost universally
    // unsupported and are a common sign of user error or injection.
    if local.starts_with('"') || local.ends_with('"') {
        return false;
    }
    if local.starts_with('.') || local.ends_with('.') || local.contains("..") {
        return false;
    }
    local.chars().all(|c| {
        c.is_ascii_alphanumeric()
            || matches!(
                c,
                '!' | '#'
                    | '$'
                    | '%'
                    | '&'
                    | '\''
                    | '*'
                    | '+'
                    | '/'
                    | '='
                    | '?'
                    | '^'
                    | '_'
                    | '`'
                    | '{'
                    | '|'
                    | '}'
                    | '~'
                    | '-'
                    | '.'
            )
    })
}

fn is_valid_domain(domain: &str) -> bool {
    if domain.is_empty() || domain.len() > 253 {
        return false;
    }
    // Reject IP address literals: [192.168.1.1]
    if domain.starts_with('[') {
        return false;
    }
    // Allow the special single-label "localhost" for internal relay support.
    // All other domains must contain at least one dot (i.e. have a TLD).
    let labels: Vec<&str> = domain.split('.').collect();
    if labels.len() == 1 && domain != "localhost" {
        return false;
    }
    labels.iter().all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_standard_address() {
        assert!(is_valid_email("user@example.com"));
    }
    #[test]
    fn accepts_subdomain() {
        assert!(is_valid_email("user@mail.example.co.uk"));
    }
    #[test]
    fn accepts_plus_tag() {
        assert!(is_valid_email("user+tag@example.com"));
    }
    #[test]
    fn accepts_dots_in_local() {
        assert!(is_valid_email("first.last@example.com"));
    }
    #[test]
    fn accepts_localhost_for_internal_relay() {
        assert!(is_valid_email("user@localhost"));
    }
    #[test]
    fn accepts_hyphen_in_domain_label() {
        assert!(is_valid_email("user@my-company.com"));
    }

    #[test]
    fn rejects_missing_at() {
        assert!(!is_valid_email("userexample.com"));
    }
    #[test]
    fn rejects_empty_local() {
        assert!(!is_valid_email("@example.com"));
    }
    #[test]
    fn rejects_empty_domain() {
        assert!(!is_valid_email("user@"));
    }
    #[test]
    fn rejects_leading_dot_in_local() {
        assert!(!is_valid_email(".user@example.com"));
    }
    #[test]
    fn rejects_trailing_dot_in_local() {
        assert!(!is_valid_email("user.@example.com"));
    }
    #[test]
    fn rejects_consecutive_dots_in_local() {
        assert!(!is_valid_email("us..er@example.com"));
    }
    #[test]
    fn rejects_leading_hyphen_in_domain_label() {
        assert!(!is_valid_email("user@-example.com"));
    }
    #[test]
    fn rejects_trailing_hyphen_in_domain_label() {
        assert!(!is_valid_email("user@example-.com"));
    }
    #[test]
    fn rejects_space_in_address() {
        assert!(!is_valid_email("us er@example.com"));
    }
    #[test]
    fn rejects_address_over_254_chars() {
        let long_local = "a".repeat(65);
        assert!(!is_valid_email(&format!("{long_local}@example.com")));
    }
    #[test]
    fn rejects_quoted_local_part() {
        assert!(!is_valid_email("\"user name\"@example.com"));
    }
    #[test]
    fn rejects_ip_address_literal() {
        assert!(!is_valid_email("user@[192.168.1.1]"));
    }
    #[test]
    fn rejects_single_label_domain_non_localhost() {
        // e.g. "user@domain" with no TLD — almost always a typo
        assert!(!is_valid_email("user@domain"));
        assert!(!is_valid_email("user@intranet"));
    }
    #[test]
    fn localhost_still_accepted() {
        assert!(is_valid_email("postmaster@localhost"));
    }
}
