//! Error message sanitization to prevent information disclosure.

use regex::Regex;
use rmcp::model::{CallToolResult, Content};
use std::sync::LazyLock;

// Pre-compiled regex patterns for error sanitization
static UNIX_PATH_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"/[\w./-]+/[\w./-]+").unwrap());

static WINDOWS_PATH_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[A-Za-z]:\\/[\w./-]+").unwrap());

static IP_PORT_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}\b").unwrap());

static IP_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b").unwrap());

static OS_ERROR_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s*\(os error \d+\)").unwrap());

static MULTIPLE_SPACES_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").unwrap());

pub(crate) fn sanitize_error(error: &str) -> String {
    tracing::debug!("Original error (before sanitization): {}", error);

    let mut sanitized = error.to_string();

    // Remove file paths first (they might contain IP-like sequences)
    sanitized = UNIX_PATH_REGEX
        .replace_all(&sanitized, "[REDACTED]")
        .to_string();
    sanitized = WINDOWS_PATH_REGEX
        .replace_all(&sanitized, "[REDACTED]")
        .to_string();

    // Remove IP:port combinations
    sanitized = IP_PORT_REGEX
        .replace_all(&sanitized, "[REDACTED]")
        .to_string();

    // Remove standalone IP addresses
    sanitized = IP_REGEX.replace_all(&sanitized, "[REDACTED]").to_string();

    // Remove OS error codes
    sanitized = OS_ERROR_REGEX.replace_all(&sanitized, "").to_string();

    // Clean up multiple spaces and trim
    sanitized = MULTIPLE_SPACES_REGEX
        .replace_all(&sanitized, " ")
        .to_string();
    sanitized = sanitized.trim().to_string();

    // Remove trailing punctuation that looks odd after sanitization
    if sanitized.ends_with(':') || sanitized.ends_with(',') {
        sanitized.pop();
    }

    sanitized
}

pub(crate) fn error_response(message: impl Into<String>) -> CallToolResult {
    let sanitized = sanitize_error(&message.into());
    CallToolResult::error(vec![Content::text(sanitized)])
}

/// Macro to reduce boilerplate when returning error responses from tool handlers.
///
/// Instead of writing `Ok(error_response("message"))`, you can write `tool_error!("message")`.
/// Supports format strings with arguments.
///
/// # Examples
/// ```ignore
/// // Simple message
/// tool_error!("Missing required argument: sql");
///
/// // Format string with arguments
/// tool_error!("Table '{}' not found in database", table_name);
/// ```
#[macro_export]
macro_rules! tool_error {
    ($msg:expr) => {
        Ok($crate::server::error::error_response($msg))
    };
    ($fmt:expr, $($arg:expr),+ $(,)?) => {
        Ok($crate::server::error::error_response(format!($fmt, $($arg),+)))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_ip_port() {
        let error = "Connection to 192.168.1.1:3306 failed";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Connection to [REDACTED] failed");
    }

    #[test]
    fn test_sanitize_os_error() {
        let error = "Connection refused (os error 111)";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Connection refused");
    }

    #[test]
    fn test_sanitize_unix_path() {
        let error = "Error reading /home/user/config.toml permission denied";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Error reading [REDACTED] permission denied");
    }

    #[test]
    fn test_sanitize_standalone_ip() {
        let error = "Cannot connect to 10.96.84.170";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Cannot connect to [REDACTED]");
    }

    #[test]
    fn test_preserves_safe_content() {
        let error = "Table users not found in database";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Table users not found in database");
    }
}
