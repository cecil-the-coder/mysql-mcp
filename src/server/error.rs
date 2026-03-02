//! Error message formatting for tool responses.
//!
//! Note: This module no longer performs aggressive sanitization of IPs, paths, or hostnames.
//! In a database MCP tool, the user explicitly provides connection details (host, port, credentials),
//! so redacting this information from error messages would hide useful debugging context.
//! The user already knows their own server addresses and file paths.

use rmcp::model::{CallToolResult, Content};

/// Format an error message for return to the MCP client.
///
/// Performs minimal cleanup: trims whitespace and removes trailing punctuation
/// that looks odd in error messages.
pub(crate) fn sanitize_error(error: &str) -> String {
    let sanitized = error.trim();

    // Remove trailing punctuation that looks odd
    sanitized
        .strip_suffix(':')
        .or_else(|| sanitized.strip_suffix(','))
        .unwrap_or(sanitized)
        .to_string()
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
    fn test_sanitize_trims_whitespace() {
        let error = "  Connection failed  ";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Connection failed");
    }

    #[test]
    fn test_sanitize_removes_trailing_colon() {
        let error = "Connection failed:";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Connection failed");
    }

    #[test]
    fn test_sanitize_removes_trailing_comma() {
        let error = "Connection failed,";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Connection failed");
    }

    #[test]
    fn test_preserves_connection_details() {
        // IPs and paths are no longer redacted - the user already knows these
        let error = "Connection to 192.168.1.1:3306 failed";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Connection to 192.168.1.1:3306 failed");
    }

    #[test]
    fn test_preserves_paths() {
        let error = "Error reading /home/user/config.toml: permission denied";
        let sanitized = sanitize_error(error);
        // Trailing colon removed, but path preserved
        assert_eq!(
            sanitized,
            "Error reading /home/user/config.toml: permission denied"
        );
    }

    #[test]
    fn test_preserves_os_errors() {
        let error = "Connection refused (os error 111)";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Connection refused (os error 111)");
    }

    #[test]
    fn test_preserves_safe_content() {
        let error = "Table users not found in database";
        let sanitized = sanitize_error(error);
        assert_eq!(sanitized, "Table users not found in database");
    }
}
