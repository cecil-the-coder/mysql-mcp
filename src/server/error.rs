//! Error message formatting for tool responses.
//!
//! Note: This module no longer performs aggressive sanitization of IPs, paths, or hostnames.
//! In a database MCP tool, the user explicitly provides connection details (host, port, credentials),
//! so redacting this information from error messages would hide useful debugging context.
//! The user already knows their own server addresses and file paths.

use rmcp::model::{CallToolResult, Content};

pub(crate) fn error_response(message: impl Into<String>) -> CallToolResult {
    let msg = message.into();
    let trimmed = msg.trim();
    let sanitized = trimmed
        .strip_suffix(':')
        .or_else(|| trimmed.strip_suffix(','))
        .unwrap_or(trimmed);
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

    fn extract_text(result: &CallToolResult) -> &str {
        result.content[0].raw.as_text().expect("expected text content").text.as_str()
    }

    #[test]
    fn test_error_response_trims_whitespace() {
        let result = error_response("  Connection failed  ");
        assert_eq!(extract_text(&result), "Connection failed");
    }

    #[test]
    fn test_error_response_removes_trailing_colon() {
        let result = error_response("Connection failed:");
        assert_eq!(extract_text(&result), "Connection failed");
    }

    #[test]
    fn test_error_response_removes_trailing_comma() {
        let result = error_response("Connection failed,");
        assert_eq!(extract_text(&result), "Connection failed");
    }

    #[test]
    fn test_error_response_preserves_connection_details() {
        // IPs and paths are no longer redacted - the user already knows these
        let result = error_response("Connection to 192.168.1.1:3306 failed");
        assert_eq!(extract_text(&result), "Connection to 192.168.1.1:3306 failed");
    }

    #[test]
    fn test_error_response_preserves_paths() {
        let result = error_response("Error reading /home/user/config.toml: permission denied");
        // Trailing colon removed, but path preserved
        assert_eq!(
            extract_text(&result),
            "Error reading /home/user/config.toml: permission denied"
        );
    }

    #[test]
    fn test_error_response_preserves_os_errors() {
        let result = error_response("Connection refused (os error 111)");
        assert_eq!(extract_text(&result), "Connection refused (os error 111)");
    }

    #[test]
    fn test_error_response_preserves_safe_content() {
        let result = error_response("Table users not found in database");
        assert_eq!(extract_text(&result), "Table users not found in database");
    }
}
