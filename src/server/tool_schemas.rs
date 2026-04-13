//! JSON schemas for MCP tools and response serialization.
//!
//! This module defines the input JSON schemas for all MySQL MCP tools, along with
//! utilities for serializing responses. These schemas define the structure and
//! validation rules for tool parameters that clients (like AI assistants) provide
//! when invoking MCP tools.
//!
//! ## Adding a New Tool Schema
//!
//! To add a new MCP tool with a custom input schema:
//!
//! 1. Define a new schema function in this module that returns
//!    `Arc<serde_json::Map<String, serde_json::Value>>`
//! 2. Use `rmcp::model::object()` to create the schema object
//! 3. Define the JSON schema with `type`, `properties`, `required`, etc.
//! 4. Register the tool in [`crate::server::ServerHandler::list_tools`]
//! 5. Implement the handler in [`handlers`](super::handlers) or [`sessions`](super::sessions)
//!
//! ### Example Schema Function
//!
//! ```rust,ignore
//! pub(crate) fn my_new_tool_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
//!     Arc::new(rmcp::model::object(json!({
//!         "type": "object",
//!         "properties": {
//!             "param_name": {
//!                 "type": "string",
//!                 "description": "Description of the parameter"
//!             },
//!             "optional_param": {
//!                 "type": "integer",
//!                 "description": "An optional parameter",
//!                 "minimum": 1
//!             }
//!         },
//!         "required": ["param_name"]
//!     })))
//! }
//! ```
//!
//! ## Response Serialization
//!
//! Use [`serialize_response`] to convert a [`serde_json::Value`] into a
//! [`CallToolResult`](rmcp::model::CallToolResult) with pretty-printed JSON text.
//! This ensures consistent formatting across all tool responses.

use rmcp::model::Content;
use serde_json::json;
use std::sync::Arc;

/// Returns the JSON input schema for the `mysql_query` tool.
///
/// This tool executes SQL queries and supports an optional `session` parameter
/// for routing queries to named connections, plus an `explain` flag for
/// performance analysis.
///
/// ## Environment Variable Mapping
///
/// The default behavior can be configured via environment variables:
/// - `MYSQL_PERFORMANCE_HINTS` - Sets the default for the `explain` parameter
///   (values: "none", "auto", "always")
pub(crate) fn mysql_query_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SQL query to execute"
            },
            "session": {
                "type": "string",
                "description": "Named session to route this query to (omit for default connection). Use 'default' or a session name from mysql_connect."
            },
            "explain": {
                "type": "boolean",
                "description": "Force an EXPLAIN run for this query, overriding the performance_hints setting"
            }
        },
        "required": ["sql"]
    })))
}

/// Returns the JSON input schema for the `mysql_schema_info` tool.
///
/// This tool retrieves schema metadata for a table, including columns,
/// indexes, foreign keys, and size information based on the `include` parameter.
pub(crate) fn mysql_schema_info_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "table": { "type": "string", "description": "Table name" },
            "database": { "type": "string", "description": "Database/schema name (optional, uses connected database if omitted)" },
            "include": {
                "type": "array",
                "items": { "type": "string", "enum": ["indexes", "foreign_keys", "size"] },
                "description": "Additional metadata to include (array, optional). Default: just columns. Options: 'indexes' (all indexes with columns), 'foreign_keys' (FK constraints), 'size' (estimated row count and byte sizes). Combine any subset, e.g. [\"indexes\", \"foreign_keys\"] for full detail."
            },
            "session": {
                "type": "string",
                "description": "Named session to use (omit for default connection). Use 'default' or a session name from mysql_connect."
            }
        },
        "required": ["table"]
    })))
}

/// Returns the JSON input schema for the `mysql_server_info` tool.
///
/// This tool retrieves server metadata such as version, configuration,
/// and current status for the specified session.
pub(crate) fn mysql_server_info_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "session": { "type": "string", "description": "Named session to use (omit for default connection). Use 'default' or a session name from mysql_connect." }
        },
        "required": []
    })))
}

/// Returns the JSON input schema for the `mysql_connect` tool.
///
/// This tool creates a named session with MySQL connection parameters.
/// Supports direct connections as well as SSH tunneling options.
///
/// ## Environment Variable Mapping
///
/// The following tool parameters correspond to environment variables that
/// configure default connection behavior. When a parameter is omitted from
/// the tool call, the server uses the value from the corresponding env var:
///
/// | Tool Parameter | Environment Variable | Description |
/// |----------------|---------------------|-------------|
/// | `host` | `MYSQL_HOST` | MySQL server hostname |
/// | `port` | `MYSQL_PORT` | MySQL server port (default: 3306) |
/// | `user` | `MYSQL_USER` | MySQL username |
/// | `password` | `MYSQL_PASS` | MySQL password |
/// | `database` | `MYSQL_DB` | Default database name |
/// | `ssl` | `MYSQL_SSL` | Enable SSL/TLS (default: false) |
/// | `ssl_ca` | `MYSQL_SSL_CA` | Path to CA certificate for SSL |
/// | `ssh_host` | `MYSQL_SSH_HOST` | SSH bastion hostname |
/// | `ssh_port` | `MYSQL_SSH_PORT` | SSH server port (default: 22) |
/// | `ssh_user` | `MYSQL_SSH_USER` | SSH username |
/// | `ssh_private_key` | `MYSQL_SSH_PRIVATE_KEY` | Path to SSH private key |
/// | `ssh_known_hosts_check` | `MYSQL_SSH_KNOWN_HOSTS_CHECK` | Host key verification mode |
/// | `ssh_known_hosts_file` | `MYSQL_SSH_KNOWN_HOSTS_FILE` | Path to known_hosts file |
///
/// Additionally, `MYSQL_ALLOW_RUNTIME_CONNECTIONS` controls whether the
/// `mysql_connect` tool can be used at all (default: false).
pub(crate) fn mysql_connect_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string", "description": "Session identifier (alphanumeric and underscore only; max 64 chars). 'default' is reserved. Named sessions use a dedicated pool of up to 5 connections (vs. the default session's configured pool size). Use this name as the 'session' parameter in other tools." },
            "host": { "type": "string", "description": "MySQL host (required unless using preset)" },
            "port": { "type": "integer", "description": "MySQL port (default: 3306).", "minimum": 1, "maximum": 65535 },
            "user": { "type": "string", "description": "MySQL username" },
            "password": { "type": "string", "description": "MySQL password (optional; use empty string for passwordless login)." },
            "database": { "type": "string", "description": "Default database for this session (optional; passed via connection string)." },
            "ssl": { "type": "boolean", "description": "Enable SSL/TLS (default: false). When enabled: ssl=true alone uses VerifyIdentity mode (full certificate and hostname verification against system trust store); ssl=true + ssl_ca uses VerifyCa mode (validates certificate chain against the provided CA file without hostname verification)." },
            "ssl_ca": { "type": "string", "description": "Path to PEM CA certificate file for SSL verification. When set alongside ssl=true, uses VerifyCa mode (validates certificate chain against this CA without hostname verification). When omitted with ssl=true, uses system trust store with full hostname verification (VerifyIdentity mode)." },
            "ssh_host": {
                "type": "string",
                "description": "SSH bastion hostname. When provided, the connection is made through an SSH tunnel via this host."
            },
            "ssh_port": {
                "type": "integer",
                "description": "SSH server port (default: 22).",
                "minimum": 1,
                "maximum": 65535,
                "default": 22
            },
            "ssh_user": {
                "type": "string",
                "description": "SSH username (required when ssh_host is set)."
            },
            "ssh_private_key": {
                "type": "string",
                "description": "Path to the SSH private key file (PEM format). If omitted, relies on SSH agent."
            },
            "ssh_known_hosts_check": {
                "type": "string",
                "enum": ["strict", "accept-new", "insecure"],
                "description": "Host key verification mode. 'strict' (default): fail on unknown host. 'accept-new': auto-add new hosts. 'insecure': skip all verification.",
                "default": "strict"
            },
            "ssh_known_hosts_file": {
                "type": "string",
                "description": "Path to a custom known_hosts file for SSH host key verification. If omitted, uses ~/.ssh/known_hosts."
            }
        },
        "required": ["name", "host", "user"]
    })))
}

/// Returns the JSON input schema for the `mysql_disconnect` tool.
///
/// This tool closes a named session created via `mysql_connect`.
pub(crate) fn mysql_disconnect_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string", "description": "Session name to disconnect. Use the name that was provided to mysql_connect." }
        },
        "required": ["name"]
    })))
}

/// Returns the JSON input schema for the `mysql_list_sessions` tool.
///
/// This tool returns a list of all active sessions, including both the
/// default session and any named sessions created via `mysql_connect`.
pub(crate) fn mysql_list_sessions_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {},
        "required": []
    })))
}

/// Returns the JSON input schema for the `mysql_explain_plan` tool.
///
/// This tool generates an execution plan for a SELECT statement,
/// helping analyze query performance before execution.
pub(crate) fn mysql_explain_plan_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SELECT statement to explain. Must be a single SELECT (not INSERT/UPDATE/DDL)."
            },
            "session": {
                "type": "string",
                "description": "Named session to use (default: 'default'). Use 'default' or a session name from mysql_connect."
            }
        },
        "required": ["sql"]
    })))
}

/// Returns the JSON input schema for the `mysql_list_tables` tool.
///
/// This tool lists all tables in a database. If no database is specified,
/// it uses the current database for the session.
pub(crate) fn mysql_list_tables_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "database": {
                "type": "string",
                "description": "Database name to list tables from (optional, defaults to current database)"
            },
            "session": {
                "type": "string",
                "description": "Named session to use (omit for default connection)"
            }
        },
        "required": []
    })))
}

/// Serialize a JSON value to pretty-printed text and wrap in a successful CallToolResult.
/// Returns a CallToolResult::error on serialization failure.
pub(crate) fn serialize_response(value: &serde_json::Value) -> rmcp::model::CallToolResult {
    match serde_json::to_string_pretty(value) {
        Ok(s) => rmcp::model::CallToolResult::success(vec![Content::text(s)]),
        Err(e) => {
            tracing::error!("Failed to serialize response: {}", e);
            rmcp::model::CallToolResult::error(vec![Content::text(format!(
                "Internal error: failed to serialize response: {}",
                e
            ))])
        }
    }
}
