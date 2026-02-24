use rmcp::model::Content;
use serde_json::json;
use std::sync::Arc;

// ============================================================
// Tool input schemas
// Each function builds and returns the JSON schema for its tool.
// ============================================================

pub(crate) fn mysql_query_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SQL query to execute"
            },
            "explain": {
                "type": "boolean",
                "description": "Set to true when investigating a slow query (default: false) — returns full execution plan including index usage, rows examined, and optimization suggestions. Overrides the server performance_hints setting for this call."
            },
            "session": {
                "type": "string",
                "description": "Named session to route this query to (omit for default connection). Use 'default' or a session name from mysql_connect."
            }
        },
        "required": ["sql"]
    })))
}

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

pub(crate) fn mysql_server_info_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "session": { "type": "string", "description": "Named session to use (omit for default connection). Use 'default' or a session name from mysql_connect." }
        },
        "required": []
    })))
}

pub(crate) fn mysql_connect_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string", "description": "Session identifier (alphanumeric, underscore, hyphen; max 64 chars). 'default' is reserved. Named sessions use a dedicated pool of up to 5 connections (vs. the default session's configured pool size). Use this name as the 'session' parameter in other tools." },
            "host": { "type": "string", "description": "MySQL host (required unless using preset)" },
            "port": { "type": "integer", "description": "MySQL port (default: 3306).", "minimum": 1, "maximum": 65535 },
            "user": { "type": "string", "description": "MySQL username" },
            "password": { "type": "string", "description": "MySQL password (optional; use empty string for passwordless login)." },
            "database": { "type": "string", "description": "Default database for this session (optional; passed via connection string)." },
            "ssl": { "type": "boolean", "description": "Enable SSL/TLS (default: false). When true and ssl_ca is omitted, uses VerifyIdentity mode (full cert+hostname check)." },
            "ssl_ca": { "type": "string", "description": "Path to PEM CA certificate file for SSL verification. When set, uses VerifyCa mode (validates cert chain without hostname check)." },
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

pub(crate) fn mysql_disconnect_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string", "description": "Session name to disconnect. Use the name that was provided to mysql_connect." }
        },
        "required": ["name"]
    })))
}

pub(crate) fn mysql_list_sessions_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {},
        "required": []
    })))
}

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
