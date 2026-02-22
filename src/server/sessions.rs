use std::sync::Arc;
use std::collections::HashMap;
use rmcp::model::{CallToolResult, Content};
use serde_json::json;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::db::DbPool;
use crate::schema::SchemaIntrospector;
use super::tool_schemas::serialize_response;

/// A named database session (non-default, runtime-created connection).
pub(crate) struct Session {
    pub(crate) pool: sqlx::MySqlPool,
    pub(crate) introspector: Arc<SchemaIntrospector>,
    pub(crate) last_used: std::time::Instant,
    /// Human-readable display info for mysql_list_sessions
    pub(crate) host: String,
    pub(crate) database: Option<String>,
}

/// Named context returned by get_session(): pool, schema introspector, and optional database.
pub(crate) struct SessionContext {
    pub(crate) pool: sqlx::MySqlPool,
    pub(crate) schema: Arc<SchemaIntrospector>,
    pub(crate) database: Option<String>,
}

/// Holds the named sessions map and the default connection references.
/// Methods on this type implement the session-related MCP tools and helpers.
pub(crate) struct SessionStore {
    pub(crate) sessions: Arc<Mutex<HashMap<String, Session>>>,
    pub(crate) config: Arc<Config>,
    pub(crate) db: Arc<DbPool>,
    pub(crate) introspector: Arc<SchemaIntrospector>,
}

impl SessionStore {
    /// Resolve a session name to a SessionContext (pool, introspector, database).
    /// Updates last_used on non-default sessions.
    /// Returns Err with a user-friendly message if not found.
    pub(crate) async fn get_session(
        &self,
        name: &str,
    ) -> std::result::Result<SessionContext, String> {
        if name == "default" || name.is_empty() {
            return Ok(SessionContext {
                pool: self.db.pool().clone(),
                schema: self.introspector.clone(),
                database: self.config.connection.database.clone(),
            });
        }
        let mut map = self.sessions.lock().await;
        match map.get_mut(name) {
            Some(session) => {
                session.last_used = std::time::Instant::now();
                Ok(SessionContext {
                    pool: session.pool.clone(),
                    schema: session.introspector.clone(),
                    database: session.database.clone(),
                })
            }
            None => Err(format!(
                "Session '{}' not found. Use mysql_connect to create it, or omit 'session' to use the default connection.",
                name
            )),
        }
    }

    /// Resolve the "session" key from the args map, returning a SessionContext or
    /// an Err(CallToolResult) that callers can propagate immediately with `?`.
    pub(crate) async fn resolve_session(
        &self,
        args: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<SessionContext, CallToolResult> {
        let session_name = args
            .get("session")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        self.get_session(session_name).await.map_err(|e| {
            CallToolResult::error(vec![Content::text(e)])
        })
    }

    // ------------------------------------------------------------------
    // Tool handler: mysql_connect
    // ------------------------------------------------------------------
    pub(crate) async fn handle_connect(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let name = match args.get("name").and_then(|v| v.as_str()) {
            Some(n) if !n.is_empty() => n.to_string(),
            _ => return Ok(CallToolResult::error(vec![Content::text("Missing required argument: name")])),
        };
        if name == "default" {
            return Ok(CallToolResult::error(vec![Content::text("Session name 'default' is reserved")]));
        }

        // Validate session name: max 64 chars, alphanumeric + underscore + hyphen only
        if name.len() > 64 {
            return Ok(CallToolResult::error(vec![Content::text(
                "Session name too long (max 64 characters)".to_string()
            )]));
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
            return Ok(CallToolResult::error(vec![Content::text(
                "Session name must contain only alphanumeric characters, underscores, or hyphens".to_string()
            )]));
        }

        if !self.config.security.allow_runtime_connections {
            return Ok(CallToolResult::error(vec![Content::text(
                "Runtime connections are disabled. Set MYSQL_ALLOW_RUNTIME_CONNECTIONS=true to enable mysql_connect with raw credentials."
            )]));
        }

        let host = match args.get("host").and_then(|v| v.as_str()) {
            Some(h) => h.to_string(),
            None => return Ok(CallToolResult::error(vec![Content::text("Missing required argument: host")])),
        };
        if super::is_private_host(&host) {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Connecting to loopback/link-local IP addresses is not allowed: {}", host
            ))]));
        }
        let port = args.get("port").and_then(|v| v.as_u64()).unwrap_or(3306) as u16;
        let user = match args.get("user").and_then(|v| v.as_str()) {
            Some(u) => u.to_string(),
            None => return Ok(CallToolResult::error(vec![Content::text("Missing required argument: user")])),
        };
        let password = args.get("password").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let database = args.get("database").and_then(|v| v.as_str()).map(|s| s.to_string());
        if let Some(ref db) = database {
            if db.len() > 64 {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Database name too long (max 64 characters)".to_string()
                )]));
            }
            if !db.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Database name must contain only alphanumeric characters, underscores, or hyphens".to_string()
                )]));
            }
        }
        let ssl = args.get("ssl").and_then(|v| v.as_bool()).unwrap_or(false);
        let ssl_ca = args.get("ssl_ca").and_then(|v| v.as_str()).map(|s| s.to_string());

        {
            let sessions = self.sessions.lock().await;
            if sessions.len() >= self.config.security.max_sessions as usize {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "Maximum session limit ({}) reached. Disconnect an existing session first.",
                    self.config.security.max_sessions
                ))]));
            }
        }

        tracing::debug!(
            name = %name,
            host = %host,
            port = port,
            user = "<redacted>",
            database = ?database,
            "mysql_connect: creating session"
        );

        match crate::db::build_session_pool(
            &host, port, &user, &password,
            database.as_deref(), ssl, false, ssl_ca.as_deref(),
            self.config.pool.connect_timeout_ms,
        ).await {
            Ok(pool) => {
                let pool_arc = Arc::new(pool.clone());
                let introspector = Arc::new(SchemaIntrospector::new(pool_arc, self.config.pool.cache_ttl_secs));
                let session = Session {
                    pool,
                    introspector,
                    last_used: std::time::Instant::now(),
                    host: host.clone(),
                    database: database.clone(),
                };
                let mut sessions = self.sessions.lock().await;
                sessions.insert(name.clone(), session);
                let info = json!({
                    "connected": true,
                    "session": name,
                    "host": host,
                    "database": database,
                });
                Ok(serialize_response(&info))
            }
            Err(e) => {
                Ok(CallToolResult::error(vec![
                    Content::text(format!("Connection failed: {}", e)),
                ]))
            }
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: mysql_disconnect
    // ------------------------------------------------------------------
    pub(crate) async fn handle_disconnect(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let name = match args.get("name").and_then(|v| v.as_str()) {
            Some(n) => n.to_string(),
            None => return Ok(CallToolResult::error(vec![Content::text("Missing required argument: name")])),
        };
        if name == "default" {
            return Ok(CallToolResult::error(vec![Content::text("The default session cannot be closed")]));
        }
        let mut sessions = self.sessions.lock().await;
        if sessions.remove(&name).is_some() {
            Ok(CallToolResult::success(vec![
                Content::text(format!("Session '{}' closed", name)),
            ]))
        } else {
            Ok(CallToolResult::error(vec![
                Content::text(format!("Session '{}' not found", name)),
            ]))
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: mysql_list_sessions
    // ------------------------------------------------------------------
    pub(crate) async fn handle_list_sessions(
        &self,
        _args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let sessions = self.sessions.lock().await;
        let mut list: Vec<serde_json::Value> = vec![];

        // Include default session if there are other sessions too
        if !sessions.is_empty() {
            list.push(json!({
                "name": "default",
                "host": self.config.connection.host,
                "database": self.config.connection.database,
                "idle_seconds": null,
            }));
        }
        for (name, session) in sessions.iter() {
            list.push(json!({
                "name": name,
                "host": session.host,
                "database": session.database,
                "idle_seconds": session.last_used.elapsed().as_secs(),
            }));
        }

        if list.is_empty() {
            // Only the default session exists — show it alone
            list.push(json!({
                "name": "default",
                "host": self.config.connection.host,
                "database": self.config.connection.database,
                "idle_seconds": null,
            }));
        }

        Ok(serialize_response(&json!({"sessions": list})))
    }
}
