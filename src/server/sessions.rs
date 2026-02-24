use rmcp::model::{CallToolResult, Content};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::tool_schemas::serialize_response;
use crate::config::Config;
use crate::schema::SchemaIntrospector;

/// A named database session (non-default, runtime-created connection).
pub(crate) struct Session {
    pub(crate) pool: sqlx::MySqlPool,
    pub(crate) introspector: Arc<SchemaIntrospector>,
    pub(crate) last_used: std::time::Instant,
    /// Human-readable display info for mysql_list_sessions
    pub(crate) host: String,
    pub(crate) database: Option<String>,
    /// SSH tunnel keeping the connection alive (None for direct connections).
    pub(crate) tunnel: Option<crate::tunnel::TunnelHandle>,
    /// Bastion hostname shown in mysql_list_sessions when tunneling.
    pub(crate) ssh_host: Option<String>,
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
    pub(crate) db: Arc<sqlx::MySqlPool>,
    pub(crate) introspector: Arc<SchemaIntrospector>,
}

/// Validate a MySQL identifier (session name or database name): max 64 chars,
/// alphanumeric/underscore/hyphen only. Returns `Err(CallToolResult)` on failure.
pub(crate) fn validate_identifier(value: &str, kind: &str) -> Result<(), CallToolResult> {
    if value.is_empty() {
        return Err(CallToolResult::error(vec![Content::text(format!(
            "{} cannot be empty",
            kind
        ))]));
    }
    if value.len() > 64 {
        return Err(CallToolResult::error(vec![Content::text(format!(
            "{} too long (max 64 characters)",
            kind
        ))]));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(CallToolResult::error(vec![Content::text(format!(
            "{} must contain only alphanumeric characters, underscores, or hyphens",
            kind
        ))]));
    }
    Ok(())
}

impl SessionStore {
    /// Resolve the "session" key from the args map to a SessionContext.
    /// Updates last_used on non-default sessions.
    /// Returns Err(CallToolResult) that callers can propagate immediately with `?`.
    pub(crate) async fn resolve_session(
        &self,
        args: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<SessionContext, CallToolResult> {
        let name = args
            .get("session")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        if name == "default" || name.is_empty() {
            return Ok(SessionContext {
                pool: self.db.as_ref().clone(),
                schema: self.introspector.clone(),
                database: self.config.connection.database.clone(),
            });
        }
        let mut map = self.sessions.lock().await;
        match map.get_mut(name) {
            Some(session) => {
                session.last_used = std::time::Instant::now();
                let pool = session.pool.clone();
                let schema = session.introspector.clone();
                let database = session.database.clone();
                drop(map);
                Ok(SessionContext {
                    pool,
                    schema,
                    database,
                })
            }
            None => {
                let msg = format!(
                    "Session '{}' not found. Use mysql_connect to create it, or omit 'session' to use the default connection.",
                    name
                );
                drop(map);
                Err(CallToolResult::error(vec![Content::text(msg)]))
            }
        }
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
            _ => {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Missing required argument: name",
                )]))
            }
        };
        if name == "default" {
            return Ok(CallToolResult::error(vec![Content::text(
                "Session name 'default' is reserved",
            )]));
        }

        // Validate session name: max 64 chars, alphanumeric + underscore + hyphen only
        if let Err(e) = validate_identifier(&name, "Session name") {
            return Ok(e);
        }

        if !self.config.security.allow_runtime_connections {
            return Ok(CallToolResult::error(vec![Content::text(
                "Runtime connections are disabled. Set MYSQL_ALLOW_RUNTIME_CONNECTIONS=true to enable mysql_connect with raw credentials."
            )]));
        }

        let host = match args.get("host").and_then(|v| v.as_str()) {
            Some(h) if !h.is_empty() => {
                if h.len() > 255 {
                    return Ok(CallToolResult::error(vec![Content::text(
                        "Host too long (max 255 characters)",
                    )]));
                }
                h.to_string()
            }
            Some(_) => {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Host cannot be empty",
                )]))
            }
            None => {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Missing required argument: host",
                )]))
            }
        };
        if super::is_private_host(&host) {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Connecting to loopback/link-local IP addresses is not allowed: {}",
                host
            ))]));
        }
        let port = match args.get("port").and_then(|v| v.as_u64()) {
            Some(p) if (1..=65535).contains(&p) => p as u16,
            Some(_) => {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Port out of range (1–65535)",
                )]))
            }
            None => 3306,
        };
        let user = match args.get("user").and_then(|v| v.as_str()) {
            Some("") | None => {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Missing required argument: user",
                )]))
            }
            Some(u) if u.len() > 255 => {
                return Ok(CallToolResult::error(vec![Content::text(
                    "User too long (max 255 characters)",
                )]))
            }
            Some(u) => u.to_string(),
        };
        let password = match args.get("password").and_then(|v| v.as_str()) {
            Some(p) if p.len() > 2048 => {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Password too long (max 2048 characters)",
                )]))
            }
            Some(p) => p.to_string(),
            None => String::new(),
        };
        let database = args
            .get("database")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if let Some(ref db) = database {
            if let Err(e) = validate_identifier(db, "Database name") {
                return Ok(e);
            }
        }
        let ssl = args.get("ssl").and_then(|v| v.as_bool()).unwrap_or(false);
        let ssl_ca = args
            .get("ssl_ca")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        if let Some(ref ca_path) = ssl_ca {
            if !std::path::Path::new(ca_path).exists() {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "SSL CA file not found: {}",
                    ca_path
                ))]));
            }
        }

        // SSH tunnel parameters (optional)
        let ssh_host = args
            .get("ssh_host")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if let Some(ref ssh_h) = ssh_host {
            if ssh_h.len() > 255 {
                return Ok(CallToolResult::error(vec![Content::text(
                    "SSH host too long (max 255 characters)",
                )]));
            }
            if super::is_private_host(ssh_h) {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "SSH bastion host: connecting to loopback/link-local IP addresses is not allowed: {}",
                    ssh_h
                ))]));
            }
        }
        let ssh_port = match args.get("ssh_port").and_then(|v| v.as_u64()) {
            Some(p) if (1..=65535).contains(&p) => p as u16,
            Some(_) => {
                return Ok(CallToolResult::error(vec![Content::text(
                    "ssh_port out of range (1–65535)",
                )]))
            }
            None => 22,
        };
        let ssh_user = match args
            .get("ssh_user")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            Some(u) if u.len() > 255 => {
                return Ok(CallToolResult::error(vec![Content::text(
                    "SSH user too long (max 255 characters)",
                )]))
            }
            other => other.map(|s| s.to_string()),
        };
        let ssh_private_key = args
            .get("ssh_private_key")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let ssh_known_hosts_check = args
            .get("ssh_known_hosts_check")
            .and_then(|v| v.as_str())
            .unwrap_or("strict")
            .to_string();
        let ssh_known_hosts_file = args
            .get("ssh_known_hosts_file")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        if !matches!(
            ssh_known_hosts_check.as_str(),
            "strict" | "accept-new" | "insecure"
        ) {
            return Ok(CallToolResult::error(vec![Content::text(
                "ssh_known_hosts_check must be one of: strict, accept-new, insecure",
            )]));
        }
        if let Some(ref key_path) = ssh_private_key {
            if !std::path::Path::new(key_path).exists() {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "SSH private key file not found: {}",
                    key_path
                ))]));
            }
        }
        if let Some(ref khf) = ssh_known_hosts_file {
            if !std::path::Path::new(khf).exists() {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "SSH known_hosts file not found: {}",
                    khf
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

        // Pre-check: fast path before the expensive pool/tunnel creation.
        // We re-check both conditions after creation to handle concurrent races.
        {
            let sessions = self.sessions.lock().await;
            if sessions.len() >= self.config.security.max_sessions as usize {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "Maximum session limit ({}) reached. Disconnect an existing session first.",
                    self.config.security.max_sessions
                ))]));
            }
            if sessions.contains_key(&name) {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "Session '{}' already exists. Use mysql_disconnect to close it first, or choose a different name.",
                    name
                ))]));
            }
        }

        let (pool, tunnel) = if let Some(ref ssh_host_str) = ssh_host {
            // Validate SSH user is present
            let ssh_user_str = match ssh_user {
                Some(ref u) => u.clone(),
                None => {
                    return Ok(CallToolResult::error(vec![Content::text(
                        "ssh_user is required when ssh_host is provided",
                    )]))
                }
            };
            let ssh_config = crate::config::SshConfig {
                host: ssh_host_str.clone(),
                port: ssh_port,
                user: ssh_user_str,
                private_key: ssh_private_key.clone(),
                known_hosts_check: ssh_known_hosts_check.clone(),
                known_hosts_file: ssh_known_hosts_file,
            };
            match crate::db::build_session_pool_with_tunnel(
                &host,
                port,
                &user,
                &password,
                database.as_deref(),
                ssl,
                self.config.security.ssl_accept_invalid_certs,
                ssl_ca.as_deref(),
                self.config.pool.connect_timeout_ms,
                &ssh_config,
            )
            .await
            {
                Ok((p, t)) => (p, Some(t)),
                Err(e) => {
                    return Ok(CallToolResult::error(vec![Content::text(format!(
                        "SSH tunnel or connection failed: {}",
                        e
                    ))]))
                }
            }
        } else {
            match crate::db::build_session_pool(
                &host,
                port,
                &user,
                &password,
                database.as_deref(),
                ssl,
                self.config.security.ssl_accept_invalid_certs,
                ssl_ca.as_deref(),
                self.config.pool.connect_timeout_ms,
            )
            .await
            {
                Ok(p) => (p, None),
                Err(e) => {
                    return Ok(CallToolResult::error(vec![Content::text(format!(
                        "Connection failed: {}",
                        e
                    ))]))
                }
            }
        };

        let pool_arc = Arc::new(pool.clone());
        let introspector = Arc::new(SchemaIntrospector::new(
            pool_arc,
            self.config.pool.cache_ttl_secs,
        ));
        let info = json!({
            "connected": true,
            "session": &name,
            "host": &host,
            "database": &database,
            "ssh_host": &ssh_host,
        });
        let mut sessions = self.sessions.lock().await;
        if sessions.len() >= self.config.security.max_sessions as usize {
            if let Some(t) = tunnel {
                let _ = t.close().await;
            }
            pool.close().await;
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Maximum session limit ({}) reached. Disconnect an existing session first.",
                self.config.security.max_sessions
            ))]));
        }
        if sessions.contains_key(&name) {
            if let Some(t) = tunnel {
                let _ = t.close().await;
            }
            pool.close().await;
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Session '{}' already exists. Use mysql_disconnect to close it first, or choose a different name.",
                name
            ))]));
        }
        let session = Session {
            pool,
            introspector,
            last_used: std::time::Instant::now(),
            host,
            database,
            tunnel,
            ssh_host,
        };
        sessions.insert(name, session);
        Ok(serialize_response(&info))
    }

    // ------------------------------------------------------------------
    // Tool handler: mysql_disconnect
    // ------------------------------------------------------------------
    pub(crate) async fn handle_disconnect(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
            return Ok(CallToolResult::error(vec![Content::text(
                "Missing required argument: name",
            )]));
        };
        if name == "default" {
            return Ok(CallToolResult::error(vec![Content::text(
                "The default session cannot be closed",
            )]));
        }
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.remove(name) {
            // Clean up SSH tunnel if present
            if let Some(tunnel) = session.tunnel {
                if let Err(e) = tunnel.close().await {
                    tracing::warn!("SSH tunnel close error on disconnect: {}", e);
                }
            }
            // Explicitly close the pool so server-side connections are released
            // immediately rather than waiting for sqlx's Drop impl to handle them.
            session.pool.close().await;
            Ok(CallToolResult::success(vec![Content::text(format!(
                "Session '{}' closed",
                name
            ))]))
        } else {
            Ok(CallToolResult::error(vec![Content::text(format!(
                "Session '{}' not found",
                name
            ))]))
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

        // Default session is always shown (first when there are named sessions, alone otherwise)
        list.push(json!({
            "name": "default",
            "host": self.config.connection.host,
            "database": self.config.connection.database,
            "idle_seconds": serde_json::Value::Null,
            "ssh_host": self.config.ssh.as_ref().map(|s| &s.host),
        }));
        for (name, session) in sessions.iter() {
            list.push(json!({
                "name": name,
                "host": session.host,
                "database": session.database,
                "idle_seconds": session.last_used.elapsed().as_secs(),
                "ssh_host": session.ssh_host,
            }));
        }

        Ok(serialize_response(&json!({"sessions": list})))
    }
}
