use rmcp::model::CallToolResult;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use super::tool_schemas::serialize_response;
use super::validate_host_with_dns;
use crate::backend::{Backend, PoolHandle, SessionConnectParams};
use crate::config::Config;
use crate::schema::SchemaIntrospector;
use crate::tool_error;

/// Pool size for named sessions (hardcoded for resource predictability)
pub(crate) const NAMED_SESSION_POOL_SIZE: u32 = 5;

/// A named database session (non-default, runtime-created connection).
pub(crate) struct Session {
    pub(crate) pool: PoolHandle,
    pub(crate) introspector: Arc<SchemaIntrospector>,
    pub(crate) last_used: std::time::Instant,
    /// Human-readable display info for list_sessions
    pub(crate) host: String,
    pub(crate) database: Option<String>,
    /// SSH tunnel keeping the connection alive (None for direct connections).
    pub(crate) tunnel: Option<crate::tunnel::TunnelHandle>,
    /// Bastion hostname shown in list_sessions when tunneling.
    pub(crate) ssh_host: Option<String>,
}

/// Named context returned by get_session(): pool, schema introspector, and optional database.
pub(crate) struct SessionContext {
    pub(crate) pool: PoolHandle,
    pub(crate) schema: Arc<SchemaIntrospector>,
    pub(crate) database: Option<String>,
}

/// Holds the named sessions map and the default connection references.
/// Methods on this type implement the session-related MCP tools and helpers.
pub(crate) struct SessionStore {
    pub(crate) sessions: Arc<Mutex<HashMap<String, Session>>>,
    pub(crate) config: Arc<Config>,
    pub(crate) db: PoolHandle,
    pub(crate) introspector: Arc<SchemaIntrospector>,
    pub(crate) backend: Arc<dyn Backend>,
    /// Total connections across all sessions (for max_total_connections enforcement)
    pub(crate) total_connections: Arc<AtomicU32>,
}

/// Validate a MySQL identifier (session name or database name): max 64 chars,
/// alphanumeric/underscore only. Returns `Err(CallToolResult)` on failure.
pub(crate) fn validate_identifier(value: &str, kind: &str) -> Result<(), CallToolResult> {
    if value.is_empty() {
        return Err(crate::server::error::error_response(format!(
            "{} cannot be empty",
            kind
        )));
    }
    if value.len() > 64 {
        return Err(crate::server::error::error_response(format!(
            "{} too long (max 64 characters)",
            kind
        )));
    }
    if !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(crate::server::error::error_response(format!(
            "{} must contain only alphanumeric characters or underscores",
            kind
        )));
    }
    Ok(())
}

/// Timeout for SSH tunnel close operations. A hung SSH server should not block cleanup.
const TUNNEL_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Close an SSH tunnel with a timeout. Logs a warning on error or timeout, never blocks
/// cleanup indefinitely.
pub(crate) async fn close_tunnel_with_timeout(tunnel: crate::tunnel::TunnelHandle, context: &str) {
    match tokio::time::timeout(TUNNEL_CLOSE_TIMEOUT, tunnel.close()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!("SSH tunnel close error {}: {}", context, e);
        }
        Err(_) => {
            tracing::warn!(
                "SSH tunnel close timed out after {}s {}",
                TUNNEL_CLOSE_TIMEOUT.as_secs(),
                context
            );
        }
    }
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
                pool: self.db.clone(),
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
                Ok(SessionContext {
                    pool,
                    schema,
                    database,
                })
            }
            None => {
                let msg = format!(
                    "Session '{}' not found. Use connect to create it, or omit 'session' to use the default connection.",
                    name
                );
                drop(map);
                Err(crate::server::error::error_response(msg))
            }
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: connect
    // ------------------------------------------------------------------
    pub(crate) async fn handle_connect(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let name = match args.get("name").and_then(|v| v.as_str()) {
            Some(n) if !n.is_empty() => n.to_string(),
            _ => return tool_error!("Missing required argument: name"),
        };
        if name == "default" {
            return tool_error!("Session name 'default' is reserved");
        }

        // Validate session name: max 64 chars, alphanumeric + underscore only
        if let Err(e) = validate_identifier(&name, "Session name") {
            return Ok(e);
        }

        if !self.config.security.allow_runtime_connections {
            return tool_error!(
                "Runtime connections are disabled. Set DB_ALLOW_RUNTIME_CONNECTIONS=true to enable connect with raw credentials."
            );
        }

        let host = match args.get("host").and_then(|v| v.as_str()) {
            Some(h) if !h.is_empty() => {
                if h.len() > 255 {
                    return tool_error!("Host too long (max 255 characters)");
                }
                h.to_string()
            }
            Some(_) => return tool_error!("Host cannot be empty"),
            None => return tool_error!("Missing required argument: host"),
        };

        // Validate host with DNS resolution
        let host_validation = validate_host_with_dns(&host).await;
        if !host_validation.allowed {
            return tool_error!(
                "Host validation failed: {}",
                host_validation
                    .reason
                    .unwrap_or_else(|| "unknown reason".to_string())
            );
        }
        let port = match args.get("port").and_then(|v| v.as_u64()) {
            Some(p) if (1..=65535).contains(&p) => p as u16,
            Some(_) => return tool_error!("Port out of range (1-65535)"),
            None => 3306,
        };
        let user = match args.get("user").and_then(|v| v.as_str()) {
            Some("") | None => return tool_error!("Missing required argument: user"),
            Some(u) if u.len() > 255 => return tool_error!("User too long (max 255 characters)"),
            Some(u) => u.to_string(),
        };
        let password = match args.get("password").and_then(|v| v.as_str()) {
            Some(p) if p.len() > 2048 => {
                return tool_error!("Password too long (max 2048 characters)")
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
            if !ssl {
                return tool_error!(
                    "Contradictory SSL config: ssl_ca was provided but ssl=false. \
                     Set ssl=true to use certificate validation, or remove ssl_ca."
                );
            }
            if let Err(e) = std::fs::File::open(ca_path) {
                return tool_error!("SSL CA file not readable: {}: {}", ca_path, e);
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
                return tool_error!("SSH host too long (max 255 characters)");
            }
            let ssh_host_validation = validate_host_with_dns(ssh_h).await;
            if !ssh_host_validation.allowed {
                return tool_error!(
                    "SSH bastion host validation failed: {}",
                    ssh_host_validation
                        .reason
                        .unwrap_or_else(|| "unknown reason".to_string())
                );
            }
        }
        let ssh_port = match args.get("ssh_port").and_then(|v| v.as_u64()) {
            Some(p) if (1..=65535).contains(&p) => p as u16,
            Some(_) => return tool_error!("ssh_port out of range (1-65535)"),
            None => 22,
        };
        let ssh_user = match args
            .get("ssh_user")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            Some(u) if u.len() > 255 => {
                return tool_error!("SSH user too long (max 255 characters)")
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
            return tool_error!(
                "ssh_known_hosts_check must be one of: strict, accept-new, insecure"
            );
        }
        if let Some(ref key_path) = ssh_private_key {
            if !std::path::Path::new(key_path).exists() {
                return tool_error!("SSH private key file not found: {}", key_path);
            }
        }
        if let Some(ref khf) = ssh_known_hosts_file {
            if !std::path::Path::new(khf).exists() {
                return tool_error!("SSH known_hosts file not found: {}", khf);
            }
        }

        tracing::debug!(
            name = %name,
            host = %host,
            port = port,
            user = "<redacted>",
            database = ?database,
            "connect: creating session"
        );

        // Pre-check: fast path before the expensive pool/tunnel creation.
        {
            let sessions = self.sessions.lock().await;
            if sessions.len() >= self.config.security.max_sessions as usize {
                return tool_error!(
                    "Maximum session limit ({}) reached. Disconnect an existing session first.",
                    self.config.security.max_sessions
                );
            }
            if sessions.contains_key(&name) {
                return tool_error!(
                    "Session '{}' already exists. Use disconnect to close it first, or choose a different name.",
                    name
                );
            }
        }

        // Atomically check and reserve connection slots
        let max_total = self.config.security.max_total_connections;
        let reserve_result =
            self.total_connections
                .fetch_update(Ordering::Release, Ordering::Relaxed, |current| {
                    if current + NAMED_SESSION_POOL_SIZE <= max_total {
                        Some(current + NAMED_SESSION_POOL_SIZE)
                    } else {
                        None
                    }
                });
        if let Err(current_total) = reserve_result {
            return tool_error!(
                "Total connection limit ({}) would be exceeded. Current: {}, new session would add {}. Disconnect a session first.",
                max_total, current_total, NAMED_SESSION_POOL_SIZE
            );
        }

        let params = SessionConnectParams {
            host: host.clone(),
            port,
            user: user.clone(),
            password: password.clone(),
            database: database.clone(),
            ssl,
            ssl_accept_invalid_certs: self.config.security.ssl_accept_invalid_certs,
            ssl_ca: ssl_ca.clone(),
            connect_timeout_ms: self.config.pool.connect_timeout_ms,
        };

        let (pool, tunnel) = if let Some(ref ssh_host_str) = ssh_host {
            let ssh_user_str = match ssh_user {
                Some(ref u) => u.clone(),
                None => return tool_error!("ssh_user is required when ssh_host is provided"),
            };
            let ssh_config = crate::config::SshConfig {
                host: ssh_host_str.clone(),
                port: ssh_port,
                user: ssh_user_str,
                private_key: ssh_private_key.clone(),
                known_hosts_check: ssh_known_hosts_check.clone(),
                known_hosts_file: ssh_known_hosts_file,
            };

            // Spawn SSH tunnel then use the backend to create a session pool through it
            let tunnel_result = crate::tunnel::spawn_ssh_tunnel(&ssh_config, &host, port).await;
            let tunnel = match tunnel_result {
                Ok(t) => t,
                Err(e) => {
                    self.total_connections
                        .fetch_sub(NAMED_SESSION_POOL_SIZE, Ordering::Release);
                    return tool_error!("SSH tunnel failed: {}", e);
                }
            };

            let tunnel_params = SessionConnectParams {
                host: "127.0.0.1".to_string(),
                port: tunnel.local_port,
                user: user.clone(),
                password: password.clone(),
                database: database.clone(),
                ssl,
                ssl_accept_invalid_certs: self.config.security.ssl_accept_invalid_certs,
                ssl_ca: ssl_ca.clone(),
                connect_timeout_ms: self.config.pool.connect_timeout_ms,
            };

            match self.backend.create_session_pool(&tunnel_params).await {
                Ok(p) => (p, Some(tunnel)),
                Err(e) => {
                    close_tunnel_with_timeout(tunnel, "on pool creation failure").await;
                    self.total_connections
                        .fetch_sub(NAMED_SESSION_POOL_SIZE, Ordering::Release);
                    return tool_error!("Connection through SSH tunnel failed: {}", e);
                }
            }
        } else {
            match self.backend.create_session_pool(&params).await {
                Ok(p) => (p, None),
                Err(e) => {
                    self.total_connections
                        .fetch_sub(NAMED_SESSION_POOL_SIZE, Ordering::Release);
                    return tool_error!("Connection failed: {}", e);
                }
            }
        };

        let introspector = Arc::new(SchemaIntrospector::new_with_backend(
            pool.clone(),
            self.backend.clone(),
            self.config.pool.cache_ttl_secs,
        ));
        let info = json!({
            "connected": true,
            "session": &name,
            "host": &host,
            "database": &database,
            "ssh_host": &ssh_host,
        });
        {
            let sessions = self.sessions.lock().await;
            if sessions.len() >= self.config.security.max_sessions as usize {
                drop(sessions);
                if let Some(t) = tunnel {
                    close_tunnel_with_timeout(t, "on session limit rejection").await;
                }
                pool.close().await;
                self.total_connections
                    .fetch_sub(NAMED_SESSION_POOL_SIZE, Ordering::Release);
                return tool_error!(
                    "Maximum session limit ({}) reached. Disconnect an existing session first.",
                    self.config.security.max_sessions
                );
            }
            if sessions.contains_key(&name) {
                drop(sessions);
                if let Some(t) = tunnel {
                    close_tunnel_with_timeout(t, "on duplicate session rejection").await;
                }
                pool.close().await;
                self.total_connections
                    .fetch_sub(NAMED_SESSION_POOL_SIZE, Ordering::Release);
                return tool_error!(
                    "Session '{}' already exists. Use disconnect to close it first, or choose a different name.",
                    name
                );
            }
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
        let mut sessions = self.sessions.lock().await;
        if sessions.len() >= self.config.security.max_sessions as usize
            || sessions.contains_key(&name)
        {
            drop(sessions);
            if let Some(t) = session.tunnel {
                close_tunnel_with_timeout(t, "on post-race cleanup").await;
            }
            session.pool.close().await;
            self.total_connections
                .fetch_sub(NAMED_SESSION_POOL_SIZE, Ordering::Release);
            return tool_error!(
                "Session name '{}' is now taken. Please try a different name.",
                name
            );
        }
        sessions.insert(name, session);

        let mut response = info;
        let warnings = self.config.security.security_warnings();
        if !warnings.is_empty() {
            response["security_warnings"] = json!(warnings);
        }
        Ok(serialize_response(&response))
    }

    // ------------------------------------------------------------------
    // Tool handler: disconnect
    // ------------------------------------------------------------------
    pub(crate) async fn handle_disconnect(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
            return tool_error!("Missing required argument: name");
        };
        if name.trim().is_empty() {
            return tool_error!("Session name cannot be empty");
        }
        if name == "default" {
            return tool_error!("The default session cannot be closed");
        }
        let removed = {
            let mut sessions = self.sessions.lock().await;
            sessions.remove(name)
        };
        if let Some(session) = removed {
            self.total_connections
                .fetch_sub(NAMED_SESSION_POOL_SIZE, Ordering::Release);
            if let Some(tunnel) = session.tunnel {
                close_tunnel_with_timeout(tunnel, "on disconnect").await;
            }
            session.pool.close().await;
            Ok(serialize_response(&json!({
                "success": true,
                "message": format!("Session '{}' closed", name)
            })))
        } else {
            tool_error!(
                "Session '{}' not found. Use list_sessions to see available sessions.",
                name
            )
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: list_sessions
    // ------------------------------------------------------------------
    pub(crate) async fn handle_list_sessions(
        &self,
        _args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let sessions = self.sessions.lock().await;
        let mut list: Vec<serde_json::Value> = vec![];

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier_empty_returns_error() {
        let result = validate_identifier("", "Identifier");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let text = err.content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("Identifier cannot be empty"));
    }

    #[test]
    fn test_validate_identifier_too_long_returns_error() {
        let long_id = "a".repeat(65);
        let result = validate_identifier(&long_id, "Identifier");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let text = err.content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("Identifier too long"));
    }

    #[test]
    fn test_validate_identifier_invalid_characters_returns_error() {
        let result = validate_identifier("invalid name", "Identifier");
        assert!(result.is_err());

        let result = validate_identifier("invalid.name", "Identifier");
        assert!(result.is_err());

        let result = validate_identifier("invalid;name", "Identifier");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_identifier_valid_alphanumeric_and_underscores() {
        let result = validate_identifier("valid_name_123", "Identifier");
        assert!(result.is_ok());

        let result = validate_identifier("SimpleName", "Identifier");
        assert!(result.is_ok());

        let result = validate_identifier("ALLCAPS_123", "Identifier");
        assert!(result.is_ok());

        let result = validate_identifier("_underscore_prefix", "Identifier");
        assert!(result.is_ok());

        let result = validate_identifier("underscore_suffix_", "Identifier");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_identifier_rejects_hyphens() {
        let result = validate_identifier("valid-name-123", "Identifier");
        assert!(result.is_err());

        let result = validate_identifier("my-session", "Identifier");
        assert!(result.is_err());

        let result = validate_identifier("test-db-name", "Identifier");
        assert!(result.is_err());

        let result = validate_identifier("mixed_name-with-hyphens", "Identifier");
        assert!(result.is_err());
    }
}
