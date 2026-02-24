use rmcp::model::{CallToolResult, Content};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::tool_schemas::serialize_response;
use super::validate_host_with_dns;
use crate::config::Config;
use crate::schema::SchemaIntrospector;
use crate::tool_error;

/// Pool size for named sessions (hardcoded for resource predictability)
pub(crate) const NAMED_SESSION_POOL_SIZE: u32 = 5;

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
    /// Count of in-flight requests on this session. The reaper skips sessions with >0.
    pub(crate) in_flight_requests: Arc<AtomicU32>,
}

/// Named context returned by get_session(): pool, schema introspector, and optional database.
pub(crate) struct SessionContext {
    pub(crate) pool: sqlx::MySqlPool,
    pub(crate) schema: Arc<SchemaIntrospector>,
    pub(crate) database: Option<String>,
}

/// Guard that tracks in-flight requests on a session.
/// Increments the counter on creation, decrements on Drop.
/// Derefs to SessionContext for transparent access.
pub(crate) struct SessionGuard {
    pub(crate) ctx: SessionContext,
    in_flight_requests: Option<Arc<AtomicU32>>,
}

impl SessionGuard {
    /// Create a guard for a named session (with in-flight tracking).
    fn new_tracked(ctx: SessionContext, in_flight_requests: Arc<AtomicU32>) -> Self {
        in_flight_requests.fetch_add(1, Ordering::Relaxed);
        Self {
            ctx,
            in_flight_requests: Some(in_flight_requests),
        }
    }

    /// Create a guard for the default session (no in-flight tracking needed).
    fn new_untracked(ctx: SessionContext) -> Self {
        Self {
            ctx,
            in_flight_requests: None,
        }
    }
}

impl std::ops::Deref for SessionGuard {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        if let Some(ref counter) = self.in_flight_requests {
            counter.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Holds the named sessions map and the default connection references.
/// Methods on this type implement the session-related MCP tools and helpers.
pub(crate) struct SessionStore {
    pub(crate) sessions: Arc<Mutex<HashMap<String, Session>>>,
    pub(crate) config: Arc<Config>,
    pub(crate) db: Arc<sqlx::MySqlPool>,
    pub(crate) introspector: Arc<SchemaIntrospector>,
    /// Total connections across all sessions (for max_total_connections enforcement)
    pub(crate) total_connections: Arc<AtomicU32>,
}

/// Validate a MySQL identifier (session name or database name): max 64 chars,
/// alphanumeric/underscore/hyphen only. Returns `Err(CallToolResult)` on failure.
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
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(crate::server::error::error_response(format!(
            "{} must contain only alphanumeric characters, underscores, or hyphens",
            kind
        )));
    }
    Ok(())
}

impl SessionStore {
    /// Resolve the "session" key from the args map to a SessionGuard.
    /// Updates last_used on non-default sessions.
    /// Increments in_flight_requests on named sessions; the guard decrements on Drop.
    /// Returns Err(CallToolResult) that callers can propagate immediately with `?`.
    pub(crate) async fn resolve_session(
        &self,
        args: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<SessionGuard, CallToolResult> {
        let name = args
            .get("session")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        if name == "default" || name.is_empty() {
            return Ok(SessionGuard::new_untracked(SessionContext {
                pool: self.db.as_ref().clone(),
                schema: self.introspector.clone(),
                database: self.config.connection.database.clone(),
            }));
        }
        let mut map = self.sessions.lock().await;
        match map.get_mut(name) {
            Some(session) => {
                session.last_used = std::time::Instant::now();
                let pool = session.pool.clone();
                let schema = session.introspector.clone();
                let database = session.database.clone();
                let in_flight_requests = session.in_flight_requests.clone();
                drop(map);
                let ctx = SessionContext {
                    pool,
                    schema,
                    database,
                };
                Ok(SessionGuard::new_tracked(ctx, in_flight_requests))
            }
            None => {
                let msg = format!(
                    "Session '{}' not found. Use mysql_connect to create it, or omit 'session' to use the default connection.",
                    name
                );
                drop(map);
                Err(crate::server::error::error_response(msg))
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
            _ => return tool_error!("Missing required argument: name"),
        };
        if name == "default" {
            return tool_error!("Session name 'default' is reserved");
        }

        // Validate session name: max 64 chars, alphanumeric + underscore + hyphen only
        if let Err(e) = validate_identifier(&name, "Session name") {
            return Ok(e);
        }

        if !self.config.security.allow_runtime_connections {
            return tool_error!(
                "Runtime connections are disabled. Set MYSQL_ALLOW_RUNTIME_CONNECTIONS=true to enable mysql_connect with raw credentials."
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

        // Validate host with DNS resolution to prevent DNS rebinding attacks
        let dns_cache_ttl = std::time::Duration::from_secs(self.config.security.dns_cache_ttl_secs);
        let host_validation = validate_host_with_dns(&host, dns_cache_ttl).await;
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
            if !std::path::Path::new(ca_path).exists() {
                return tool_error!("SSL CA file not found: {}", ca_path);
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
            // Validate SSH bastion host with DNS resolution
            let ssh_host_validation = validate_host_with_dns(ssh_h, dns_cache_ttl).await;
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
            "mysql_connect: creating session"
        );

        // Pre-check: fast path before the expensive pool/tunnel creation.
        // We re-check both conditions after creation to handle concurrent races.
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
                    "Session '{}' already exists. Use mysql_disconnect to close it first, or choose a different name.",
                    name
                );
            }
        }

        // Check total connections limit before creating a new session pool
        let current_total = self.total_connections.load(Ordering::Relaxed);
        let max_total = self.config.security.max_total_connections;
        if current_total + NAMED_SESSION_POOL_SIZE > max_total {
            return tool_error!(
                "Total connection limit ({}) would be exceeded. Current: {}, new session would add {}. Disconnect a session first.",
                max_total, current_total, NAMED_SESSION_POOL_SIZE
            );
        }

        let (pool, tunnel) = if let Some(ref ssh_host_str) = ssh_host {
            // Validate SSH user is present
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
                Err(e) => return tool_error!("SSH tunnel or connection failed: {}", e),
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
                Err(e) => return tool_error!("Connection failed: {}", e),
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
        {
            let sessions = self.sessions.lock().await;
            if sessions.len() >= self.config.security.max_sessions as usize {
                // Drop the lock before closing resources — tunnel.close() or pool.close()
                // could be slow, and holding the lock would block all session operations.
                drop(sessions);
                if let Some(t) = tunnel {
                    if let Err(e) = t.close().await {
                        tracing::warn!("SSH tunnel close error on session limit rejection: {}", e);
                    }
                }
                pool.close().await;
                return tool_error!(
                    "Maximum session limit ({}) reached. Disconnect an existing session first.",
                    self.config.security.max_sessions
                );
            }
            if sessions.contains_key(&name) {
                drop(sessions);
                if let Some(t) = tunnel {
                    if let Err(e) = t.close().await {
                        tracing::warn!(
                            "SSH tunnel close error on duplicate session rejection: {}",
                            e
                        );
                    }
                }
                pool.close().await;
                return tool_error!(
                    "Session '{}' already exists. Use mysql_disconnect to close it first, or choose a different name.",
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
            in_flight_requests: Arc::new(AtomicU32::new(0)),
        };
        // Re-acquire the lock to insert. A concurrent connect() could have raced
        // us since we released the lock above; re-check before inserting.
        let mut sessions = self.sessions.lock().await;
        if sessions.len() >= self.config.security.max_sessions as usize
            || sessions.contains_key(&name)
        {
            // Lost the race — clean up and report.
            drop(sessions);
            if let Some(t) = session.tunnel {
                if let Err(e) = t.close().await {
                    tracing::warn!("SSH tunnel close error on post-race cleanup: {}", e);
                }
            }
            session.pool.close().await;
            return tool_error!("Session creation raced with another request. Please retry.");
        }
        sessions.insert(name, session);
        // Increment total connections counter after successful insertion
        self.total_connections
            .fetch_add(NAMED_SESSION_POOL_SIZE, Ordering::Relaxed);

        // Add security warnings if any
        let mut response = info;
        let warnings = self.config.security.security_warnings();
        if !warnings.is_empty() {
            response["security_warnings"] = json!(warnings);
        }
        Ok(serialize_response(&response))
    }

    // ------------------------------------------------------------------
    // Tool handler: mysql_disconnect
    // ------------------------------------------------------------------
    pub(crate) async fn handle_disconnect(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let Some(name) = args.get("name").and_then(|v| v.as_str()) else {
            return tool_error!("Missing required argument: name");
        };
        if name == "default" {
            return tool_error!("The default session cannot be closed");
        }
        let removed = {
            let mut sessions = self.sessions.lock().await;
            sessions.remove(name)
        };
        if let Some(session) = removed {
            // Decrement total connections counter
            self.total_connections
                .fetch_sub(NAMED_SESSION_POOL_SIZE, Ordering::Relaxed);
            // Clean up SSH tunnel if present (outside the lock — close() may be slow).
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
            tool_error!("Session '{}' not found", name)
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
