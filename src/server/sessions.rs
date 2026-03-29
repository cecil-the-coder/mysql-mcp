//! Named database session management for multi-database connectivity.
//!
//! This module implements [`SessionStore`] which manages both the default connection
//! (configured at startup) and named sessions created at runtime via the `mysql_connect` tool.
//!
//! # Key Features
//!
//! - **Session creation**: Create named database connections with optional SSH tunnel support
//!   for accessing databases through bastion hosts
//! - **Idle session reaping**: Sessions track their `last_used` timestamp; the server's
//!   reaper task cleans up sessions idle for more than 10 minutes
//! - **Connection pool limits**: Each named session uses a fixed pool size (5 connections),
//!   with enforcement of `max_sessions` and `max_total_connections` limits
//! - **Session lifecycle**: Full lifecycle management including creation, lookup,
//!   listing, and cleanup with proper resource release
//!
//! # Session Resolution
//!
//! Tools that accept a `session` parameter use [`SessionStore::resolve_session`] to
//! obtain a [`SessionContext`] containing the connection pool and schema introspector.
//! If no session is specified, the default connection is used.

use rmcp::model::CallToolResult;
use serde_json::json;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    /// Total connections across all sessions (for max_total_connections enforcement)
    pub(crate) total_connections: Arc<AtomicU32>,
}

/// Validate a MySQL identifier (session name or database name): max 64 chars,
/// alphanumeric/underscore only, and explicitly rejects path traversal characters
/// (`..`, `/`, `\`) as a defense-in-depth measure against security issues if the
/// identifier is ever used in file paths. Returns `Err(CallToolResult)` on failure.
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
    // Defense-in-depth: explicitly reject path traversal characters even though the
    // alphanumeric check below would also catch them. These dedicated checks ensure
    // that if the character allowlist is ever relaxed, path traversal protection
    // remains in place and produces a clear security-focused error message.
    if value.contains("..") {
        return Err(crate::server::error::error_response(format!(
            "{} must not contain '..' (path traversal rejected)",
            kind
        )));
    }
    if value.contains('/') || value.contains('\\') {
        return Err(crate::server::error::error_response(format!(
            "{} must not contain path separators ('/' or '\\')",
            kind
        )));
    }
    if value.contains('.') {
        return Err(crate::server::error::error_response(format!(
            "{} must not contain '.'",
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

/// Guard that decrements total_connections on drop unless dismissed.
/// Used in handle_connect to ensure the connection counter is always
/// decremented exactly once on any failure path.
pub(crate) struct ConnectionReservationGuard<'a> {
    total_connections: &'a AtomicU32,
    dismissed: bool,
}

impl<'a> ConnectionReservationGuard<'a> {
    fn new(total_connections: &'a AtomicU32) -> Self {
        Self {
            total_connections,
            dismissed: false,
        }
    }

    /// Dismiss the guard so it won't decrement on drop.
    /// Call this once the session is successfully inserted.
    fn dismiss(mut self) {
        self.dismissed = true;
    }
}

impl<'a> Drop for ConnectionReservationGuard<'a> {
    fn drop(&mut self) {
        if !self.dismissed {
            self.total_connections
                .fetch_sub(NAMED_SESSION_POOL_SIZE, Ordering::AcqRel);
        }
    }
}

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
            // The TunnelHandle is dropped here, which triggers non-blocking start_kill()
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

        // Validate session name: max 64 chars, alphanumeric + underscore only
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
            Some(p) if (1..=65535).contains(&p) => {
                p.try_into().expect("port range already validated")
            }
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
            // Validate SSH bastion host with DNS resolution
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
            Some(p) if (1..=65535).contains(&p) => {
                p.try_into().expect("ssh_port range already validated")
            }
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

        // Block dangerous combination: runtime connections + insecure SSH host key checking.
        // Without host key verification, an attacker can MITM the SSH tunnel and
        // intercept database credentials supplied at runtime.
        if ssh_host.is_some() && ssh_known_hosts_check == "insecure" {
            return tool_error!(
                "ssh_known_hosts_check='insecure' cannot be used with runtime connections. \
                 This combination allows SSH tunnels without host key verification, enabling \
                 man-in-the-middle attacks that could intercept database credentials. \
                 Use 'strict' or 'accept-new' instead."
            );
        }
        if let Some(ref key_path) = ssh_private_key {
            if !std::path::Path::new(key_path).exists() {
                return tool_error!("SSH private key file not found: {}", key_path);
            }
            if let Err(e) = crate::config::check_private_key_permissions(key_path) {
                return tool_error!("{}", e);
            }
        }
        if let Some(ref khf) = ssh_known_hosts_file {
            let khf_path = std::path::Path::new(khf);
            if ssh_known_hosts_check == "strict" {
                if !khf_path.exists() {
                    return tool_error!(
                        "SSH known_hosts file not found: {} (required for strict mode)",
                        khf
                    );
                }
                if let Err(e) = crate::config::check_known_hosts_permissions(khf) {
                    return tool_error!("{}", e);
                }
            } else if let Some(parent) = khf_path.parent() {
                if !parent.exists() {
                    return tool_error!(
                        "SSH known_hosts_file parent directory does not exist: {}",
                        parent.display()
                    );
                }
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
        // A single atomic check-and-insert after pool creation handles any races.
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

        // Atomically check and reserve connection slots to prevent races.
        // We use fetch_update to atomically check the limit and increment.
        let max_total = self.config.security.max_total_connections;
        let reserve_result =
            self.total_connections
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    if current.saturating_add(NAMED_SESSION_POOL_SIZE) <= max_total {
                        Some(current.saturating_add(NAMED_SESSION_POOL_SIZE))
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
        // Guard ensures counter is decremented if we exit before dismissing it.
        let reservation_guard = ConnectionReservationGuard::new(self.total_connections.as_ref());

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
                Err(e) => {
                    return tool_error!("SSH tunnel or connection failed: {}", e);
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
                    return tool_error!("Connection failed: {}", e);
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

        // Single atomic check-and-insert using entry API.
        // This eliminates the redundant triple-checking pattern while maintaining correctness.
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
        // Check session limit first
        if sessions.len() >= self.config.security.max_sessions as usize {
            // Release lock before cleanup operations
            drop(sessions);
            if let Some(t) = session.tunnel {
                close_tunnel_with_timeout(t, "on session limit rejection").await;
            }
            session.pool.close().await;
            return tool_error!(
                "Maximum session limit ({}) reached. Disconnect an existing session first.",
                self.config.security.max_sessions
            );
        }

        // Use entry API for atomic check-and-insert
        match sessions.entry(name.clone()) {
            Entry::Occupied(_) => {
                // Release lock before cleanup operations
                drop(sessions);
                if let Some(t) = session.tunnel {
                    close_tunnel_with_timeout(t, "on duplicate session rejection").await;
                }
                session.pool.close().await;
                return tool_error!(
                    "Session '{}' already exists. Use mysql_disconnect to close it first, or choose a different name.",
                    name
                );
            }
            Entry::Vacant(entry) => {
                entry.insert(session);
                // Connection slots were already reserved atomically at the start of handle_connect
            }
        }

        // Success: dismiss the guard so it won't decrement the counter.
        reservation_guard.dismiss();

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
            // Decrement total connections counter
            self.total_connections
                .fetch_sub(NAMED_SESSION_POOL_SIZE, Ordering::AcqRel);
            // Clean up SSH tunnel if present (outside the lock — close() may be slow).
            if let Some(tunnel) = session.tunnel {
                close_tunnel_with_timeout(tunnel, "on disconnect").await;
            }
            // Explicitly close the pool so server-side connections are released
            // immediately rather than waiting for sqlx's Drop impl to handle them.
            session.pool.close().await;
            Ok(serialize_response(&json!({
                "success": true,
                "message": format!("Session '{}' closed", name)
            })))
        } else {
            tool_error!(
                "Session '{}' not found. Use mysql_list_sessions to see available sessions.",
                name
            )
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
                "idle_seconds": std::time::Instant::now().saturating_duration_since(session.last_used).as_secs(),
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
        // CallToolResult wraps the error content; check the text
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
        // Test with space
        let result = validate_identifier("invalid name", "Identifier");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let text = err.content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("must contain only alphanumeric"));

        // Test with dot
        let result = validate_identifier("invalid.name", "Identifier");
        assert!(result.is_err());

        // Test with semicolon
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
        // Hyphens are not allowed — in MySQL, unquoted hyphens parse as subtraction
        let result = validate_identifier("valid-name-123", "Identifier");
        assert!(result.is_err());

        let result = validate_identifier("my-session", "Identifier");
        assert!(result.is_err());

        let result = validate_identifier("test-db-name", "Identifier");
        assert!(result.is_err());

        let result = validate_identifier("mixed_name-with-hyphens", "Identifier");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_identifier_rejects_path_traversal_dotdot() {
        let result = validate_identifier("..", "Session name");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let text = err.content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("path traversal"));

        let result = validate_identifier("foo..bar", "Session name");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let text = err.content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("path traversal"));

        let result = validate_identifier("../etc/passwd", "Session name");
        assert!(result.is_err());
        let text = result.unwrap_err().content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("path traversal"));
    }

    #[test]
    fn test_validate_identifier_rejects_slashes() {
        let result = validate_identifier("foo/bar", "Session name");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let text = err.content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("path separators"));

        let result = validate_identifier("foo\\bar", "Session name");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let text = err.content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("path separators"));

        let result = validate_identifier("/abs/path", "Session name");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_identifier_rejects_dots() {
        let result = validate_identifier("foo.bar", "Session name");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let text = err.content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("must not contain '.'"));

        let result = validate_identifier(".hidden", "Session name");
        assert!(result.is_err());
    }
}
