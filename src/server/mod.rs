//! MCP (Model Context Protocol) server implementation for MySQL.
//!
//! This module serves as the primary entry point for the MySQL MCP server,
//! implementing the MCP protocol to expose MySQL databases via standardized
//! tool interfaces.
//!
//! # Architecture Overview
//!
//! The server acts as a bridge between the MCP protocol layer (via the `rmcp` crate)
//! and MySQL database operations. It coordinates several subsystems:
//!
//! - **Tool handlers** ([`handlers`]): Implement the actual MCP tools like `mysql_query`,
//!   `mysql_schema_info`, `mysql_connect`, etc. These handlers parse input parameters,
//!   validate permissions, execute database operations, and serialize responses.
//!
//! - **Session management** ([`sessions`]): Manages both the default connection
//!   (configured at startup) and named sessions created at runtime. Sessions support
//!   SSH tunneling for secure access through bastion hosts, with automatic cleanup
//!   of idle sessions via a background reaper task.
//!
//! - **Database connection pool** ([`crate::db`]): Provides the underlying MySQL
//!   connection pools used by both the default session and named sessions.
//!
//! - **Schema introspection** ([`crate::schema`]): Caches and provides metadata about
//!   database tables, indexes, and foreign keys for schema-aware operations.
//!
//! # The [`McpServer`] Struct
//!
//! [`McpServer`] is the core server type that implements the [`rmcp::ServerHandler`]
//! trait. It handles MCP protocol requests including:
//!
//! - **Server info**: Returns server capabilities and metadata
//! - **Tool listing**: Advertises available MySQL tools (`mysql_query`, `mysql_connect`, etc.)
//! - **Tool invocation**: Routes incoming tool calls to the appropriate handlers
//!
//! The struct holds:
//! - Configuration ([`Config`]) for security settings and pool parameters
//! - The default database connection pool ([`sqlx::MySqlPool`])
//! - A schema introspector for metadata caching
//! - A [`SessionStore`] for managing named sessions
//! - A background reaper task that cleans up idle sessions after 10 minutes
//!
//! # Security Features
//!
//! The server includes several security mechanisms:
//!
//! - **Host validation**: Validates hostnames via DNS resolution, blocking loopback,
//!   link-local, multicast, and other private IP ranges to prevent SSRF attacks.
//!   See [`validate_host_with_dns`] and [`is_blocked_ip`].
//!
//! - **Permission checking**: Enforces configurable restrictions on SQL statement
//!   types (INSERT, UPDATE, DELETE, DDL) via [`crate::permissions`].
//!
//! - **Connection limits**: Enforces maximum session counts and total connection
//!   limits to prevent resource exhaustion.
//!
//! - **Identifier validation**: Ensures session names and database names contain
//!   only safe characters (alphanumeric and underscores).
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use mysql_mcp::config::Config;
//! use mysql_mcp::server::McpServer;
//!
//! async fn start_server() -> anyhow::Result<()> {
//!     let config = Arc::new(Config::from_env()?);
//!     let db = Arc::new(sqlx::MySqlPool::connect("...").await?);
//!     let server = McpServer::new(config, db, None);
//!     server.run().await
//! }
//! ```
//!
//! # Module Structure
//!
//! - [`error`]: Error response formatting and the `tool_error!` macro
//! - [`handlers`]: MCP tool handler implementations
//! - [`sessions`]: Named session management and lifecycle
//! - [`tool_schemas`]: JSON schemas for tool input validation

use anyhow::Result;
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResult, ErrorCode, Implementation, ListToolsResult,
        PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
    transport::stdio,
    ErrorData as McpError, RoleServer, ServerHandler, ServiceExt,
};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tokio::sync::{oneshot, Mutex};

use crate::config::Config;
use crate::schema::SchemaIntrospector;

mod error;
mod handlers;
mod sessions;
mod tool_schemas;

use sessions::SessionStore;
use tool_schemas::*;

/// Check if an IP address is in a blocked range.
///
/// This function is used for SSRF prevention by blocking potentially dangerous
/// IP address ranges that could be used to access internal services.
///
/// When `allow_loopback` is true, loopback addresses are permitted (for hostname
/// resolution where localhost is legitimate). When false, loopback is blocked
/// (for direct IP connections).
///
/// # Blocked ranges
///
/// - IPv4: link-local (169.254.0.0/16), broadcast, unspecified (0.0.0.0), multicast
/// - IPv6: link-local (fe80::/10), unspecified (::), multicast
/// - IPv4-mapped IPv6 addresses are checked as IPv4
fn is_blocked_ip(ip: IpAddr, allow_loopback: bool) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let loopback_blocked = !allow_loopback && v4.is_loopback();
            loopback_blocked
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_unspecified()
                || v4.is_multicast()
        }
        IpAddr::V6(v6) => {
            // Check IPv4-mapped addresses (::ffff:a.b.c.d)
            // Use to_ipv4_mapped() — NOT to_ipv4() — because to_ipv4()
            // also converts "IPv4-compatible" addresses like ::1 into
            // Some(0.0.0.1), which would bypass the IPv6 loopback check.
            if let Some(v4) = v6.to_ipv4_mapped() {
                let loopback_blocked = !allow_loopback && v4.is_loopback();
                return loopback_blocked
                    || v4.is_link_local()
                    || v4.is_broadcast()
                    || v4.is_unspecified()
                    || v4.is_multicast();
            }
            let loopback_blocked = !allow_loopback && v6.is_loopback();
            loopback_blocked
                || v6.is_unspecified()
                || v6.is_unicast_link_local()
                || v6.is_multicast()
        }
    }
}

/// Result of host validation with DNS resolution.
///
/// Returned by [`validate_host_with_dns`] to indicate whether a host is allowed
/// and optionally provide a reason for blocking.
pub(crate) struct HostValidation {
    /// Whether the host is allowed for connections
    pub(crate) allowed: bool,
    /// Optional human-readable reason when the host is blocked
    pub(crate) reason: Option<String>,
}

/// Validate a host string, resolving hostnames via DNS.
///
/// This function performs SSRF-safe host validation:
/// 1. For literal IP addresses: checks against blocked ranges directly
/// 2. For hostnames: resolves via DNS with a 5-second timeout, then validates
///    all returned IP addresses
///
/// Loopback addresses are allowed for hostnames (to support "localhost" resolution)
/// but blocked for direct IP connections.
///
/// # Arguments
///
/// - `host`: The host string to validate (IP address or hostname)
///
/// # Returns
///
/// A [`HostValidation`] indicating whether the host is allowed and why not if blocked.
pub(crate) async fn validate_host_with_dns(host: &str) -> HostValidation {
    // Fast path: literal IP address (no DNS lookup needed)
    if let Ok(ip) = host.parse::<IpAddr>() {
        let blocked = is_blocked_ip(ip, false);
        return HostValidation {
            allowed: !blocked,
            reason: if blocked {
                Some(format!(
                    "IP address {} is in a blocked range (loopback/link-local/multicast)",
                    host
                ))
            } else {
                None
            },
        };
    }

    // Hostname: resolve via DNS and check all IPs
    let hostname = host.to_lowercase();
    if hostname.is_empty() {
        return HostValidation {
            allowed: false,
            reason: Some("Host cannot be empty".to_string()),
        };
    }

    let lookup_target = format!("{}:0", hostname);
    let lookup_future = tokio::net::lookup_host(&lookup_target);
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), lookup_future).await;
    match result {
        Ok(Ok(addrs)) => {
            let mut found_any = false;
            for ip in addrs.map(|a| a.ip()) {
                found_any = true;
                if is_blocked_ip(ip, true) {
                    return HostValidation {
                        allowed: false,
                        reason: Some(format!(
                            "Hostname '{}' resolves to blocked IP address {} (link-local/multicast)",
                            hostname, ip
                        )),
                    };
                }
            }
            if !found_any {
                return HostValidation {
                    allowed: false,
                    reason: Some(format!("Hostname '{}' resolved to no addresses", hostname)),
                };
            }
            HostValidation {
                allowed: true,
                reason: None,
            }
        }
        Ok(Err(e)) => HostValidation {
            allowed: false,
            reason: Some(format!("DNS resolution failed for '{}': {}", hostname, e)),
        },
        Err(_) => HostValidation {
            allowed: false,
            reason: Some(format!("DNS resolution timed out for '{}'", hostname)),
        },
    }
}

/// Check whether a host string is a blocked IP address.
/// This is a helper for testing `is_blocked_ip` logic.
/// For async operations with DNS caching, use `validate_host_with_dns`.
#[cfg(test)]
fn is_private_host(host: &str) -> bool {
    if let Ok(ip) = host.parse::<IpAddr>() {
        is_blocked_ip(ip, false)
    } else {
        false // hostname — allowed (DNS validation happens in async path)
    }
}

/// The main MCP server that handles protocol requests and coordinates MySQL operations.
///
/// This struct implements [`rmcp::ServerHandler`] to serve as the MCP protocol endpoint.
/// It manages the default database connection, named sessions, and routes tool calls
/// to the appropriate handlers.
///
/// # Fields
///
/// - `config`: Server configuration including security settings, pool parameters, and
///   connection limits
/// - `db`: The default database connection pool used when no session is specified
/// - `introspector`: Schema introspector for caching table metadata
/// - `store`: Manages named sessions and their lifecycle
/// - `_default_tunnel`: Optional SSH tunnel handle for the default connection
/// - `_shutdown_tx`: Signals the session reaper task to shut down when the server drops
pub struct McpServer {
    pub config: Arc<Config>,
    pub db: Arc<sqlx::MySqlPool>,
    pub introspector: Arc<SchemaIntrospector>,
    store: SessionStore,
    /// Holds the SSH tunnel for the default session alive for the server's lifetime.
    /// None when not using SSH tunneling.
    _default_tunnel: Option<crate::tunnel::TunnelHandle>,
    /// When dropped, signals the session reaper task to shut down.
    _shutdown_tx: oneshot::Sender<()>,
}

impl McpServer {
    /// Creates a new MCP server instance with the given configuration and database pool.
    ///
    /// This initializes:
    /// - A schema introspector for caching table metadata
    /// - An empty session store for named sessions
    /// - A background reaper task that periodically cleans up idle sessions (after 10 minutes)
    ///
    /// # Arguments
    ///
    /// - `config`: Server configuration (connection settings, security, pool parameters)
    /// - `db`: The default database connection pool
    /// - `tunnel`: Optional SSH tunnel handle for the default connection
    ///
    /// # Session Reaper
    ///
    /// The server spawns a background task that runs every 60 seconds to detect and
    /// clean up named sessions that have been idle for more than 10 minutes. This task
    /// gracefully shuts down when the server is dropped.
    pub fn new(
        config: Arc<Config>,
        db: Arc<sqlx::MySqlPool>,
        tunnel: Option<crate::tunnel::TunnelHandle>,
    ) -> Self {
        let introspector = Arc::new(SchemaIntrospector::new(
            db.clone(),
            config.pool.cache_ttl_secs,
        ));
        let sessions: Arc<Mutex<HashMap<String, sessions::Session>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Total connections counter shared between session store and reaper
        let total_connections: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

        // Shutdown channel for graceful termination of the session reaper task.
        // When McpServer is dropped, the sender is dropped, causing receivers to get
        // a Closed error, which signals the reaper to exit.
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        // Background task: drop sessions idle for > 10 minutes (600 s).
        // "default" is never dropped. SSH tunnels are explicitly closed so the
        // subprocess is reaped rather than relying on Drop's non-blocking start_kill().
        // The task exits when _shutdown_tx is dropped (server shutdown).
        let sessions_reaper = sessions.clone();
        let reaper_total_connections = total_connections.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tokio::select! {
                    // Prioritize shutdown signal over interval tick
                    _ = &mut shutdown_rx => break,
                    _ = interval.tick() => {}
                }
                // Use a single lock scope for both identifying and removing stale sessions
                // to avoid TOCTOU race conditions between collection and removal.
                let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(600);
                let reaped: Vec<sessions::Session> = {
                    let mut map = sessions_reaper.lock().await;
                    let stale_names: Vec<String> = map
                        .iter()
                        .filter(|(_, s)| s.last_used <= cutoff)
                        .map(|(name, _)| name.clone())
                        .collect();
                    let mut reaped = Vec::with_capacity(stale_names.len());
                    for name in stale_names {
                        if let Some(session) = map.remove(&name) {
                            // Decrement total connections counter for reaped session
                            reaper_total_connections
                                .fetch_sub(sessions::NAMED_SESSION_POOL_SIZE, Ordering::AcqRel);
                            reaped.push(session);
                        }
                    }
                    reaped
                };
                // Perform async cleanup outside the lock
                for session in reaped {
                    if let Some(tunnel) = session.tunnel {
                        sessions::close_tunnel_with_timeout(tunnel, "during session reap").await;
                    }
                    session.pool.close().await;
                }
            }
        });

        let store = SessionStore {
            sessions,
            config: config.clone(),
            db: db.clone(),
            introspector: introspector.clone(),
            total_connections,
        };

        Self {
            config,
            db,
            introspector,
            store,
            _default_tunnel: tunnel,
            _shutdown_tx: shutdown_tx,
        }
    }

    /// Runs the MCP server over stdio transport.
    ///
    /// This method starts the server and blocks until the MCP protocol connection
    /// closes (e.g., when the client disconnects). It uses stdio for communication,
    /// making it suitable for use as a subprocess in MCP hosts.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport fails to initialize or if there's an
    /// unrecoverable protocol error during operation.
    pub async fn run(self) -> Result<()> {
        let service = self.serve(stdio()).await?;
        service.waiting().await?;
        Ok(())
    }
}

impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::default(),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "mysql-mcp".to_string(),
                title: Some("MySQL MCP Server".to_string()),
                version: env!("CARGO_PKG_VERSION").to_string(),
                description: Some(
                    "Expose MySQL databases via the Model Context Protocol".to_string(),
                ),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Use mysql_query to execute SQL queries against the connected MySQL database."
                    .to_string(),
            ),
        }
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        {
            let tools = vec![
                Tool::new(
                    "mysql_query",
                    concat!(
                        "Execute a SQL query against MySQL. ",
                        "Always returned: rows, row_count, execution_time_ms, serialization_time_ms. ",
                        "Optional: plan (only when explain:true or server auto-triggers for slow queries), ",
                        "capped+next_offset+capped_hint (only when result was truncated to max_rows limit), ",
                        "suggestions (only when a full table scan is detected). ",
                        "parse_warnings (only when non-empty — hints about missing LIMIT, leading wildcards, etc.). ",
                        "Supports SELECT, SHOW, EXPLAIN, and (if configured) INSERT, UPDATE, DELETE, DDL.",
                    ),
                    mysql_query_schema(),
                ),
                Tool::new(
                    "mysql_schema_info",
                    concat!(
                        "Get schema metadata for a table. ",
                        "Default (no include): column names, types, nullability only. ",
                        "include:[indexes]: also returns all indexes with their columns. ",
                        "include:[foreign_keys]: also returns FK constraints. ",
                        "include:[size]: also returns estimated row count and byte sizes. ",
                        "Combine any subset, e.g. include:[indexes,foreign_keys,size] for full detail.",
                    ),
                    mysql_schema_info_schema(),
                ),
                Tool::new(
                    "mysql_server_info",
                    "Get MySQL server metadata: version, current_database, current_user, sql_mode, character_set, collation, time_zone, read_only flag, accessible_features (list of enabled operation types), and which write operations are enabled by server config. Use to understand the environment before writing queries or when diagnosing connection issues.",
                    mysql_server_info_schema(),
                ),
                Tool::new(
                    "mysql_connect",
                    concat!(
                        "Create a named session to a different MySQL server or database. ",
                        "Use this to: (1) access a different MySQL host, (2) connect with different credentials, ",
                        "(3) route queries to a read replica, (4) work with a different database on the same server. ",
                        "Requires MYSQL_ALLOW_RUNTIME_CONNECTIONS=true. ",
                        "Sessions idle for >10 minutes are automatically closed. ",
                        "Pass the session name to other tools via the 'session' parameter.",
                    ),
                    mysql_connect_schema(),
                ),
                Tool::new(
                    "mysql_disconnect",
                    "Explicitly close a named database session. The default session cannot be closed.",
                    mysql_disconnect_schema(),
                ),
                Tool::new(
                    "mysql_list_sessions",
                    "List all active named database sessions with host, database, and idle time. The default session is always shown first.",
                    mysql_list_sessions_schema(),
                ),
                Tool::new(
                    "mysql_explain_plan",
                    "Get the execution plan for a SELECT query without running it. Returns index_used, rows_examined_estimate, optimization tier, full_table_scan (bool), extra_flags (array of optimizer notes), and note. Use this before executing a potentially expensive query to check efficiency.",
                    mysql_explain_plan_schema(),
                ),
                Tool::new(
                    "mysql_list_tables",
                    "List all tables in the current or specified database. More discoverable than querying information_schema directly.",
                    mysql_list_tables_schema(),
                ),
            ];

            Ok(ListToolsResult {
                meta: None,
                tools,
                next_cursor: None,
            })
        }
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        {
            let args = request.arguments.unwrap_or_default();
            match request.name.as_ref() {
                "mysql_connect" => self.store.handle_connect(args).await,
                "mysql_disconnect" => self.store.handle_disconnect(args).await,
                "mysql_list_sessions" => self.store.handle_list_sessions(args).await,
                "mysql_schema_info" => self.store.handle_schema_info(args).await,
                "mysql_server_info" => self.store.handle_server_info(args).await,
                "mysql_explain_plan" => self.store.handle_explain_plan(args).await,
                "mysql_list_tables" => self.store.handle_list_tables(args).await,
                "mysql_query" => self.store.handle_query(args).await,
                name => Err(McpError::new(
                    ErrorCode::METHOD_NOT_FOUND,
                    format!("Unknown tool: {}", name),
                    None,
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{is_blocked_ip, is_private_host};

    #[test]
    fn ipv6_link_local_is_blocked() {
        assert!(is_private_host("fe80::1"), "fe80::1 must be blocked");
        assert!(
            is_private_host("fe80::169:254:169:254"),
            "fe80::169:254:169:254 must be blocked"
        );
    }

    #[test]
    fn ipv6_documentation_range_is_allowed() {
        assert!(
            !is_private_host("2001:db8::1"),
            "2001:db8::1 (documentation) must be allowed"
        );
    }

    #[test]
    fn ipv6_mapped_ipv4_loopback_is_blocked() {
        assert!(
            is_private_host("::ffff:127.0.0.1"),
            "::ffff:127.0.0.1 (IPv6-mapped loopback) must be blocked"
        );
    }

    #[test]
    fn ipv6_mapped_ipv4_link_local_is_blocked() {
        assert!(
            is_private_host("::ffff:169.254.169.254"),
            "::ffff:169.254.169.254 (IPv6-mapped cloud metadata) must be blocked"
        );
    }

    #[test]
    fn ipv4_multicast_is_blocked() {
        assert!(
            is_private_host("224.0.0.1"),
            "224.0.0.1 (IPv4 multicast) must be blocked"
        );
    }

    #[test]
    fn ipv6_multicast_is_blocked() {
        assert!(
            is_private_host("ff02::1"),
            "ff02::1 (IPv6 multicast) must be blocked"
        );
    }

    #[test]
    fn ipv4_private_rfc1918_is_allowed() {
        assert!(!is_private_host("10.0.0.1"), "10.0.0.1 must be allowed");
        assert!(!is_private_host("172.16.0.1"), "172.16.0.1 must be allowed");
        assert!(
            !is_private_host("192.168.1.1"),
            "192.168.1.1 must be allowed"
        );
    }

    // Tests for is_blocked_ip function
    // allow_loopback=false: used for direct IP connections (loopback blocked)
    // allow_loopback=true: used for hostname resolution (loopback allowed for localhost)

    #[test]
    fn test_is_blocked_ip_ipv4_loopback() {
        // Direct IP: loopback blocked
        assert!(is_blocked_ip("127.0.0.1".parse().unwrap(), false));
        assert!(is_blocked_ip("127.255.255.255".parse().unwrap(), false));
        // Hostname resolution: loopback allowed
        assert!(!is_blocked_ip("127.0.0.1".parse().unwrap(), true));
    }

    #[test]
    fn test_is_blocked_ip_ipv4_link_local() {
        // Link-local always blocked
        assert!(is_blocked_ip("169.254.0.1".parse().unwrap(), false));
        assert!(is_blocked_ip("169.254.169.254".parse().unwrap(), false));
        assert!(is_blocked_ip("169.254.0.1".parse().unwrap(), true));
    }

    #[test]
    fn test_is_blocked_ip_ipv4_broadcast() {
        assert!(is_blocked_ip("255.255.255.255".parse().unwrap(), false));
        assert!(is_blocked_ip("255.255.255.255".parse().unwrap(), true));
    }

    #[test]
    fn test_is_blocked_ip_ipv4_unspecified() {
        assert!(is_blocked_ip("0.0.0.0".parse().unwrap(), false));
        assert!(is_blocked_ip("0.0.0.0".parse().unwrap(), true));
    }

    #[test]
    fn test_is_blocked_ip_ipv4_multicast() {
        assert!(is_blocked_ip("224.0.0.1".parse().unwrap(), false));
        assert!(is_blocked_ip("239.255.255.255".parse().unwrap(), false));
        assert!(is_blocked_ip("224.0.0.1".parse().unwrap(), true));
    }

    #[test]
    fn test_is_blocked_ip_ipv4_public_allowed() {
        assert!(!is_blocked_ip("8.8.8.8".parse().unwrap(), false));
        assert!(!is_blocked_ip("1.1.1.1".parse().unwrap(), false));
        assert!(!is_blocked_ip("8.8.8.8".parse().unwrap(), true));
    }

    #[test]
    fn test_is_blocked_ip_ipv6_loopback() {
        // Direct IP: loopback blocked
        assert!(is_blocked_ip("::1".parse().unwrap(), false));
        // Hostname resolution: loopback allowed
        assert!(!is_blocked_ip("::1".parse().unwrap(), true));
    }

    #[test]
    fn test_is_blocked_ip_ipv6_unspecified() {
        assert!(is_blocked_ip("::".parse().unwrap(), false));
        assert!(is_blocked_ip("::".parse().unwrap(), true));
    }

    #[test]
    fn test_is_blocked_ip_ipv6_link_local() {
        assert!(is_blocked_ip("fe80::1".parse().unwrap(), false));
        assert!(is_blocked_ip("fe80::1".parse().unwrap(), true));
    }

    #[test]
    fn test_is_blocked_ip_ipv6_multicast() {
        assert!(is_blocked_ip("ff02::1".parse().unwrap(), false));
        assert!(is_blocked_ip("ff02::1".parse().unwrap(), true));
    }

    #[test]
    fn test_is_blocked_ip_public_ipv6_allowed() {
        assert!(!is_blocked_ip(
            "2001:4860:4860::8888".parse().unwrap(),
            false
        ));
        assert!(!is_blocked_ip(
            "2001:4860:4860::8888".parse().unwrap(),
            true
        ));
    }
}
