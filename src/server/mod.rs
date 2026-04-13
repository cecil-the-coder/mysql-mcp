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
/// When `allow_loopback` is true, loopback addresses are permitted (for hostname
/// resolution where localhost is legitimate). When false, loopback is blocked
/// (for direct IP connections).
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
pub(crate) struct HostValidation {
    pub(crate) allowed: bool,
    pub(crate) reason: Option<String>,
}

/// Validate a host string, resolving hostnames via DNS.
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
                            // Use saturating_sub to prevent underflow in edge cases
                            let _ = reaper_total_connections.fetch_update(
                                Ordering::AcqRel,
                                Ordering::Acquire,
                                |current| {
                                    Some(current.saturating_sub(sessions::NAMED_SESSION_POOL_SIZE))
                                },
                            );
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
                concat!(
                    "You are connected to a MySQL database via the Model Context Protocol. ",
                    "Available tools and recommended workflows:",
                    "\n\n",
                    "1. INITIAL SETUP: Use mysql_server_info to check the MySQL version, ",
                    "current database, user permissions, and which features are enabled. ",
                    "This helps you understand what operations are allowed and the environment context.",
                    "\n\n",
                    "2. SCHEMA DISCOVERY: Before querying unknown tables, use mysql_list_tables ",
                    "to see available tables, then use mysql_schema_info to inspect table structure, ",
                    "columns, indexes, foreign keys, and table sizes. This helps write correct queries ",
                    "and understand relationships.",
                    "\n\n",
                    "3. QUERY PLANNING: For complex or potentially expensive queries, use mysql_explain_plan ",
                    "to check the execution plan before running. This shows if indexes will be used, ",
                    "estimated rows to scan, and whether a full table scan will occur.",
                    "\n\n",
                    "4. EXECUTION: Use mysql_query to run SELECT, INSERT, UPDATE, DELETE, or DDL statements. ",
                    "The query tool returns results, timing, and may include warnings about missing LIMITs ",
                    "or full table scans. Enable explain:true for automatic execution plan inclusion.",
                    "\n\n",
                    "5. SESSION MANAGEMENT: Use mysql_connect to create named sessions for different ",
                    "databases or hosts (requires MYSQL_ALLOW_RUNTIME_CONNECTIONS). Use mysql_list_sessions ",
                    "to see active sessions and mysql_disconnect to close unused ones. Idle sessions ",
                    "are automatically cleaned up after 10 minutes.",
                    "\n\n",
                    "RECOMMENDED WORKFLOW: server_info → list_tables → schema_info → (explain_plan for expensive queries) → mysql_query"
                )
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
