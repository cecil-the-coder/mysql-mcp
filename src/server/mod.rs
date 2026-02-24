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
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::config::Config;
use crate::schema::SchemaIntrospector;

mod error;
mod handlers;
mod sessions;
mod tool_schemas;

use sessions::SessionStore;
use tool_schemas::*;

// ---------------------------------------------------------------------------
// DNS cache for hostname validation (prevents DNS rebinding attacks)
// ---------------------------------------------------------------------------

/// Cached DNS resolution result with timestamp.
struct DnsCacheEntry {
    ips: Vec<IpAddr>,
    cached_at: Instant,
    blocked: bool,
    reason: Option<String>,
}

/// Global DNS cache protected by a mutex.
/// Key: lowercase hostname. Value: resolution result with TTL.
static DNS_CACHE: std::sync::OnceLock<Arc<Mutex<HashMap<String, DnsCacheEntry>>>> =
    std::sync::OnceLock::new();

fn get_dns_cache() -> Arc<Mutex<HashMap<String, DnsCacheEntry>>> {
    DNS_CACHE
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

/// Clear the DNS cache (for testing).
#[cfg(test)]
#[allow(dead_code)]
async fn clear_dns_cache() {
    let cache = get_dns_cache();
    {
        let _guard = cache.lock().await;
        // drop the guard to release the lock
    }
    let mut guard = cache.lock().await;
    guard.clear();
}

/// Check if an IP address is in a blocked range (loopback, link-local, etc.).
fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_unspecified()
                || v4.is_multicast()
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return v4.is_loopback()
                    || v4.is_link_local()
                    || v4.is_broadcast()
                    || v4.is_unspecified()
                    || v4.is_multicast();
            }
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unicast_link_local()
                || v6.is_multicast()
        }
    }
}

/// Result of host validation with DNS resolution.
#[allow(dead_code)]
pub(crate) struct HostValidation {
    pub(crate) allowed: bool,
    pub(crate) reason: Option<String>,
    pub(crate) resolved_ips: Vec<IpAddr>,
    pub(crate) was_literal_ip: bool,
}

/// Validate a host string, resolving hostnames via DNS with caching.
/// This prevents DNS rebinding attacks by caching resolved IPs.
pub(crate) async fn validate_host_with_dns(host: &str, dns_cache_ttl: Duration) -> HostValidation {
    // Fast path: literal IP address (no DNS lookup needed)
    if let Ok(ip) = host.parse::<IpAddr>() {
        let blocked = is_blocked_ip(ip);
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
            resolved_ips: vec![ip],
            was_literal_ip: true,
        };
    }

    // Hostname: normalize to lowercase for cache key
    let hostname = host.to_lowercase();
    if hostname.is_empty() {
        return HostValidation {
            allowed: false,
            reason: Some("Host cannot be empty".to_string()),
            resolved_ips: vec![],
            was_literal_ip: false,
        };
    }

    // Check cache
    {
        let cache_arc = get_dns_cache();
        let cache = cache_arc.lock().await;
        if let Some(entry) = cache.get(&hostname) {
            if entry.cached_at.elapsed() < dns_cache_ttl {
                tracing::debug!(
                    hostname = %hostname,
                    ips = ?entry.ips,
                    blocked = entry.blocked,
                    "DNS cache hit for hostname validation"
                );
                return HostValidation {
                    allowed: !entry.blocked,
                    reason: entry.reason.clone(),
                    resolved_ips: entry.ips.clone(),
                    was_literal_ip: false,
                };
            }
        }
    }

    // Cache miss or expired — perform DNS lookup
    tracing::debug!(hostname = %hostname, "DNS cache miss, performing lookup");
    // lookup_host expects "hostname:port" format; use a dummy port since we only care about IPs
    let lookup_target = format!("{}:0", hostname);
    let (ips, resolution_error) = match tokio::net::lookup_host(&lookup_target).await {
        Ok(addr_iter) => {
            let resolved_ips: Vec<IpAddr> = addr_iter.map(|addr| addr.ip()).collect();
            (resolved_ips, None)
        }
        Err(e) => (vec![], Some(e.to_string())),
    };

    // Check if any resolved IP is blocked
    let mut blocked = false;
    let mut reason = None;
    if ips.is_empty() {
        if let Some(err) = resolution_error {
            blocked = true;
            reason = Some(format!("DNS resolution failed for '{}': {}", hostname, err));
        } else {
            blocked = true;
            reason = Some(format!(
                "DNS resolution returned no addresses for '{}'",
                hostname
            ));
        }
    } else {
        for ip in &ips {
            if is_blocked_ip(*ip) {
                blocked = true;
                reason = Some(format!(
                    "Hostname '{}' resolves to blocked IP address {} (loopback/link-local/multicast)",
                    hostname, ip
                ));
                break;
            }
        }
    }

    // Cache the result
    {
        let cache_arc = get_dns_cache();
        let mut cache = cache_arc.lock().await;
        cache.insert(
            hostname,
            DnsCacheEntry {
                ips: ips.clone(),
                cached_at: Instant::now(),
                blocked,
                reason: reason.clone(),
            },
        );
    }

    HostValidation {
        allowed: !blocked,
        reason,
        resolved_ips: ips,
        was_literal_ip: false,
    }
}

/// Check whether a host string is a blocked IP address.
/// This is a helper for testing `is_blocked_ip` logic.
/// For async operations with DNS caching, use `validate_host_with_dns`.
#[cfg(test)]
fn is_private_host(host: &str) -> bool {
    if let Ok(ip) = host.parse::<IpAddr>() {
        is_blocked_ip(ip)
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
        let total_connections: Arc<AtomicU32> = Arc::new(AtomicU32::new(config.pool.size));

        // Background task: drop sessions idle for > 10 minutes (600 s).
        // "default" is never dropped. SSH tunnels are explicitly closed so the
        // subprocess is reaped rather than relying on Drop's non-blocking start_kill().
        // Sessions with in-flight requests are skipped to prevent query failures.
        let sessions_reaper = sessions.clone();
        let reaper_total_connections = total_connections.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(600);
                let stale: Vec<String> = {
                    let map = sessions_reaper.lock().await;
                    map.iter()
                        .filter(|(_, s)| s.last_used <= cutoff)
                        .map(|(name, _)| name.clone())
                        .collect()
                };
                for name in stale {
                    let mut map = sessions_reaper.lock().await;
                    // Re-check: session may have been used since we collected the stale
                    // list (TOCTOU). If last_used has advanced past the cutoff, skip it.
                    let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(600);
                    if let Some(session) = map.get(&name) {
                        if session.last_used > cutoff {
                            continue;
                        }
                        // Skip sessions with in-flight requests to prevent query failures
                        if session.in_flight_requests.load(Ordering::Relaxed) > 0 {
                            tracing::debug!(
                                session = %name,
                                in_flight = session.in_flight_requests.load(Ordering::Relaxed),
                                "Skipping idle session reap: in-flight requests"
                            );
                            continue;
                        }
                    }
                    if let Some(session) = map.remove(&name) {
                        // Decrement total connections counter for reaped session
                        reaper_total_connections
                            .fetch_sub(sessions::NAMED_SESSION_POOL_SIZE, Ordering::Relaxed);
                        drop(map); // release lock before awaiting async operations
                        if let Some(tunnel) = session.tunnel {
                            if let Err(e) = tunnel.close().await {
                                tracing::warn!("SSH tunnel close error during session reap: {}", e);
                            }
                        }
                        session.pool.close().await;
                    }
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
        }
    }

    pub async fn run(self) -> Result<()> {
        let service = self.serve(stdio()).await?;
        service.waiting().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::is_private_host;

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
