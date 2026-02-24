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
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::schema::SchemaIntrospector;

mod handlers;
mod sessions;
mod tool_schemas;

use sessions::SessionStore;
use tool_schemas::*;

/// Check whether a host string resolves to a blocked address category.
///
/// Blocks loopback, link-local (cloud metadata endpoint 169.254.169.254), and multicast.
/// Also blocks IPv6-mapped IPv4 addresses (e.g. ::ffff:127.0.0.1) which would otherwise
/// bypass the IPv4 loopback check.
/// RFC 1918 private ranges (10/8, 172.16/12, 192.168/16) are intentionally ALLOWED:
/// database servers legitimately live at those addresses in private networks.
/// Hostnames are always allowed — DNS is the operator's responsibility.
pub(crate) fn is_private_host(host: &str) -> bool {
    use std::net::IpAddr;
    if let Ok(ip) = host.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(v4) => {
                v4.is_loopback()       // 127.0.0.0/8 — localhost services
                || v4.is_link_local()  // 169.254.0.0/16 — cloud metadata
                || v4.is_broadcast()   // 255.255.255.255
                || v4.is_unspecified() // 0.0.0.0
                || v4.is_multicast() // 224.0.0.0/4
            }
            IpAddr::V6(v6) => {
                // IPv6-mapped IPv4 (::ffff:x.x.x.x) — apply the same IPv4 rules so that
                // e.g. ::ffff:127.0.0.1 and ::ffff:169.254.169.254 are blocked.
                if let Some(v4) = v6.to_ipv4_mapped() {
                    return v4.is_loopback()
                        || v4.is_link_local()
                        || v4.is_broadcast()
                        || v4.is_unspecified()
                        || v4.is_multicast();
                }
                v6.is_loopback()              // ::1
                || v6.is_unspecified()        // ::
                || v6.is_unicast_link_local()  // fe80::/10 — cloud metadata / link-local
                || v6.is_multicast() // ff00::/8
            }
        }
    } else {
        false // hostname — allow (operator controls DNS)
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

        // Background task: drop sessions idle for > 10 minutes (600 s).
        // "default" is never dropped. SSH tunnels are explicitly closed so the
        // subprocess is reaped rather than relying on Drop's non-blocking start_kill().
        let sessions_reaper = sessions.clone();
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
                    if let Some(session) = map.remove(&name) {
                        drop(map); // release lock before awaiting async operations
                        if let Some(tunnel) = session.tunnel {
                            let _ = tunnel.close().await;
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
                    "List all active named database sessions with host, database, and idle time. The default session is omitted when it is the only active session.",
                    mysql_list_sessions_schema(),
                ),
                Tool::new(
                    "mysql_explain_plan",
                    "Get the execution plan for a SELECT query without running it. Returns index_used, rows_examined_estimate, optimization tier, full_table_scan (bool), extra_flags (array of optimizer notes), and note. Use this before executing a potentially expensive query to check efficiency.",
                    mysql_explain_plan_schema(),
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
