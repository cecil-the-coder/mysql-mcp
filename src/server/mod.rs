use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;
use rmcp::{
    ServerHandler, ServiceExt,
    model::{
        ServerInfo, ServerCapabilities, Implementation, ProtocolVersion,
        ListToolsResult, CallToolResult, CallToolRequestParams,
        Tool, PaginatedRequestParams,
        ErrorCode,
    },
    service::RequestContext,
    RoleServer,
    ErrorData as McpError,
    transport::stdio,
};
use tokio::sync::Mutex;

use crate::config::Config;
use crate::db::DbPool;
use crate::schema::SchemaIntrospector;

mod tool_schemas;
mod sessions;
mod handlers;

use sessions::{Session, SessionStore};
use tool_schemas::*;

/// Check whether a host string resolves to a blocked address category.
///
/// Blocks loopback and link-local (cloud metadata endpoint 169.254.169.254).
/// RFC 1918 private ranges (10/8, 172.16/12, 192.168/16) are intentionally ALLOWED:
/// database servers legitimately live at those addresses in private networks.
/// Hostnames are always allowed — DNS is the operator's responsibility.
pub(crate) fn is_private_host(host: &str) -> bool {
    use std::net::IpAddr;
    if let Ok(ip) = host.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(v4) => {
                v4.is_loopback()           // 127.0.0.0/8 — localhost services
                || v4.is_link_local()      // 169.254.0.0/16 — cloud metadata
                || v4.is_broadcast()       // 255.255.255.255
                || v4.is_unspecified()     // 0.0.0.0
            }
            IpAddr::V6(v6) => {
                v6.is_loopback()           // ::1
                || v6.is_unspecified()     // ::
            }
        }
    } else {
        false // hostname — allow (operator controls DNS)
    }
}

pub struct McpServer {
    pub config: Arc<Config>,
    pub db: Arc<DbPool>,
    pub introspector: Arc<SchemaIntrospector>,
    store: SessionStore,
}

impl McpServer {
    pub fn new(config: Arc<Config>, db: Arc<DbPool>) -> Self {
        let pool = Arc::new(db.pool().clone());
        let introspector = Arc::new(SchemaIntrospector::new(
            pool,
            config.pool.cache_ttl_secs,
        ));
        let sessions: Arc<Mutex<HashMap<String, Session>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Background task: drop sessions idle for > 10 minutes (600 s).
        // "default" is never dropped.
        let sessions_reaper = sessions.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                let mut map = sessions_reaper.lock().await;
                map.retain(|_name, session| {
                    session.last_used.elapsed().as_secs() < 600
                });
            }
        });

        let store = SessionStore {
            sessions,
            config: config.clone(),
            db: db.clone(),
            introspector: introspector.clone(),
        };

        Self { config, db, introspector, store }
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
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .build(),
            server_info: Implementation {
                name: "mysql-mcp".to_string(),
                title: Some("MySQL MCP Server".to_string()),
                version: env!("CARGO_PKG_VERSION").to_string(),
                description: Some("Expose MySQL databases via the Model Context Protocol".to_string()),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Use mysql_query to execute SQL queries against the connected MySQL database.".to_string()
            ),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        async move {
            let tools = vec![
                Tool::new(
                    "mysql_query",
                    concat!(
                        "Execute a SQL query against MySQL. ",
                        "Always returned: rows, row_count, execution_time_ms, serialization_time_ms, parse_warnings. ",
                        "Optional: plan (only when explain:true or server auto-triggers for slow queries), ",
                        "capped+next_offset+capped_hint (only when result was truncated to max_rows limit), ",
                        "suggestions (only when a full table scan is detected). ",
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
                    "Get MySQL server metadata: version, current_database, current_user, sql_mode, character_set, collation, time_zone, read_only flag, and which write operations are enabled by server config. Use to understand the environment before writing queries or when diagnosing connection issues.",
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
                    "Get the execution plan for a SELECT query without running it. Returns index usage, rows examined estimate, and optimization tier. Use this before executing a potentially expensive query to check efficiency.",
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

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        async move {
            let args = request.arguments.unwrap_or_default();
            match request.name.as_ref() {
                "mysql_connect"       => self.store.handle_connect(args).await,
                "mysql_disconnect"    => self.store.handle_disconnect(args).await,
                "mysql_list_sessions" => self.store.handle_list_sessions(args).await,
                "mysql_schema_info"   => self.store.handle_schema_info(args).await,
                "mysql_server_info"   => self.store.handle_server_info(args).await,
                "mysql_explain_plan"  => self.store.handle_explain_plan(args).await,
                "mysql_query"         => self.store.handle_query(args).await,
                name => Err(McpError::new(
                    ErrorCode::METHOD_NOT_FOUND,
                    format!("Unknown tool: {}", name),
                    None,
                )),
            }
        }
    }
}
