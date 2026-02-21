use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;
use rmcp::{
    ServerHandler, ServiceExt,
    model::{
        ServerInfo, ServerCapabilities, Implementation, ProtocolVersion,
        ListToolsResult, CallToolResult, CallToolRequestParams,
        ListResourcesResult, ListResourceTemplatesResult,
        ReadResourceResult, ReadResourceRequestParams,
        ResourceContents, Content, Tool,
        RawResource, PaginatedRequestParams,
        AnnotateAble, ErrorCode,
    },
    service::RequestContext,
    RoleServer,
    ErrorData as McpError,
    transport::stdio,
};
use serde_json::json;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::db::DbPool;
use crate::schema::SchemaIntrospector;

/// A named database session (non-default, runtime-created connection).
struct Session {
    pool: sqlx::MySqlPool,
    introspector: Arc<SchemaIntrospector>,
    last_used: std::time::Instant,
    /// Human-readable display info for mysql_list_sessions
    host: String,
    database: Option<String>,
}

/// Named context returned by get_session(): pool, schema introspector, and optional database.
struct SessionContext {
    pool: sqlx::MySqlPool,
    schema: Arc<SchemaIntrospector>,
    database: Option<String>,
}

fn is_private_host(host: &str) -> bool {
    use std::net::IpAddr;
    // Try to parse as IP; if it's a hostname, allow it (DNS is the operator's responsibility).
    // Block loopback and link-local (169.254.x — cloud metadata endpoint 169.254.169.254).
    // RFC 1918 private ranges (10/8, 172.16/12, 192.168/16) are intentionally ALLOWED:
    // database servers legitimately live at those addresses in private networks.
    if let Ok(ip) = host.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(v4) => {
                v4.is_loopback()           // 127.0.0.0/8 — localhost services
                || v4.is_link_local()      // 169.254.0.0/16 — cloud metadata (169.254.169.254)
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

type IntrospectorKey = (String, u16, Option<String>); // (host, port, database)

pub struct McpServer {
    pub config: Arc<Config>,
    pub db: Arc<DbPool>,
    pub introspector: Arc<SchemaIntrospector>,
    /// Named sessions (does not include "default" which uses self.db/self.introspector).
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    /// Shared introspector registry keyed by (host, port, database).
    introspectors: Arc<tokio::sync::Mutex<std::collections::HashMap<IntrospectorKey, Arc<SchemaIntrospector>>>>,
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

        let mut intr_map = std::collections::HashMap::new();
        let default_key = (
            config.connection.host.clone(),
            config.connection.port,
            config.connection.database.clone(),
        );
        intr_map.insert(default_key, Arc::clone(&introspector));
        let introspectors = Arc::new(tokio::sync::Mutex::new(intr_map));

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

        Self { config, db, introspector, sessions, introspectors }
    }

    pub async fn run(self) -> Result<()> {
        let service = self.serve(stdio()).await?;
        service.waiting().await?;
        Ok(())
    }

    /// Resolve a session name to a SessionContext (pool, introspector, database).
    /// Updates last_used on non-default sessions.
    /// Returns Err with a user-friendly message if not found.
    async fn get_session(
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

    /// Return a shared SchemaIntrospector for (host, port, database), creating one if needed.
    async fn get_or_create_introspector(
        &self,
        host: &str,
        port: u16,
        database: Option<&str>,
        pool: &sqlx::MySqlPool,
    ) -> Arc<SchemaIntrospector> {
        let key = (host.to_string(), port, database.map(str::to_string));
        let mut map = self.introspectors.lock().await;
        if let Some(existing) = map.get(&key) {
            return Arc::clone(existing);
        }
        let pool_arc = Arc::new(pool.clone());
        let new_intr = Arc::new(SchemaIntrospector::new(pool_arc, self.config.pool.cache_ttl_secs));
        map.insert(key, Arc::clone(&new_intr));
        new_intr
    }
}

/// Serialize a JSON value to pretty-printed text and wrap in a successful CallToolResult.
/// Returns a CallToolResult::error on serialization failure.
fn serialize_response(value: &serde_json::Value) -> CallToolResult {
    match serde_json::to_string_pretty(value) {
        Ok(s) => CallToolResult::success(vec![Content::text(s)]),
        Err(e) => {
            tracing::error!("Failed to serialize response: {}", e);
            CallToolResult::error(vec![Content::text(format!(
                "Internal error: failed to serialize response: {}", e
            ))])
        }
    }
}


// ============================================================
// Tool input schemas — defined at module level to keep list_tools() concise.
// Each function builds and returns the JSON schema for its tool.
// ============================================================

fn mysql_query_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SQL query to execute"
            },
            "explain": {
                "type": "boolean",
                "description": "Set to true when investigating a slow query — returns full execution plan including index usage, rows examined, and optimization suggestions. Overrides the server performance_hints setting for this call."
            },
            "session": {
                "type": "string",
                "description": "Named session to route this query to (omit for default connection)"
            }
        },
        "required": ["sql"]
    })))
}

fn mysql_schema_info_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(serde_json::json!({
        "type": "object",
        "properties": {
            "table": { "type": "string", "description": "Table name" },
            "database": { "type": "string", "description": "Database/schema name (optional, uses connected database if omitted)" },
            "include": {
                "type": "array",
                "items": { "type": "string", "enum": ["indexes", "foreign_keys", "size"] },
                "description": "Additional metadata to include. Default: just columns. Options: 'indexes' (all indexes with columns), 'foreign_keys' (FK constraints), 'size' (estimated row count and byte sizes)."
            },
            "session": {
                "type": "string",
                "description": "Named session to use (omit for default connection)"
            }
        },
        "required": ["table"]
    })))
}

fn mysql_server_info_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(serde_json::json!({
        "type": "object",
        "properties": {
            "session": { "type": "string", "description": "Named session to use (omit for default connection)" }
        },
        "required": []
    })))
}

fn mysql_connect_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(serde_json::json!({
        "type": "object",
        "properties": {
            "name": { "type": "string", "description": "Session identifier (alphanumeric, underscore, hyphen; max 64 chars). 'default' is reserved." },
            "host": { "type": "string", "description": "MySQL host (required unless using preset)" },
            "port": { "type": "integer", "description": "MySQL port (default: 3306)." },
            "user": { "type": "string", "description": "MySQL username" },
            "password": { "type": "string", "description": "MySQL password (optional; use empty string for passwordless login)." },
            "database": { "type": "string", "description": "Default database to use" },
            "ssl": { "type": "boolean", "description": "Enable SSL/TLS (default: false). When true and ssl_ca is omitted, uses VerifyIdentity mode (full cert+hostname check)." },
            "ssl_ca": { "type": "string", "description": "Path to PEM CA certificate file for SSL verification. When set, uses VerifyCa mode (validates cert chain without hostname check)." }
        },
        "required": ["name", "host", "user"]
    })))
}

fn mysql_disconnect_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(serde_json::json!({
        "type": "object",
        "properties": {
            "name": { "type": "string", "description": "Session name to disconnect" }
        },
        "required": ["name"]
    })))
}

fn mysql_list_sessions_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(serde_json::json!({
        "type": "object",
        "properties": {},
        "required": []
    })))
}

fn mysql_explain_plan_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SELECT statement to explain. Must be a single SELECT (not INSERT/UPDATE/DDL)."
            },
            "session": {
                "type": "string",
                "description": "Named session to use (default: 'default')."
            }
        },
        "required": ["sql"]
    })))
}

impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::default(),
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
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
            let tool = Tool::new(
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
            );
            let schema_info_tool = Tool::new(
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
            );
            let server_info_tool = Tool::new(
                "mysql_server_info",
                "Get MySQL server metadata: version, current_database, current_user, sql_mode, character_set, collation, time_zone, read_only flag, and which write operations are enabled by server config. Use to understand the environment before writing queries or when diagnosing connection issues.",
                mysql_server_info_schema(),
            );
            let connect_tool = Tool::new(
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
            );
            let disconnect_tool = Tool::new(
                "mysql_disconnect",
                "Explicitly close a named database session. The default session cannot be closed.",
                mysql_disconnect_schema(),
            );
            let list_sessions_tool = Tool::new(
                "mysql_list_sessions",
                "List all active named database sessions with host, database, and idle time. The default session is omitted when it is the only active session.",
                mysql_list_sessions_schema(),
            );
            let explain_plan_tool = Tool::new(
                "mysql_explain_plan",
                "Get the execution plan for a SELECT query without running it. Returns index usage, rows examined estimate, and optimization tier. Use this before executing a potentially expensive query to check efficiency.",
                mysql_explain_plan_schema(),
            );

            Ok(ListToolsResult {
                meta: None,
                tools: vec![
                    tool,
                    schema_info_tool,
                    server_info_tool,
                    connect_tool,
                    disconnect_tool,
                    list_sessions_tool,
                    explain_plan_tool,
                ],
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
            // ------------------------------------------------------------------
            // mysql_connect: create a named session
            // ------------------------------------------------------------------
            if request.name == "mysql_connect" {
                let args = request.arguments.unwrap_or_default();
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
                if is_private_host(&host) {
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
                        let introspector = self.get_or_create_introspector(
                            &host, port, database.as_deref(), &pool,
                        ).await;
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
                        return Ok(serialize_response(&info));
                    }
                    Err(e) => {
                        return Ok(CallToolResult::error(vec![
                            Content::text(format!("Connection failed: {}", e)),
                        ]));
                    }
                }
            }

            // ------------------------------------------------------------------
            // mysql_disconnect: drop a named session
            // ------------------------------------------------------------------
            if request.name == "mysql_disconnect" {
                let args = request.arguments.unwrap_or_default();
                let name = match args.get("name").and_then(|v| v.as_str()) {
                    Some(n) => n.to_string(),
                    None => return Ok(CallToolResult::error(vec![Content::text("Missing required argument: name")])),
                };
                if name == "default" {
                    return Ok(CallToolResult::error(vec![Content::text("The default session cannot be closed")]));
                }
                let mut sessions = self.sessions.lock().await;
                if sessions.remove(&name).is_some() {
                    return Ok(CallToolResult::success(vec![
                        Content::text(format!("Session '{}' closed", name)),
                    ]));
                } else {
                    return Ok(CallToolResult::error(vec![
                        Content::text(format!("Session '{}' not found", name)),
                    ]));
                }
            }

            // ------------------------------------------------------------------
            // mysql_list_sessions: list active sessions
            // ------------------------------------------------------------------
            if request.name == "mysql_list_sessions" {
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

                return Ok(serialize_response(&json!({"sessions": list})));
            }

            if request.name == "mysql_schema_info" {
                let args = request.arguments.clone().unwrap_or_default();
                let table = match args.get("table").and_then(|v: &serde_json::Value| v.as_str()) {
                    Some(t) => t.to_string(),
                    None => return Ok(CallToolResult::error(vec![Content::text("Missing required parameter: table")])),
                };
                let database = args.get("database").and_then(|v: &serde_json::Value| v.as_str()).map(|s| s.to_string());
                let include: Vec<String> = args.get("include")
                    .and_then(|v: &serde_json::Value| v.as_array())
                    .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                    .unwrap_or_default();
                let include_indexes = include.contains(&"indexes".to_string());
                let include_fk = include.contains(&"foreign_keys".to_string());
                let include_size = include.contains(&"size".to_string());

                let session_name = args.get("session").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("default");
                let ctx = match self.get_session(session_name).await {
                    Ok(c) => c,
                    Err(e) => return Ok(CallToolResult::error(vec![Content::text(e)])),
                };

                let result = crate::schema::get_schema_info(
                    &ctx.pool,
                    &table,
                    database.as_deref(),
                    include_indexes,
                    include_fk,
                    include_size,
                ).await;

                return match result {
                    Ok(info) => Ok(serialize_response(&info)),
                    Err(e) => Ok(CallToolResult::error(vec![
                        Content::text(format!("Schema info error for '{}': {}", table, e)),
                    ])),
                };
            }

            if request.name == "mysql_server_info" {
                let args = request.arguments.clone().unwrap_or_default();
                let session_name = args.get("session").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("default");
                let ctx = match self.get_session(session_name).await {
                    Ok(c) => c,
                    Err(e) => return Ok(CallToolResult::error(vec![Content::text(e)])),
                };
                let rows = sqlx::query(
                    "SELECT VERSION() AS mysql_version,
                            CURRENT_USER() AS `current_user`,
                            DATABASE() AS current_database,
                            @@sql_mode AS sql_mode,
                            @@character_set_connection AS character_set,
                            @@collation_connection AS collation,
                            @@time_zone AS time_zone,
                            @@read_only AS read_only"
                ).fetch_all(&ctx.pool).await;

                return match rows {
                    Ok(rows) if !rows.is_empty() => {
                        use sqlx::Row;
                        let row = &rows[0];
                        let version: String = row.try_get("mysql_version").unwrap_or_default();
                        let user: String = row.try_get("current_user").unwrap_or_default();
                        let db: Option<String> = row.try_get("current_database").ok().flatten();
                        let sql_mode: String = row.try_get("sql_mode").unwrap_or_default();
                        let character_set: String = row.try_get("character_set").unwrap_or_default();
                        let collation: String = row.try_get("collation").unwrap_or_default();
                        let time_zone: String = row.try_get("time_zone").unwrap_or_default();
                        let read_only_raw: i64 = row.try_get("read_only").unwrap_or(0);
                        let read_only = read_only_raw != 0;

                        let mut accessible_features = vec!["SELECT", "SHOW", "EXPLAIN"];
                        if self.config.security.allow_insert { accessible_features.push("INSERT"); }
                        if self.config.security.allow_update { accessible_features.push("UPDATE"); }
                        if self.config.security.allow_delete { accessible_features.push("DELETE"); }
                        if self.config.security.allow_ddl { accessible_features.push("DDL (CREATE/ALTER/DROP)"); }

                        let info = serde_json::json!({
                            "mysql_version": version,
                            "current_user": user,
                            "current_database": db,
                            "sql_mode": sql_mode,
                            "character_set": character_set,
                            "collation": collation,
                            "time_zone": time_zone,
                            "read_only": read_only,
                            "accessible_features": accessible_features,
                        });
                        Ok(serialize_response(&info))
                    }
                    Ok(_) => Ok(CallToolResult::error(vec![Content::text("No response from server")])),
                    Err(e) => Ok(CallToolResult::error(vec![Content::text(format!("Server info error: {}", e))])),
                };
            }

            // ------------------------------------------------------------------
            // mysql_explain_plan: EXPLAIN a SELECT without executing it
            // ------------------------------------------------------------------
            if request.name == "mysql_explain_plan" {
                let args = request.arguments.unwrap_or_default();
                let sql = match args.get("sql").and_then(|v| v.as_str()) {
                    Some(s) if !s.is_empty() => s.to_string(),
                    _ => return Ok(CallToolResult::error(vec![Content::text("Missing required argument: sql")])),
                };
                const MAX_SQL_LENGTH: usize = 1_000_000; // 1 MB
                if sql.len() > MAX_SQL_LENGTH {
                    return Ok(CallToolResult::error(vec![
                        Content::text(format!("SQL too large: {} bytes (max {} bytes / 1 MB)", sql.len(), MAX_SQL_LENGTH)),
                    ]));
                }
                let parsed = match crate::sql_parser::parse_sql(&sql) {
                    Ok(p) => p,
                    Err(e) => {
                        return Ok(CallToolResult::error(vec![
                            Content::text(format!("SQL parse error: {}", e)),
                        ]));
                    }
                };
                if parsed.statement_type != crate::sql_parser::StatementType::Select {
                    return Ok(CallToolResult::error(vec![
                        Content::text(format!(
                            "mysql_explain_plan only supports SELECT statements, got: {}",
                            parsed.statement_type.name()
                        )),
                    ]));
                }
                let session_name = args.get("session").and_then(|v| v.as_str()).unwrap_or("default");
                let ctx = match self.get_session(session_name).await {
                    Ok(c) => c,
                    Err(e) => return Ok(CallToolResult::error(vec![Content::text(e)])),
                };
                match crate::query::explain::run_explain(&ctx.pool, &sql).await {
                    Ok(plan) => {
                        let rows = plan.rows_examined_estimate;
                        let full_table_scan = plan.full_table_scan;
                        let tier = if full_table_scan && rows > 10_000 {
                            "very_slow"
                        } else if full_table_scan || rows > 1_000 {
                            "slow"
                        } else {
                            "fast"
                        };
                        let output = json!({
                            "full_table_scan": full_table_scan,
                            "index_used": plan.index_used,
                            "rows_examined_estimate": rows,
                            "filtered_pct": plan.filtered_pct,
                            "extra_flags": plan.extra_flags,
                            "tier": tier,
                            "note": "Execution plan only — query was not executed"
                        });
                        return Ok(serialize_response(&output));
                    }
                    Err(e) => {
                        return Ok(CallToolResult::error(vec![
                            Content::text(format!("EXPLAIN failed: {}", e)),
                        ]));
                    }
                }
            }

            if request.name != "mysql_query" {
                return Err(McpError::new(
                    ErrorCode::METHOD_NOT_FOUND,
                    format!("Unknown tool: {}", request.name),
                    None,
                ));
            }

            let args = request.arguments.unwrap_or_default();
            let sql = match args.get("sql").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => {
                    return Ok(CallToolResult::error(vec![
                        Content::text("Missing required argument: sql"),
                    ]));
                }
            };
            const MAX_SQL_LENGTH: usize = 1_000_000; // 1 MB
            if sql.len() > MAX_SQL_LENGTH {
                return Ok(CallToolResult::error(vec![
                    Content::text(format!("SQL too large: {} bytes (max {} bytes / 1 MB)", sql.len(), MAX_SQL_LENGTH)),
                ]));
            }
            let explain_requested = args.get("explain").and_then(|v| v.as_bool()).unwrap_or(false);
            let effective_hints = if explain_requested {
                "always".to_string()
            } else {
                self.config.pool.performance_hints.clone()
            };

            // Session routing: resolve pool and introspector for this request
            let session_name = args.get("session").and_then(|v| v.as_str()).unwrap_or("default");
            let ctx = match self.get_session(session_name).await {
                Ok(c) => c,
                Err(e) => return Ok(CallToolResult::error(vec![Content::text(e)])),
            };
            let query_pool = ctx.pool;
            let query_introspector = ctx.schema;
            let session_db = ctx.database;

            // Parse and check permissions
            let parsed = match crate::sql_parser::parse_sql(&sql) {
                Ok(p) => p,
                Err(e) => {
                    let sql_preview = if sql.len() <= 120 { sql.as_str() } else { &sql[..120] };
                    return Ok(CallToolResult::error(vec![
                        Content::text(format!(
                            "SQL parse error: {}. Query: {}{}",
                            e,
                            sql_preview,
                            if sql.len() > 120 { "..." } else { "" }
                        )),
                    ]));
                }
            };

            if let Err(e) = crate::permissions::check_permission(
                &self.config,
                &parsed.statement_type,
                parsed.target_schema.as_deref(),
            ) {
                let perm_type = match parsed.statement_type {
                    crate::sql_parser::StatementType::Insert => "INSERT",
                    crate::sql_parser::StatementType::Update => "UPDATE",
                    crate::sql_parser::StatementType::Delete => "DELETE",
                    crate::sql_parser::StatementType::Create
                    | crate::sql_parser::StatementType::Alter
                    | crate::sql_parser::StatementType::Drop
                    | crate::sql_parser::StatementType::Truncate => "DDL",
                    _ => "this operation",
                };
                let schema_hint = parsed.target_schema.as_deref().unwrap_or("(default database)");
                return Ok(CallToolResult::error(vec![
                    Content::text(format!(
                        "{} operation denied on '{}': {}. Check ALLOW_{}_OPERATION env var.",
                        perm_type, schema_hint, e, perm_type.replace(' ', "_")
                    )),
                ]));
            }

            if parsed.statement_type.is_read_only() {
                match crate::query::read::execute_read_query(
                    &query_pool,
                    &sql,
                    &parsed.statement_type,
                    self.config.pool.readonly_transaction,
                    self.config.pool.max_rows,
                    &effective_hints,
                    self.config.pool.slow_query_threshold_ms,
                ).await {
                    Ok(result) => {
                        // Generate schema-aware index suggestions when EXPLAIN detected a full
                        // table scan with no index used.
                        let mut suggestions: Vec<String> = vec![];
                        if let Some(ref plan) = result.plan {
                            let is_full_scan = plan.get("full_table_scan")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let no_index = plan.get("index_used")
                                .map(|v| v.is_null())
                                .unwrap_or(true);
                            if is_full_scan && no_index {
                                let where_cols = crate::sql_parser::extract_where_columns(&parsed);
                                if !where_cols.is_empty() {
                                    if let Some(ref tname) = parsed.target_table {
                                        let db = session_db.as_deref();
                                        if let Ok(indexed_cols) = query_introspector.list_indexed_columns(tname, db).await {
                                            for col in &where_cols {
                                                if !indexed_cols.iter().any(|ic| ic.eq_ignore_ascii_case(col)) {
                                                    suggestions.push(format!(
                                                        "Column `{}` in WHERE clause on table `{}` has no index. Consider: CREATE INDEX idx_{}_{} ON {}({});",
                                                        col, tname, tname, col, tname, col
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Structured performance log (issue mysql-mcp-881)
                        {
                            let sql_truncated = if sql.len() > 200 { &sql[..200] } else { &sql };
                            let plan_tier = result.plan.as_ref()
                                .and_then(|p| p.get("tier"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("none");
                            let full_table_scan = result.plan.as_ref()
                                .and_then(|p| p.get("full_table_scan"))
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let is_slow = result.execution_time_ms >= self.config.pool.slow_query_threshold_ms;
                            if is_slow {
                                tracing::info!(
                                    sql = sql_truncated,
                                    execution_time_ms = result.execution_time_ms,
                                    serialization_time_ms = result.serialization_time_ms,
                                    row_count = result.row_count,
                                    capped = result.capped,
                                    parse_warnings = ?result.parse_warnings,
                                    plan_tier = plan_tier,
                                    full_table_scan = full_table_scan,
                                    suggestions = ?suggestions,
                                    "slow query"
                                );
                            } else {
                                tracing::debug!(
                                    sql = sql_truncated,
                                    execution_time_ms = result.execution_time_ms,
                                    serialization_time_ms = result.serialization_time_ms,
                                    row_count = result.row_count,
                                    capped = result.capped,
                                    parse_warnings = ?result.parse_warnings,
                                    plan_tier = plan_tier,
                                    full_table_scan = full_table_scan,
                                    suggestions = ?suggestions,
                                    "query executed"
                                );
                            }
                        }

                        let mut output = json!({
                            "rows": result.rows,
                            "row_count": result.row_count,
                            "execution_time_ms": result.execution_time_ms,
                            "serialization_time_ms": result.serialization_time_ms,
                        });
                        if result.capped {
                            output["capped"] = json!(true);
                            output["next_offset"] = json!(result.row_count);
                            output["capped_hint"] = json!(format!(
                                "Result truncated to {} rows. Add 'LIMIT {} OFFSET {}' to your query to fetch the next page.",
                                result.row_count, result.row_count, result.row_count
                            ));
                        }
                        output["parse_warnings"] = json!(result.parse_warnings);
                        if let Some(plan) = result.plan {
                            output["plan"] = plan;
                        }
                        if !suggestions.is_empty() {
                            output["suggestions"] = json!(suggestions);
                        }
                        Ok(serialize_response(&output))
                    }
                    Err(e) => Ok(CallToolResult::error(vec![
                        Content::text(format!("Query error: {}", e)),
                    ])),
                }
            } else if parsed.statement_type.is_ddl() {
                match crate::query::write::execute_ddl_query(&query_pool, &sql).await {
                    Ok(result) => {
                        // Invalidate the schema cache so subsequent mysql_schema_info /
                        // list_resources calls reflect the DDL change immediately.
                        match (&parsed.statement_type, &parsed.target_table) {
                            // DROP with no known table name: full invalidation (safe fallback).
                            (crate::sql_parser::StatementType::Drop, None) => {
                                query_introspector.invalidate_all().await;
                            }
                            // Any DDL with a known target table: per-table invalidation.
                            (_, Some(tname)) => {
                                query_introspector.invalidate_table(tname).await;
                            }
                            // Any other DDL (no target_table): safe full invalidation.
                            _ => {
                                query_introspector.invalidate_all().await;
                            }
                        }

                        let mut output = json!({
                            "rows_affected": result.rows_affected,
                            "execution_time_ms": result.execution_time_ms,
                        });
                        if let Some(id) = result.last_insert_id {
                            output["last_insert_id"] = json!(id);
                        }
                        if !result.parse_warnings.is_empty() {
                            output["parse_warnings"] = json!(result.parse_warnings);
                        }
                        Ok(serialize_response(&output))
                    }
                    Err(e) => Ok(CallToolResult::error(vec![
                        Content::text(format!("Query error: {}", e)),
                    ])),
                }
            } else {
                match crate::query::write::execute_write_query(&query_pool, &sql).await {
                    Ok(result) => {
                        let mut output = json!({
                            "rows_affected": result.rows_affected,
                            "execution_time_ms": result.execution_time_ms,
                        });
                        if let Some(id) = result.last_insert_id {
                            output["last_insert_id"] = json!(id);
                        }
                        if !result.parse_warnings.is_empty() {
                            output["parse_warnings"] = json!(result.parse_warnings);
                        }
                        Ok(serialize_response(&output))
                    }
                    Err(e) => Ok(CallToolResult::error(vec![
                        Content::text(format!("Query error: {}", e)),
                    ])),
                }
            }
        }
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourcesResult, McpError>> + Send + '_ {
        async move {
            let database = self.config.connection.database.as_deref();
            let tables = match self.introspector.list_tables(database).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!("Failed to list tables for resources: {}", e);
                    vec![]
                }
            };

            let resources = tables
                .into_iter()
                .map(|t| {
                    RawResource {
                        uri: format!("mysql://tables/{}/{}", t.schema, t.name),
                        name: format!("{}.{}", t.schema, t.name),
                        title: Some(format!("Table: {}.{}", t.schema, t.name)),
                        description: t.row_count.map(|r| format!("~{} rows", r)),
                        mime_type: Some("application/json".to_string()),
                        size: None,
                        icons: None,
                        meta: None,
                    }
                    .no_annotation()
                })
                .collect();

            Ok(ListResourcesResult {
                meta: None,
                resources,
                next_cursor: None,
            })
        }
    }

    fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourceTemplatesResult, McpError>> + Send + '_ {
        async move {
            use rmcp::model::RawResourceTemplate;
            let templates = vec![
                RawResourceTemplate {
                    uri_template: "mysql://tables/{schema}/{table}".to_string(),
                    name: "MySQL Table".to_string(),
                    title: Some("MySQL Table Schema".to_string()),
                    description: Some("Get column information for a specific table".to_string()),
                    mime_type: Some("application/json".to_string()),
                    icons: None,
                }
                .no_annotation(),
            ];
            Ok(ListResourceTemplatesResult {
                meta: None,
                resource_templates: templates,
                next_cursor: None,
            })
        }
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ReadResourceResult, McpError>> + Send + '_ {
        async move {
            let uri = &request.uri;

            // Parse the URI: mysql://tables/{schema}.{table} or mysql://tables/{schema}/{table}
            let (schema, table) = if let Some(rest) = uri.strip_prefix("mysql://tables/") {
                if let Some((s, t)) = rest.split_once('.') {
                    (Some(s.to_string()), t.to_string())
                } else if let Some((s, t)) = rest.split_once('/') {
                    (Some(s.to_string()), t.to_string())
                } else {
                    (None, rest.to_string())
                }
            } else {
                return Err(McpError::new(
                    ErrorCode::INVALID_PARAMS,
                    format!("Unknown resource URI: {}", uri),
                    None,
                ));
            };

            let database = self.config.connection.database.as_deref().or(schema.as_deref());
            let columns = self.introspector.get_columns(&table, database).await
                .map_err(|e| McpError::new(
                    ErrorCode::INTERNAL_ERROR,
                    format!("Failed to get columns: {}", e),
                    None,
                ))?;

            let content_text = serde_json::to_string_pretty(&columns)
                .unwrap_or_else(|e| format!("Serialization error: {}", e));

            Ok(ReadResourceResult {
                contents: vec![ResourceContents::TextResourceContents {
                    uri: uri.clone(),
                    mime_type: Some("application/json".to_string()),
                    text: content_text,
                    meta: None,
                }],
            })
        }
    }
}
