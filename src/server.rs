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

pub struct McpServer {
    pub config: Arc<Config>,
    pub db: Arc<DbPool>,
    pub introspector: Arc<SchemaIntrospector>,
    /// Named sessions (does not include "default" which uses self.db/self.introspector).
    sessions: Arc<Mutex<HashMap<String, Session>>>,
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

        Self { config, db, introspector, sessions }
    }

    pub async fn run(self) -> Result<()> {
        let service = self.serve(stdio()).await?;
        service.waiting().await?;
        Ok(())
    }

    /// Resolve a session name to (pool, introspector, database).
    /// Updates last_used on non-default sessions.
    /// Returns Err with a user-friendly message if not found.
    async fn get_session(
        &self,
        name: &str,
    ) -> std::result::Result<(sqlx::MySqlPool, Arc<SchemaIntrospector>, Option<String>), String> {
        if name == "default" || name.is_empty() {
            return Ok((self.db.pool().clone(), self.introspector.clone(), self.config.connection.database.clone()));
        }
        let mut map = self.sessions.lock().await;
        match map.get_mut(name) {
            Some(session) => {
                session.last_used = std::time::Instant::now();
                Ok((session.pool.clone(), session.introspector.clone(), session.database.clone()))
            }
            None => Err(format!(
                "Session '{}' not found. Use mysql_connect to create it, or omit 'session' to use the default connection.",
                name
            )),
        }
    }
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
            let schema = Arc::new(rmcp::model::object(json!({
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
            })));

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
                schema,
            );

            let multi_schema = Arc::new(rmcp::model::object(json!({
                "type": "object",
                "properties": {
                    "queries": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "SQL statements to execute in parallel. Must all be read-only (SELECT, SHOW, EXPLAIN)."
                    },
                    "session": {
                        "type": "string",
                        "description": "Named session to route these queries to (omit for default connection)"
                    }
                },
                "required": ["queries"]
            })));
            let multi_tool = Tool::new(
                "mysql_multi_query",
                concat!(
                    "Execute 2+ independent read-only SQL queries in parallel (SELECT, SHOW, EXPLAIN only). ",
                    "Saves N-1 round trips vs sequential mysql_query calls. ",
                    "Response: results[] (each with sql, rows, row_count, execution_time_ms, capped, optionally parse_warnings/plan/next_offset/capped_hint) ",
                    "plus wall_time_ms and summary (slowest_query_index, slowest_query_ms, queries_with_full_scans). ",
                    "Check summary first to identify problematic queries before parsing all results.",
                ),
                multi_schema,
            );


            let schema_info_schema = Arc::new(rmcp::model::object(serde_json::json!({
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
            })));
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
                schema_info_schema,
            );

            let server_info_schema = Arc::new(rmcp::model::object(serde_json::json!({
                "type": "object",
                "properties": {
                    "session": { "type": "string", "description": "Named session to use (omit for default connection)" }
                },
                "required": []
            })));
            let server_info_tool = Tool::new(
                "mysql_server_info",
                "Get MySQL server metadata: version, current database, current user, and which write operations are enabled by server config. Use to understand the environment before writing queries or when diagnosing connection issues.",
                server_info_schema,
            );

            let connect_schema = Arc::new(rmcp::model::object(serde_json::json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Session name (e.g. 'staging', 'prod')" },
                    "host": { "type": "string", "description": "MySQL host (required unless using preset)" },
                    "port": { "type": "integer", "description": "MySQL port (default 3306)" },
                    "user": { "type": "string", "description": "MySQL username" },
                    "password": { "type": "string", "description": "MySQL password" },
                    "database": { "type": "string", "description": "Default database to use" },
                    "ssl": { "type": "boolean", "description": "Enable SSL (default false)" },
                    "ssl_ca": { "type": "string", "description": "Path to PEM CA bundle for SSL verification" }
                },
                "required": ["name", "host", "user"]
            })));
            let connect_tool = Tool::new(
                "mysql_connect",
                concat!(
                    "Create a named database session to an additional MySQL server. ",
                    "Requires MYSQL_ALLOW_RUNTIME_CONNECTIONS=true on the server. ",
                    "After connecting, pass 'session' param to other tools to route queries there. ",
                    "Sessions idle for 10 minutes are automatically closed.",
                ),
                connect_schema,
            );

            let disconnect_schema = Arc::new(rmcp::model::object(serde_json::json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "Session name to disconnect" }
                },
                "required": ["name"]
            })));
            let disconnect_tool = Tool::new(
                "mysql_disconnect",
                "Explicitly close a named database session. The default session cannot be closed.",
                disconnect_schema,
            );

            let list_sessions_schema = Arc::new(rmcp::model::object(serde_json::json!({
                "type": "object",
                "properties": {},
                "required": []
            })));
            let list_sessions_tool = Tool::new(
                "mysql_list_sessions",
                "List all active named database sessions with host, database, and idle time. The default session is omitted when it is the only active session.",
                list_sessions_schema,
            );

            Ok(ListToolsResult {
                meta: None,
                tools: vec![
                    tool,
                    multi_tool,
                    schema_info_tool,
                    server_info_tool,
                    connect_tool,
                    disconnect_tool,
                    list_sessions_tool,
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

                if !self.config.security.allow_runtime_connections {
                    return Ok(CallToolResult::error(vec![Content::text(
                        "Runtime connections are disabled. Set MYSQL_ALLOW_RUNTIME_CONNECTIONS=true to enable mysql_connect with raw credentials."
                    )]));
                }

                let host = match args.get("host").and_then(|v| v.as_str()) {
                    Some(h) => h.to_string(),
                    None => return Ok(CallToolResult::error(vec![Content::text("Missing required argument: host")])),
                };
                let port = args.get("port").and_then(|v| v.as_u64()).unwrap_or(3306) as u16;
                let user = match args.get("user").and_then(|v| v.as_str()) {
                    Some(u) => u.to_string(),
                    None => return Ok(CallToolResult::error(vec![Content::text("Missing required argument: user")])),
                };
                let password = args.get("password").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let database = args.get("database").and_then(|v| v.as_str()).map(|s| s.to_string());
                let ssl = args.get("ssl").and_then(|v| v.as_bool()).unwrap_or(false);
                let ssl_ca = args.get("ssl_ca").and_then(|v| v.as_str()).map(|s| s.to_string());

                tracing::debug!(
                    name = %name,
                    host = %host,
                    port = port,
                    user = %user,
                    database = ?database,
                    "mysql_connect: creating session"
                );

                match crate::db::build_session_pool(
                    &host, port, &user, &password,
                    database.as_deref(), ssl, false, ssl_ca.as_deref(),
                    self.config.pool.connect_timeout_ms,
                ).await {
                    Ok(pool) => {
                        let pool_arc = Arc::new(pool.clone());
                        let introspector = Arc::new(SchemaIntrospector::new(
                            pool_arc,
                            self.config.pool.cache_ttl_secs,
                        ));
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
                        return Ok(CallToolResult::success(vec![
                            Content::text(serde_json::to_string_pretty(&info).unwrap_or_default()),
                        ]));
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

                return Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&json!({"sessions": list})).unwrap_or_default()),
                ]));
            }

            if request.name == "mysql_multi_query" {
                let args = request.arguments.unwrap_or_default();
                let queries: Vec<String> = match args.get("queries").and_then(|v| v.as_array()) {
                    Some(arr) => {
                        let mut out = Vec::with_capacity(arr.len());
                        for v in arr {
                            match v.as_str() {
                                Some(s) => out.push(s.to_string()),
                                None => {
                                    return Ok(CallToolResult::error(vec![
                                        Content::text("Each entry in 'queries' must be a string"),
                                    ]));
                                }
                            }
                        }
                        out
                    }
                    None => {
                        return Ok(CallToolResult::error(vec![
                            Content::text("Missing required argument: queries"),
                        ]));
                    }
                };

                const MAX_QUERIES: usize = 100;
                if queries.len() > MAX_QUERIES {
                    return Ok(CallToolResult::error(vec![
                        Content::text(format!("Too many queries: {} (max {})", queries.len(), MAX_QUERIES)),
                    ]));
                }

                // Parse and validate all queries up-front (all must be read-only)
                let mut parsed_queries = Vec::with_capacity(queries.len());
                for sql in &queries {
                    let parsed = match crate::sql_parser::parse_sql(sql) {
                        Ok(p) => p,
                        Err(e) => {
                            return Ok(CallToolResult::error(vec![
                                Content::text(format!("SQL parse error for '{}': {}", sql, e)),
                            ]));
                        }
                    };
                    if let Err(e) = crate::permissions::check_permission(
                        &self.config,
                        &parsed.statement_type,
                        parsed.target_schema.as_deref(),
                    ) {
                        return Ok(CallToolResult::error(vec![
                            Content::text(format!("Permission denied for '{}': {}", sql, e)),
                        ]));
                    }
                    if !parsed.statement_type.is_read_only() {
                        return Ok(CallToolResult::error(vec![
                            Content::text(format!(
                                "Query is not read-only: '{}'. mysql_multi_query only supports SELECT, SHOW, EXPLAIN.",
                                sql
                            )),
                        ]));
                    }
                    parsed_queries.push(parsed);
                }

                let session_name = args.get("session").and_then(|v| v.as_str()).unwrap_or("default");
                let (pool, _introspector, _session_db) = match self.get_session(session_name).await {
                    Ok(pair) => pair,
                    Err(e) => return Ok(CallToolResult::error(vec![Content::text(e)])),
                };
                let readonly_transaction = self.config.pool.readonly_transaction;
                let max_rows = self.config.pool.max_rows;
                let multi_hints = self.config.pool.performance_hints.clone();

                let wall_start = std::time::Instant::now();
                let mut join_set = tokio::task::JoinSet::new();

                for (sql, parsed) in queries.iter().zip(parsed_queries.iter()) {
                    let pool_clone = pool.clone();
                    let sql_clone = sql.clone();
                    let stmt_type = parsed.statement_type.clone();
                    let hints_clone = multi_hints.clone();
                    join_set.spawn(async move {
                        let result = crate::query::read::execute_read_query(
                            &pool_clone,
                            &sql_clone,
                            &stmt_type,
                            readonly_transaction,
                            max_rows,
                            &hints_clone,
                            0,
                        ).await;
                        (sql_clone, result)
                    });
                }

                let mut results: Vec<(String, std::result::Result<crate::query::read::QueryResult, anyhow::Error>)> = Vec::new();
                while let Some(join_result) = join_set.join_next().await {
                    match join_result {
                        Ok(pair) => results.push(pair),
                        Err(e) => results.push(("(unknown)".to_string(), Err(anyhow::anyhow!("Task panicked: {}", e)))),
                    }
                }

                let wall_time_ms = wall_start.elapsed().as_millis() as u64;

                // Re-order results to match the original query order
                let mut ordered: Vec<serde_json::Value> = queries.iter().map(|_| serde_json::Value::Null).collect();
                for (sql, result) in results {
                    let idx = queries.iter().position(|q| q == &sql).unwrap_or(0);
                    ordered[idx] = match result {
                        Ok(r) => {
                            let mut entry = json!({
                                "sql": sql,
                                "rows": r.rows,
                                "row_count": r.row_count,
                                "execution_time_ms": r.execution_time_ms,
                                "serialization_time_ms": r.serialization_time_ms,
                            });
                            if r.capped {
                                entry["capped"] = json!(true);
                            }
                            if !r.parse_warnings.is_empty() {
                                entry["parse_warnings"] = json!(r.parse_warnings);
                            }
                            if let Some(plan) = r.plan {
                                entry["plan"] = plan;
                            }
                            entry
                        }
                        Err(e) => {
                            json!({
                                "sql": sql,
                                "error": e.to_string(),
                            })
                        }
                    };
                }

                // Structured performance log for multi-query (issue mysql-mcp-881)
                {
                    let slow_threshold = self.config.pool.slow_query_threshold_ms;
                    // Determine if any individual query was slow (or wall_time is slow)
                    let any_slow = ordered.iter().any(|entry| {
                        entry.get("execution_time_ms")
                            .and_then(|v| v.as_u64())
                            .map(|ms| ms >= slow_threshold)
                            .unwrap_or(false)
                    });
                    let per_query: Vec<_> = queries.iter().zip(ordered.iter()).map(|(sql, entry)| {
                        let sql_truncated = if sql.len() > 200 { &sql[..200] } else { sql.as_str() };
                        json!({
                            "sql": sql_truncated,
                            "execution_time_ms": entry.get("execution_time_ms"),
                            "row_count": entry.get("row_count"),
                        })
                    }).collect();
                    if any_slow {
                        tracing::info!(
                            wall_time_ms = wall_time_ms,
                            queries = ?per_query,
                            "slow multi-query"
                        );
                    } else {
                        tracing::debug!(
                            wall_time_ms = wall_time_ms,
                            queries = ?per_query,
                            "multi-query executed"
                        );
                    }
                }

                let slowest_idx = ordered.iter()
                    .enumerate()
                    .filter_map(|(i, entry)| {
                        entry.get("execution_time_ms")
                            .and_then(|v| v.as_u64())
                            .map(|ms| (i, ms))
                    })
                    .max_by_key(|(_, ms)| *ms);
                let queries_with_full_scans = ordered.iter()
                    .filter(|entry| {
                        entry.get("plan")
                            .and_then(|p| p.get("full_table_scan"))
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    })
                    .count();
                let summary = json!({
                    "total_queries": ordered.len(),
                    "slowest_query_index": slowest_idx.map(|(i, _)| i),
                    "slowest_execution_time_ms": slowest_idx.map(|(_, ms)| ms),
                    "queries_with_full_scans": queries_with_full_scans,
                });
                let output = json!({
                    "results": ordered,
                    "summary": summary,
                    "wall_time_ms": wall_time_ms,
                });
                return Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&output).unwrap_or_default()),
                ]));
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
                let (session_pool, _introspector, _session_db) = match self.get_session(session_name).await {
                    Ok(pair) => pair,
                    Err(e) => return Ok(CallToolResult::error(vec![Content::text(e)])),
                };

                let result = crate::schema::get_schema_info(
                    &session_pool,
                    &table,
                    database.as_deref(),
                    include_indexes,
                    include_fk,
                    include_size,
                ).await;

                return match result {
                    Ok(info) => Ok(CallToolResult::success(vec![
                        Content::text(serde_json::to_string_pretty(&info).unwrap_or_default()),
                    ])),
                    Err(e) => Ok(CallToolResult::error(vec![
                        Content::text(format!("Schema info error for '{}': {}", table, e)),
                    ])),
                };
            }

            if request.name == "mysql_server_info" {
                let args = request.arguments.clone().unwrap_or_default();
                let session_name = args.get("session").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("default");
                let (session_pool, _introspector, _session_db) = match self.get_session(session_name).await {
                    Ok(pair) => pair,
                    Err(e) => return Ok(CallToolResult::error(vec![Content::text(e)])),
                };
                let rows = sqlx::query(
                    "SELECT VERSION() AS mysql_version, CURRENT_USER() AS db_user, DATABASE() AS db_name"
                ).fetch_all(&session_pool).await;

                return match rows {
                    Ok(rows) if !rows.is_empty() => {
                        use sqlx::Row;
                        let row = &rows[0];
                        let version: String = row.try_get("mysql_version").unwrap_or_default();
                        let user: String = row.try_get("db_user").unwrap_or_default();
                        let db: Option<String> = row.try_get("db_name").ok().flatten();

                        let mut accessible_features = vec!["SELECT", "SHOW", "EXPLAIN"];
                        if self.config.security.allow_insert { accessible_features.push("INSERT"); }
                        if self.config.security.allow_update { accessible_features.push("UPDATE"); }
                        if self.config.security.allow_delete { accessible_features.push("DELETE"); }
                        if self.config.security.allow_ddl { accessible_features.push("DDL (CREATE/ALTER/DROP)"); }

                        let info = serde_json::json!({
                            "mysql_version": version,
                            "current_user": user,
                            "current_database": db,
                            "accessible_features": accessible_features,
                        });
                        Ok(CallToolResult::success(vec![
                            Content::text(serde_json::to_string_pretty(&info).unwrap_or_default()),
                        ]))
                    }
                    Ok(_) => Ok(CallToolResult::error(vec![Content::text("No response from server")])),
                    Err(e) => Ok(CallToolResult::error(vec![Content::text(format!("Server info error: {}", e))])),
                };
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
            let (query_pool, query_introspector, session_db) = match self.get_session(session_name).await {
                Ok(pair) => pair,
                Err(e) => return Ok(CallToolResult::error(vec![Content::text(e)])),
            };

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
                        Ok(CallToolResult::success(vec![
                            Content::text(serde_json::to_string_pretty(&output).unwrap_or_default()),
                        ]))
                    }
                    Err(e) => Ok(CallToolResult::error(vec![
                        Content::text(format!("Query error: {}", e)),
                    ])),
                }
            } else if parsed.statement_type.is_ddl() {
                match crate::query::write::execute_ddl_query(&query_pool, &sql).await {
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
                        Ok(CallToolResult::success(vec![
                            Content::text(serde_json::to_string_pretty(&output).unwrap_or_default()),
                        ]))
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
                        Ok(CallToolResult::success(vec![
                            Content::text(serde_json::to_string_pretty(&output).unwrap_or_default()),
                        ]))
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
