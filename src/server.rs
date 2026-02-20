use std::sync::Arc;
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

use crate::config::Config;
use crate::db::DbPool;
use crate::schema::SchemaIntrospector;

pub struct McpServer {
    pub config: Arc<Config>,
    pub db: Arc<DbPool>,
    pub introspector: Arc<SchemaIntrospector>,
}

impl McpServer {
    pub fn new(config: Arc<Config>, db: Arc<DbPool>) -> Self {
        let pool = Arc::new(db.pool().clone());
        let introspector = Arc::new(SchemaIntrospector::new(
            pool,
            config.pool.cache_ttl_secs,
        ));
        Self { config, db, introspector }
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
                        "description": "Request EXPLAIN analysis for this query (overrides server performance_hints setting)"
                    }
                },
                "required": ["sql"]
            })));

            let tool = Tool::new(
                "mysql_query",
                "Execute a SQL query against the MySQL database. Supports SELECT, SHOW, EXPLAIN, and (if configured) INSERT, UPDATE, DELETE, DDL statements.",
                schema,
            );

            let multi_schema = Arc::new(rmcp::model::object(json!({
                "type": "object",
                "properties": {
                    "queries": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "SQL statements to execute in parallel. Must all be read-only (SELECT, SHOW, EXPLAIN)."
                    }
                },
                "required": ["queries"]
            })));
            let multi_tool = Tool::new(
                "mysql_multi_query",
                "Execute multiple read-only SQL queries in parallel and return all results together. Use this when you need data from multiple independent tables or queries — it completes in ~1 RTT instead of N sequential RTTs.",
                multi_schema,
            );

            Ok(ListToolsResult {
                meta: None,
                tools: vec![tool, multi_tool],
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

                let pool = self.db.pool().clone();
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
                                "capped": r.capped,
                            });
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

                let output = json!({
                    "results": ordered,
                    "wall_time_ms": wall_time_ms,
                });
                return Ok(CallToolResult::success(vec![
                    Content::text(serde_json::to_string_pretty(&output).unwrap_or_default()),
                ]));
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
            let explain_requested = args.get("explain").and_then(|v| v.as_bool()).unwrap_or(false);
            let effective_hints = if explain_requested {
                "always".to_string()
            } else {
                self.config.pool.performance_hints.clone()
            };

            // Parse and check permissions
            let parsed = match crate::sql_parser::parse_sql(&sql) {
                Ok(p) => p,
                Err(e) => {
                    return Ok(CallToolResult::error(vec![
                        Content::text(format!("SQL parse error: {}", e)),
                    ]));
                }
            };

            if let Err(e) = crate::permissions::check_permission(
                &self.config,
                &parsed.statement_type,
                parsed.target_schema.as_deref(),
            ) {
                return Ok(CallToolResult::error(vec![
                    Content::text(format!("Permission denied: {}", e)),
                ]));
            }

            let pool = self.db.pool();

            if parsed.statement_type.is_read_only() {
                match crate::query::read::execute_read_query(
                    pool,
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
                                        let db = self.config.connection.database.as_deref();
                                        if let Ok(indexed_cols) = self.introspector.list_indexed_columns(tname, db).await {
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

                        let mut output = json!({
                            "rows": result.rows,
                            "row_count": result.row_count,
                            "execution_time_ms": result.execution_time_ms,
                            "serialization_time_ms": result.serialization_time_ms,
                        });
                        if result.capped {
                            output["capped"] = json!(true);
                        }
                        if !result.parse_warnings.is_empty() {
                            output["parse_warnings"] = json!(result.parse_warnings);
                        }
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
                match crate::query::write::execute_ddl_query(pool, &sql).await {
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
                match crate::query::write::execute_write_query(pool, &sql).await {
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
