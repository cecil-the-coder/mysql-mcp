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
                    }
                },
                "required": ["sql"]
            })));

            let tool = Tool::new(
                "mysql_query",
                "Execute a SQL query against the MySQL database. Supports SELECT, SHOW, EXPLAIN, and (if configured) INSERT, UPDATE, DELETE, DDL statements.",
                schema,
            );

            Ok(ListToolsResult {
                meta: None,
                tools: vec![tool],
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
                ).await {
                    Ok(result) => {
                        let mut output = json!({
                            "rows": result.rows,
                            "row_count": result.row_count,
                            "execution_time_ms": result.execution_time_ms,
                            "serialization_time_ms": result.serialization_time_ms,
                        });
                        if result.capped {
                            output["capped"] = json!(true);
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
