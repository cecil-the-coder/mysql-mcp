use rmcp::model::{CallToolResult, Content};
use serde_json::json;

use super::sessions::SessionStore;
use super::tool_schemas::serialize_response;

/// Maximum SQL statement length (1 MB). Enforced in both mysql_query and mysql_explain_plan.
const MAX_SQL_LEN: usize = 1_000_000;

/// Check that a SQL string is within the allowed length limit.
/// Returns `Err(CallToolResult)` with an error message when the limit is exceeded.
fn check_sql_length(sql: &str) -> Result<(), CallToolResult> {
    if sql.len() > MAX_SQL_LEN {
        Err(CallToolResult::error(vec![Content::text(format!(
            "SQL too large: {} bytes (max {} bytes / 1 MB)",
            sql.len(),
            MAX_SQL_LEN
        ))]))
    } else {
        Ok(())
    }
}

impl SessionStore {
    // ------------------------------------------------------------------
    // Tool handler: mysql_schema_info
    // ------------------------------------------------------------------
    pub(crate) async fn handle_schema_info(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let table = match args.get("table").and_then(|v: &serde_json::Value| v.as_str()) {
            Some(t) => t.to_string(),
            None => return Ok(CallToolResult::error(vec![Content::text("Missing required parameter: table")])),
        };
        let database = args
            .get("database")
            .and_then(|v: &serde_json::Value| v.as_str())
            .map(|s| s.to_string());
        let include: Vec<String> = args
            .get("include")
            .and_then(|v: &serde_json::Value| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        let include_indexes = include.iter().any(|s| s == "indexes");
        let include_fk = include.iter().any(|s| s == "foreign_keys");
        let include_size = include.iter().any(|s| s == "size");

        let ctx = match self.resolve_session(&args).await {
            Ok(c) => c,
            Err(e) => return Ok(e),
        };

        match ctx.schema.get_schema_info(
            &table,
            database.as_deref(),
            include_indexes,
            include_fk,
            include_size,
        ).await {
            Ok(info) => Ok(serialize_response(&info)),
            Err(e) => Ok(CallToolResult::error(vec![
                Content::text(format!("Schema info error for '{}': {}", table, e)),
            ])),
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: mysql_server_info
    // ------------------------------------------------------------------
    pub(crate) async fn handle_server_info(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let ctx = match self.resolve_session(&args).await {
            Ok(c) => c,
            Err(e) => return Ok(e),
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

        match rows {
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
                let sec = &self.config.security;
                for (enabled, name) in [
                    (sec.allow_insert, "INSERT"),
                    (sec.allow_update, "UPDATE"),
                    (sec.allow_delete, "DELETE"),
                    (sec.allow_ddl, "DDL (CREATE/ALTER/DROP)"),
                ] {
                    if enabled { accessible_features.push(name); }
                }

                let info = json!({
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
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: mysql_explain_plan
    // ------------------------------------------------------------------
    pub(crate) async fn handle_explain_plan(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let sql = match args.get("sql").and_then(|v| v.as_str()) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return Ok(CallToolResult::error(vec![Content::text("Missing required argument: sql")])),
        };
        if let Err(e) = check_sql_length(&sql) {
            return Ok(e);
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

        let ctx = match self.resolve_session(&args).await {
            Ok(c) => c,
            Err(e) => return Ok(e),
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
                    "extra_flags": plan.extra_flags,
                    "tier": tier,
                    "note": "Execution plan only — query was not executed"
                });
                Ok(serialize_response(&output))
            }
            Err(e) => {
                Ok(CallToolResult::error(vec![
                    Content::text(format!("EXPLAIN failed: {}", e)),
                ]))
            }
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: mysql_query
    // ------------------------------------------------------------------
    pub(crate) async fn handle_query(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let sql = match args.get("sql").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => {
                return Ok(CallToolResult::error(vec![
                    Content::text("Missing required argument: sql"),
                ]));
            }
        };
        if let Err(e) = check_sql_length(&sql) {
            return Ok(e);
        }

        let explain_requested = args.get("explain").and_then(|v| v.as_bool()).unwrap_or(false);
        let effective_hints = if explain_requested {
            "always".to_string()
        } else {
            self.config.pool.performance_hints.clone()
        };

        // Session routing: resolve pool and introspector for this request
        let ctx = match self.resolve_session(&args).await {
            Ok(c) => c,
            Err(e) => return Ok(e),
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
                &parsed,
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
                            if let Some(ref tname) = parsed.target_table {
                                let where_cols = parsed.where_columns.clone();
                                if !where_cols.is_empty() {
                                    suggestions = query_introspector.generate_index_suggestions(
                                        tname,
                                        session_db.as_deref(),
                                        &where_cols,
                                    ).await;
                                }
                            }
                        }
                    }

                    // Structured performance log
                    log_query_result(&sql, &result, &suggestions, self.config.pool.slow_query_threshold_ms);

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

                    Ok(serialize_response(&write_result_content(&result)))
                }
                Err(e) => Ok(CallToolResult::error(vec![
                    Content::text(format!("Query error: {}", e)),
                ])),
            }
        } else {
            match crate::query::write::execute_write_query(&query_pool, &sql, &parsed).await {
                Ok(result) => {
                    Ok(serialize_response(&write_result_content(&result)))
                }
                Err(e) => Ok(CallToolResult::error(vec![
                    Content::text(format!("Query error: {}", e)),
                ])),
            }
        }
    }
}

fn log_query_result(
    sql: &str,
    result: &crate::query::read::QueryResult,
    suggestions: &[String],
    slow_threshold_ms: u64,
) {
    let sql_truncated = if sql.len() > 200 { &sql[..200] } else { sql };
    let plan_tier = result.plan.as_ref()
        .and_then(|p| p.get("tier"))
        .and_then(|v| v.as_str())
        .unwrap_or("none");
    let full_table_scan = result.plan.as_ref()
        .and_then(|p| p.get("full_table_scan"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_slow = result.execution_time_ms >= slow_threshold_ms;
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

fn write_result_content(result: &crate::query::write::WriteResult) -> serde_json::Value {
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
    output
}
