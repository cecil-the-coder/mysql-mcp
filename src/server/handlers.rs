use rmcp::model::CallToolResult;
use serde_json::json;

use super::sessions::{validate_identifier, SessionStore};
use super::tool_schemas::serialize_response;
use crate::tool_error;

/// Maximum SQL statement length (1 MB). Enforced in both query and explain_plan.
const MAX_SQL_LEN: usize = 1_000_000;
/// Number of SQL characters shown in parse-error messages to give context without flooding output.
const SQL_ERROR_PREVIEW_LEN: usize = 120;

/// Check that a SQL string is within the allowed length limit.
/// Returns `Err(CallToolResult)` with an error message when the limit is exceeded.
fn check_sql_length(sql: &str) -> Result<(), CallToolResult> {
    if sql.is_empty() {
        return Err(crate::server::error::error_response("SQL cannot be empty"));
    }
    if sql.len() > MAX_SQL_LEN {
        Err(crate::server::error::error_response(format!(
            "SQL too large: {} bytes (max {} bytes / 1 MB)",
            sql.len(),
            MAX_SQL_LEN
        )))
    } else {
        Ok(())
    }
}

impl SessionStore {
    // ------------------------------------------------------------------
    // Tool handler: schema_info
    // ------------------------------------------------------------------
    pub(crate) async fn handle_schema_info(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let table = match args
            .get("table")
            .and_then(|v: &serde_json::Value| v.as_str())
        {
            Some(t) if !t.trim().is_empty() => t.to_string(),
            Some(_) => return tool_error!("Table name cannot be empty"),
            None => return tool_error!("Missing required argument: table"),
        };
        if let Err(e) = validate_identifier(&table, "Table name") {
            return Ok(e);
        }
        let database = args
            .get("database")
            .and_then(|v: &serde_json::Value| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if let Some(ref db) = database {
            if let Err(e) = validate_identifier(db, "Database name") {
                return Ok(e);
            }
        }
        let include_arr = args.get("include").and_then(|v| v.as_array());
        if let Some(arr) = include_arr {
            const VALID: &[&str] = &["indexes", "foreign_keys", "size"];
            for (i, elem) in arr.iter().enumerate() {
                match elem.as_str() {
                    Some(s) if VALID.contains(&s) => {}
                    Some(s) => {
                        return tool_error!(
                            "include[{}]: unrecognized value '{}'. Valid values: indexes, foreign_keys, size",
                            i, s
                        );
                    }
                    None => {
                        return tool_error!("include[{}]: expected a string, got {}", i, elem);
                    }
                }
            }
        }
        let include_indexes =
            include_arr.is_some_and(|a| a.iter().any(|v| v.as_str() == Some("indexes")));
        let include_fk =
            include_arr.is_some_and(|a| a.iter().any(|v| v.as_str() == Some("foreign_keys")));
        let include_size =
            include_arr.is_some_and(|a| a.iter().any(|v| v.as_str() == Some("size")));

        let ctx = match self.resolve_session(&args).await {
            Ok(c) => c,
            Err(e) => return Ok(e),
        };

        match ctx
            .schema
            .get_schema_info(
                &table,
                database.as_deref(),
                include_indexes,
                include_fk,
                include_size,
            )
            .await
        {
            Ok(info) => Ok(serialize_response(&info)),
            Err(e) => tool_error!("Schema info error for '{}': {}", table, e),
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: server_info
    // ------------------------------------------------------------------
    pub(crate) async fn handle_server_info(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let ctx = match self.resolve_session(&args).await {
            Ok(c) => c,
            Err(e) => return Ok(e),
        };

        match self
            .backend
            .fetch_server_info(&ctx.pool, &self.config.security)
            .await
        {
            Ok(info) => Ok(serialize_response(&info)),
            Err(e) => tool_error!("Server info error: {}", e),
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: list_tables
    // ------------------------------------------------------------------
    pub(crate) async fn handle_list_tables(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let database = args
            .get("database")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if let Some(ref db) = database {
            if let Err(e) = validate_identifier(db, "Database name") {
                return Ok(e);
            }
        }

        let ctx = match self.resolve_session(&args).await {
            Ok(c) => c,
            Err(e) => return Ok(e),
        };

        let Some(target_db) = database.as_deref().or(ctx.database.as_deref()) else {
            return tool_error!("No database specified and no default database for this session");
        };

        match self.backend.fetch_list_tables(&ctx.pool, target_db).await {
            Ok(tables) => {
                let output = json!({
                    "tables": tables,
                    "database": target_db,
                });
                Ok(serialize_response(&output))
            }
            Err(e) => tool_error!("Failed to list tables: {}", e),
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: explain_plan
    // ------------------------------------------------------------------
    pub(crate) async fn handle_explain_plan(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let sql = match args.get("sql").and_then(|v| v.as_str()) {
            Some(s) if !s.trim().is_empty() => s.to_string(),
            _ => return tool_error!("Missing required argument: sql"),
        };
        if sql.contains('\0') {
            return tool_error!("SQL contains NUL bytes, which are not valid in SQL statements");
        }
        if let Err(e) = check_sql_length(&sql) {
            return Ok(e);
        }
        let parsed = match crate::sql_parser::parse_sql(&sql, self.backend.sql_dialect()) {
            Ok(p) => p,
            Err(e) => {
                return tool_error!("SQL parse error: {}", e);
            }
        };
        if parsed.statement_type != crate::sql_parser::StatementType::Select {
            return tool_error!(
                "explain_plan only supports SELECT statements, got: {}",
                parsed.statement_type.name()
            );
        }

        let ctx = match self.resolve_session(&args).await {
            Ok(c) => c,
            Err(e) => return Ok(e),
        };

        let explain_start = std::time::Instant::now();
        match self
            .backend
            .run_explain(&ctx.pool, &sql, self.config.pool.query_timeout_ms)
            .await
        {
            Ok(plan) => {
                let elapsed = explain_start.elapsed().as_millis() as u64;
                let output = json!({
                    "full_table_scan": plan.full_table_scan,
                    "index_used": plan.index_used,
                    "rows_examined_estimate": plan.rows_examined_estimate,
                    "extra_flags": plan.extra_flags,
                    "tier": plan.tier,
                    "execution_time_ms": elapsed,
                    "note": "Execution plan only — query was not executed"
                });
                Ok(serialize_response(&output))
            }
            Err(e) => tool_error!("EXPLAIN failed: {}", e),
        }
    }

    // ------------------------------------------------------------------
    // Tool handler: query
    // ------------------------------------------------------------------
    pub(crate) async fn handle_query(
        &self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult, rmcp::ErrorData> {
        let sql = match args.get("sql").and_then(|v| v.as_str()) {
            Some(s) if !s.trim().is_empty() => s.to_string(),
            _ => return tool_error!("Missing required argument: sql"),
        };
        if sql.contains('\0') {
            return tool_error!("SQL contains NUL bytes, which are not valid in SQL statements");
        }
        if let Err(e) = check_sql_length(&sql) {
            return Ok(e);
        }

        // Session routing: resolve pool and introspector for this request
        let ctx = match self.resolve_session(&args).await {
            Ok(c) => c,
            Err(e) => return Ok(e),
        };
        let query_pool = ctx.pool.clone();
        let query_introspector = ctx.schema.clone();
        let session_db = ctx.database.clone();

        // Parse and check permissions
        let parsed = match crate::sql_parser::parse_sql(&sql, self.backend.sql_dialect()) {
            Ok(p) => p,
            Err(e) => {
                let cut = sql.len() > SQL_ERROR_PREVIEW_LEN;
                let mut preview_end = SQL_ERROR_PREVIEW_LEN.min(sql.len());
                while preview_end > 0 && !sql.is_char_boundary(preview_end) {
                    preview_end -= 1;
                }
                return tool_error!(
                    "SQL parse error: {}. Query: {}{}",
                    e,
                    &sql[..preview_end],
                    if cut { "..." } else { "" }
                );
            }
        };

        if let Err(e) = crate::permissions::check_all_permissions(&self.config, &parsed) {
            let perm_type = parsed.statement_type.permission_category();
            if perm_type == "this operation" {
                return tool_error!("{}", e);
            }
            let schema_hint = parsed
                .target_schema
                .as_deref()
                .unwrap_or("(default database)");
            return tool_error!(
                "{} operation denied on '{}': {}. Set DB_ALLOW_{} env var to enable.",
                perm_type,
                schema_hint,
                e,
                perm_type
            );
        }

        if parsed.statement_type.is_read_only() {
            match crate::query::read::execute_read_query_pool(
                &query_pool,
                &sql,
                &parsed,
                &self.config.pool,
            )
            .await
            {
                Ok(result) => {
                    // Run EXPLAIN via the backend for performance hints
                    let (plan, explain_error) = self
                        .maybe_run_explain(&query_pool, &sql, &parsed, &self.config.pool, &result)
                        .await;

                    // Generate schema-aware index suggestions
                    let mut suggestions: Vec<String> = vec![];
                    let needs_suggestions = plan.as_ref().is_some_and(|p| {
                        p.get("full_table_scan")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                            && p.get("index_used").map(|v| v.is_null()).unwrap_or(true)
                    }) && parsed.target_table.is_some()
                        && !parsed.where_columns.is_empty();
                    if needs_suggestions {
                        let tname = parsed
                            .target_table
                            .as_deref()
                            .expect("target_table checked for Some above");
                        suggestions = query_introspector
                            .generate_index_suggestions(
                                tname,
                                session_db.as_deref(),
                                &parsed.where_columns,
                            )
                            .await;
                    }

                    log_query_result(
                        &sql,
                        &result,
                        &suggestions,
                        self.config.pool.slow_query_threshold_ms,
                    );

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
                    if !result.parse_warnings.is_empty() {
                        output["parse_warnings"] = json!(result.parse_warnings);
                    }
                    if let Some(plan) = plan {
                        output["plan"] = plan;
                    }
                    if let Some(ref explain_error) = explain_error {
                        output["explain_error"] = json!(explain_error);
                    }
                    if !suggestions.is_empty() {
                        output["suggestions"] = json!(suggestions);
                    }
                    Ok(serialize_response(&output))
                }
                Err(e) => tool_error!("Query error: {}", e),
            }
        } else if parsed.statement_type.is_ddl() {
            match crate::query::write::execute_ddl_query_pool(
                &query_pool,
                &sql,
                self.config.pool.query_timeout_ms,
                self.config.pool.retry_attempts,
            )
            .await
            {
                Ok(mut result) => {
                    if let Some(tname) = &parsed.target_table {
                        query_introspector
                            .invalidate_table(tname, parsed.target_schema.as_deref())
                            .await;
                    } else {
                        query_introspector.invalidate_all().await;
                    }

                    result.parse_warnings = crate::sql_parser::parse_write_warnings(&parsed);
                    Ok(serialize_response(&write_result_content(&result)))
                }
                Err(e) => tool_error!("Query error: {}", e),
            }
        } else {
            match crate::query::write::execute_write_query_pool(
                &query_pool,
                &sql,
                &parsed,
                self.config.pool.query_timeout_ms,
                self.config.pool.retry_attempts,
            )
            .await
            {
                Ok(result) => Ok(serialize_response(&write_result_content(&result))),
                Err(e) => tool_error!("Query error: {}", e),
            }
        }
    }

    /// Optionally run EXPLAIN based on performance_hints settings.
    /// Returns (plan, explain_error).
    async fn maybe_run_explain(
        &self,
        pool: &crate::backend::PoolHandle,
        sql: &str,
        parsed: &crate::sql_parser::ParsedStatement,
        pool_config: &crate::config::PoolConfig,
        result: &crate::query::read::QueryResult,
    ) -> (Option<serde_json::Value>, Option<String>) {
        let run_explain = matches!(
            parsed.statement_type,
            crate::sql_parser::StatementType::Select
        ) && match pool_config.performance_hints.as_str() {
            "always" => true,
            "auto" => result.execution_time_ms >= pool_config.slow_query_threshold_ms,
            _ => false,
        };

        if !run_explain {
            return (None, None);
        }

        match self
            .backend
            .run_explain(pool, sql, pool_config.query_timeout_ms)
            .await
        {
            Ok(explain_result) => (serde_json::to_value(explain_result).ok(), None),
            Err(e) => {
                let msg = e.to_string();
                let mut preview_end = sql.len().min(200);
                while preview_end > 0 && !sql.is_char_boundary(preview_end) {
                    preview_end -= 1;
                }
                let sql_preview = sql.get(..preview_end).unwrap_or("");
                tracing::warn!(sql = %sql_preview, error = %msg, "EXPLAIN failed; continuing without plan");
                (None, Some(msg))
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
    let sql_truncated = sql.get(..200).unwrap_or(sql);
    let plan_tier = result
        .plan
        .as_ref()
        .and_then(|p| p.get("tier"))
        .and_then(|v| v.as_str())
        .unwrap_or("none");
    let full_table_scan = result
        .plan
        .as_ref()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_sql_length_empty_returns_error() {
        let result = check_sql_length("");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.is_error, Some(true));
        let text = err.content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("SQL cannot be empty"));
    }

    #[test]
    fn test_check_sql_length_exceeds_limit_returns_error() {
        let large_sql = "x".repeat(1_000_001);
        let result = check_sql_length(&large_sql);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.is_error, Some(true));
        let text = err.content[0].raw.as_text().expect("expected text content");
        assert!(text.text.contains("SQL too large"));
        assert!(text.text.contains("1 MB"));
    }

    #[test]
    fn test_check_sql_length_within_limits_returns_ok() {
        let valid_sql = "SELECT * FROM users";
        let result = check_sql_length(valid_sql);
        assert!(result.is_ok());
    }
}
