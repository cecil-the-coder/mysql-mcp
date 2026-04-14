use crate::backend::{PoolHandle, RowData};
use crate::sql_parser::ParsedStatement;
use anyhow::Result;
use serde_json::{Map, Value};
#[cfg(feature = "mysql")]
use sqlx::{Column, MySqlPool, Row, TypeInfo};
use std::collections::HashMap;
use std::time::Instant;

use super::retry::retry_on_transient_error;
use super::with_timeout;

/// Binary columns with invalid UTF-8 are hex-encoded. Cap the output at 512 KB
/// of raw bytes (-> ~1 MB hex string) to prevent OOM on unexpectedly large BLOBs.
#[cfg(feature = "mysql")]
const MAX_BINARY_DISPLAY_BYTES: usize = 512 * 1024;

/// Estimate the memory size of a JSON value in bytes.
fn estimate_value_size(v: &Value) -> usize {
    match v {
        Value::Null => 8,
        Value::Bool(_) => 1,
        Value::Number(_) => 24,
        Value::String(s) => s.len() + 24,
        Value::Array(arr) => arr.iter().map(estimate_value_size).sum::<usize>() + arr.len() * 8,
        Value::Object(obj) => obj
            .iter()
            .map(|(k, v)| k.len() + estimate_value_size(v) + 16)
            .sum(),
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub rows: Vec<Map<String, Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    pub serialization_time_ms: u64,
    pub capped: bool,
    pub parse_warnings: Vec<String>,
    pub plan: Option<Value>,
    pub explain_error: Option<String>,
}

// ---------------------------------------------------------------------------
// Legacy MySqlPool interface (kept for backward compatibility with tests)
// ---------------------------------------------------------------------------
#[cfg(feature = "mysql")]
/// Execute a read query using a MySqlPool directly.
pub async fn execute_read_query(
    pool: &MySqlPool,
    sql: &str,
    parsed: &ParsedStatement,
    config: &crate::config::PoolConfig,
) -> Result<QueryResult> {
    execute_read_query_inner(pool, sql, parsed, config).await
}

#[cfg(feature = "mysql")]
/// Internal implementation that works with any type implementing the pool operations.
async fn execute_read_query_inner(
    pool: &MySqlPool,
    sql: &str,
    parsed: &ParsedStatement,
    config: &crate::config::PoolConfig,
) -> Result<QueryResult> {
    let stmt_type = &parsed.statement_type;
    let max_rows = config.max_rows;
    let performance_hints = config.performance_hints.as_str();
    let slow_query_threshold_ms = config.slow_query_threshold_ms;
    let query_timeout_ms = config.query_timeout_ms;
    let retry_attempts = config.retry_attempts;
    let max_result_memory_mb = config.max_result_memory_mb;

    let warnings = if performance_hints != "none" {
        parsed.warnings.clone()
    } else {
        vec![]
    };

    let added_limit = max_rows > 0
        && matches!(stmt_type, crate::sql_parser::StatementType::Select)
        && !parsed.has_limit;
    let effective_sql;
    let effective_sql_ref = if added_limit {
        effective_sql = format!("{} LIMIT {}", sql, max_rows as u64 + 1);
        effective_sql.as_str()
    } else {
        sql
    };

    // DB phase with retry logic for transient network errors
    let db_start = Instant::now();
    let pool_clone = pool.clone();
    let effective_sql_owned = effective_sql_ref.to_string();

    let db_result = retry_on_transient_error(
        || {
            let pool = pool_clone.clone();
            let sql = effective_sql_owned.clone();
            async move { Ok(sqlx::query(&sql).fetch_all(&pool).await?) }
        },
        retry_attempts,
        "read_query",
    );

    let rows: Vec<sqlx::mysql::MySqlRow> =
        with_timeout(query_timeout_ms, "Query", db_result).await?;
    let db_elapsed = db_start.elapsed().as_millis() as u64;

    // Serialization phase with memory tracking
    let ser_start = Instant::now();
    let mut warnings = warnings;
    let max_memory_bytes = (max_result_memory_mb as usize) * 1024 * 1024;
    let mut total_memory_bytes: usize = 0;
    let initial_capacity = if max_rows > 0 {
        rows.len().min(max_rows as usize + 1)
    } else {
        rows.len().min(1000)
    };
    let mut json_rows: Vec<Map<String, Value>> = Vec::with_capacity(initial_capacity);
    for row in &rows {
        let json_row = row_to_json(row, &mut warnings);

        let row_size: usize = json_row.values().map(estimate_value_size).sum();
        let row_overhead = json_row.len().saturating_mul(32);
        let row_total = row_size.saturating_add(row_overhead);

        if max_memory_bytes > 0 && total_memory_bytes.saturating_add(row_total) > max_memory_bytes {
            warnings.push(format!(
                "Result truncated at {} rows due to memory limit ({} MB). Add a more specific WHERE clause or reduce selected columns.",
                json_rows.len(), max_result_memory_mb
            ));
            break;
        }

        total_memory_bytes = total_memory_bytes.saturating_add(row_total);
        json_rows.push(json_row);
    }
    let ser_elapsed = ser_start.elapsed().as_millis() as u64;

    let over_limit = added_limit && json_rows.len() > max_rows as usize;
    if over_limit {
        json_rows.truncate(max_rows as usize);
    }

    let show_capped = matches!(stmt_type, crate::sql_parser::StatementType::Show)
        && max_rows > 0
        && json_rows.len() > max_rows as usize;
    if show_capped {
        json_rows.truncate(max_rows as usize);
    }

    let row_count = json_rows.len();
    let was_capped = over_limit || show_capped;

    let run_explain = matches!(stmt_type, crate::sql_parser::StatementType::Select)
        && match performance_hints {
            "always" => true,
            "auto" => db_elapsed >= slow_query_threshold_ms,
            _ => false,
        };

    let (plan, explain_error): (Option<Value>, Option<String>) = if run_explain {
        match crate::query::explain::run_explain(pool, effective_sql_ref).await {
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
    } else {
        (None, None)
    };

    Ok(QueryResult {
        rows: json_rows,
        row_count,
        execution_time_ms: db_elapsed,
        serialization_time_ms: ser_elapsed,
        capped: was_capped,
        parse_warnings: warnings,
        plan,
        explain_error,
    })
}

// ---------------------------------------------------------------------------
// PoolHandle interface (used by the server handlers)
// ---------------------------------------------------------------------------

/// Execute a read query using a PoolHandle.
pub async fn execute_read_query_pool(
    pool: &PoolHandle,
    sql: &str,
    parsed: &ParsedStatement,
    config: &crate::config::PoolConfig,
) -> Result<QueryResult> {
    let stmt_type = &parsed.statement_type;
    let max_rows = config.max_rows;
    let performance_hints = config.performance_hints.as_str();
    let slow_query_threshold_ms = config.slow_query_threshold_ms;
    let query_timeout_ms = config.query_timeout_ms;
    let retry_attempts = config.retry_attempts;
    let max_result_memory_mb = config.max_result_memory_mb;

    let warnings = if performance_hints != "none" {
        parsed.warnings.clone()
    } else {
        vec![]
    };

    let added_limit = max_rows > 0
        && matches!(stmt_type, crate::sql_parser::StatementType::Select)
        && !parsed.has_limit;
    let effective_sql;
    let effective_sql_ref = if added_limit {
        effective_sql = format!("{} LIMIT {}", sql, max_rows as u64 + 1);
        effective_sql.as_str()
    } else {
        sql
    };

    // DB phase with retry logic for transient network errors
    let db_start = Instant::now();
    let pool_clone = pool.clone();
    let effective_sql_owned = effective_sql_ref.to_string();

    let db_result = retry_on_transient_error(
        || {
            let pool = pool_clone.clone();
            let sql = effective_sql_owned.clone();
            async move { pool.fetch_all(&sql).await }
        },
        retry_attempts,
        "read_query",
    );

    let rows: Vec<RowData> = with_timeout(query_timeout_ms, "Query", db_result).await?;
    let db_elapsed = db_start.elapsed().as_millis() as u64;

    // Serialization phase with memory tracking
    let ser_start = Instant::now();
    let mut warnings = warnings;
    let max_memory_bytes = (max_result_memory_mb as usize) * 1024 * 1024;
    let mut total_memory_bytes: usize = 0;
    let initial_capacity = if max_rows > 0 {
        rows.len().min(max_rows as usize + 1)
    } else {
        rows.len().min(1000)
    };
    let mut json_rows: Vec<Map<String, Value>> = Vec::with_capacity(initial_capacity);
    for row in &rows {
        let json_row = row_data_to_json(row, &mut warnings);

        let row_size: usize = json_row.values().map(estimate_value_size).sum();
        let row_overhead = json_row.len().saturating_mul(32);
        let row_total = row_size.saturating_add(row_overhead);

        if max_memory_bytes > 0 && total_memory_bytes.saturating_add(row_total) > max_memory_bytes {
            warnings.push(format!(
                "Result truncated at {} rows due to memory limit ({} MB). Add a more specific WHERE clause or reduce selected columns.",
                json_rows.len(), max_result_memory_mb
            ));
            break;
        }

        total_memory_bytes = total_memory_bytes.saturating_add(row_total);
        json_rows.push(json_row);
    }
    let ser_elapsed = ser_start.elapsed().as_millis() as u64;

    let over_limit = added_limit && json_rows.len() > max_rows as usize;
    if over_limit {
        json_rows.truncate(max_rows as usize);
    }

    let show_capped = matches!(stmt_type, crate::sql_parser::StatementType::Show)
        && max_rows > 0
        && json_rows.len() > max_rows as usize;
    if show_capped {
        json_rows.truncate(max_rows as usize);
    }

    let row_count = json_rows.len();
    let was_capped = over_limit || show_capped;

    let run_explain = matches!(stmt_type, crate::sql_parser::StatementType::Select)
        && match performance_hints {
            "always" => true,
            "auto" => db_elapsed >= slow_query_threshold_ms,
            _ => false,
        };

    let (plan, explain_error): (Option<Value>, Option<String>) = if run_explain {
        // Use the backend's run_explain if available through pool
        // For now, we skip EXPLAIN when using PoolHandle since we don't have
        // access to the backend from here. The handler can run it separately.
        // TODO: Pass backend reference to enable EXPLAIN with PoolHandle
        (None, None)
    } else {
        (None, None)
    };

    Ok(QueryResult {
        rows: json_rows,
        row_count,
        execution_time_ms: db_elapsed,
        serialization_time_ms: ser_elapsed,
        capped: was_capped,
        parse_warnings: warnings,
        plan,
        explain_error,
    })
}

// ---------------------------------------------------------------------------
// Row conversion functions
// ---------------------------------------------------------------------------

#[cfg(feature = "mysql")]
/// Convert a MySQL row to a JSON map, handling duplicate column names.
fn row_to_json(row: &sqlx::mysql::MySqlRow, warnings: &mut Vec<String>) -> Map<String, Value> {
    let mut map = Map::new();
    let mut suffix_counters: HashMap<&str, u64> = HashMap::new();
    for (i, col) in row.columns().iter().enumerate() {
        let base = col.name();
        let key = if let Some(&counter) = suffix_counters.get(base) {
            let owned_key = format!("{}_{}", base, counter);
            suffix_counters.insert(base, counter.saturating_add(1));
            owned_key
        } else {
            suffix_counters.insert(base, 2);
            base.to_string()
        };
        map.insert(key, column_to_json(row, i, col, warnings));
    }
    map
}

#[cfg(feature = "mysql")]
/// Convert a MySQL column value to JSON (type-aware).
fn column_to_json(
    row: &sqlx::mysql::MySqlRow,
    idx: usize,
    col: &sqlx::mysql::MySqlColumn,
    warnings: &mut Vec<String>,
) -> Value {
    let type_name = col.type_info().name();
    match type_name {
        "TINYINT(1)" | "BOOLEAN" | "BOOL" => {
            if let Ok(v) = row.try_get::<Option<bool>, _>(idx) {
                return v.map(Value::Bool).unwrap_or(Value::Null);
            }
        }
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
            if let Ok(v) = row.try_get::<Option<i64>, _>(idx) {
                return v
                    .map(|n| {
                        if !(-9_007_199_254_740_992i64..=9_007_199_254_740_992i64).contains(&n) {
                            Value::String(n.to_string())
                        } else {
                            Value::Number(n.into())
                        }
                    })
                    .unwrap_or(Value::Null);
            }
        }
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
            if let Ok(v) = row.try_get::<Option<u64>, _>(idx) {
                return v.map(|n| serde_json::json!(n)).unwrap_or(Value::Null);
            }
        }
        "BIGINT UNSIGNED" => {
            if let Ok(v) = row.try_get::<Option<u64>, _>(idx) {
                return v
                    .map(|n| {
                        if n > 9_007_199_254_740_992u64 {
                            Value::String(n.to_string())
                        } else {
                            serde_json::json!(n)
                        }
                    })
                    .unwrap_or(Value::Null);
            }
        }
        "FLOAT" | "DOUBLE" => match row.try_get::<Option<f64>, _>(idx) {
            Ok(Some(v)) => {
                return serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .unwrap_or_else(|| {
                        warnings.push(format!(
                            "Column '{}' contains NaN/Infinity value converted to NULL",
                            col.name()
                        ));
                        Value::Null
                    });
            }
            Ok(None) => return Value::Null,
            Err(_) => {}
        },
        "DECIMAL" | "NUMERIC" | "NEWDECIMAL" => {
            match row.try_get_unchecked::<Option<String>, _>(idx) {
                Ok(Some(s)) => return Value::String(s),
                Ok(None) => return Value::Null,
                Err(e) => {
                    tracing::warn!(
                        "DECIMAL column at index {} failed to decode as string: {}",
                        idx,
                        e
                    );
                    return Value::Null;
                }
            }
        }
        "BIT" => {
            if let Ok(v) = row.try_get::<Option<u64>, _>(idx) {
                return v.map(|n| serde_json::json!(n)).unwrap_or(Value::Null);
            }
            if let Ok(v) = row.try_get::<Option<bool>, _>(idx) {
                return v.map(Value::Bool).unwrap_or(Value::Null);
            }
        }
        "YEAR" => {
            if let Ok(v) = row.try_get::<Option<u16>, _>(idx) {
                return v.map(|y| Value::Number(y.into())).unwrap_or(Value::Null);
            }
        }
        "JSON" => {
            if let Ok(Some(s)) = row.try_get::<Option<String>, _>(idx) {
                match serde_json::from_str::<Value>(&s) {
                    Ok(v) => return v,
                    Err(e) => {
                        tracing::warn!(
                            "JSON column could not be parsed (returning as string): {}",
                            e
                        );
                        return Value::String(s);
                    }
                }
            }
            return Value::Null;
        }
        "ENUM" | "SET" => {
            if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }
        "DATETIME" | "TIMESTAMP" => {
            if let Ok(v) = row.try_get_unchecked::<Option<chrono::NaiveDateTime>, _>(idx) {
                return v
                    .map(|dt| Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
                    .unwrap_or(Value::Null);
            }
        }
        "DATE" => {
            if let Ok(v) = row.try_get_unchecked::<Option<chrono::NaiveDate>, _>(idx) {
                return v
                    .map(|d| Value::String(d.format("%Y-%m-%d").to_string()))
                    .unwrap_or(Value::Null);
            }
        }
        "TIME" => {
            if let Ok(v) = row.try_get_unchecked::<Option<chrono::NaiveTime>, _>(idx) {
                return v
                    .map(|t| Value::String(t.format("%H:%M:%S").to_string()))
                    .unwrap_or(Value::Null);
            }
        }
        _ => {}
    }
    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return v.map(Value::String).unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
        return match v {
            None => Value::Null,
            Some(b) => String::from_utf8(b).map(Value::String).unwrap_or_else(|e| {
                let bytes = e.into_bytes();
                let total = bytes.len();
                let display = &bytes[..total.min(MAX_BINARY_DISPLAY_BYTES)];
                let mut hex = String::with_capacity(display.len() * 2 + 2);
                hex.push_str("0x");
                for b in display {
                    use std::fmt::Write as _;
                    let _ = write!(hex, "{:02x}", b);
                }
                if total > MAX_BINARY_DISPLAY_BYTES {
                    warnings.push(format!(
                        "Binary column '{}' truncated: {} bytes total, displayed first {} bytes as hex",
                        col.name(),
                        total,
                        MAX_BINARY_DISPLAY_BYTES
                    ));
                }
                Value::String(hex)
            }),
        };
    }
    warnings.push(format!(
        "Column '{}' (type '{}') could not be decoded as text or binary, returning NULL",
        col.name(),
        type_name
    ));
    Value::Null
}

/// Convert a RowData to a JSON map, handling duplicate column names.
fn row_data_to_json(row: &RowData, _warnings: &mut Vec<String>) -> Map<String, Value> {
    let mut map = Map::new();
    let mut suffix_counters: HashMap<String, u64> = HashMap::new();
    for (col_name, value) in &row.columns {
        let key = if let Some(&counter) = suffix_counters.get(col_name) {
            let owned_key = format!("{}_{}", col_name, counter);
            suffix_counters.insert(col_name.clone(), counter.saturating_add(1));
            owned_key
        } else {
            suffix_counters.insert(col_name.clone(), 2);
            col_name.clone()
        };
        map.insert(key, value.clone());
    }
    map
}

#[cfg(all(test, feature = "mysql"))]
mod integration_tests {
    use super::*;
    use crate::sql_parser::parse_sql;
    use crate::test_helpers::setup_test_db;

    async fn read_query(
        pool: &sqlx::MySqlPool,
        sql: &str,
        max_rows: u32,
        hints: &str,
        slow_ms: u64,
    ) -> anyhow::Result<QueryResult> {
        let parsed = parse_sql(sql, "MySQL").map_err(|e| anyhow::anyhow!(e))?;
        let config = crate::config::PoolConfig {
            max_rows,
            performance_hints: hints.to_string(),
            slow_query_threshold_ms: slow_ms,
            ..Default::default()
        };
        execute_read_query(pool, sql, &parsed, &config).await
    }

    #[tokio::test]
    async fn test_select_basic() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SELECT 1 AS one", 0, "none", 0).await;
        assert!(result.is_ok(), "SELECT should succeed: {:?}", result.err());
        assert_eq!(result.unwrap().row_count, 1);
    }

    #[tokio::test]
    async fn test_null_values() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SELECT NULL AS null_col ", 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.rows[0]["null_col"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_empty_result_set() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SELECT 1 WHERE 1=0", 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 0);
    }

    #[tokio::test]
    async fn test_show_tables() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SHOW TABLES", 0, "none", 0).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execution_time_tracked() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SELECT 1", 0, "none", 0)
            .await
            .unwrap();
        assert!(result.execution_time_ms < 5000);
    }

    #[tokio::test]
    async fn test_datetime_serialization() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(
            &test_db.pool,
            "SELECT NOW() as now, CURDATE() as today, CURTIME() as t",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(
            row["now"].is_string(),
            "NOW() must serialize as string, got {:?}",
            row["now"]
        );
        assert!(
            row["today"].is_string(),
            "CURDATE() must serialize as string, got {:?}",
            row["today"]
        );
        assert!(
            row["t"].is_string(),
            "CURTIME() must serialize as string, got {:?}",
            row["t"]
        );
    }

    #[tokio::test]
    async fn test_query_timeout_enforced() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let sql = "SELECT SLEEP(5)";
        let parsed = parse_sql(sql, "MySQL")
            .map_err(|e| anyhow::anyhow!(e))
            .unwrap();
        let config = crate::config::PoolConfig {
            query_timeout_ms: 1,
            retry_attempts: 0,
            ..Default::default()
        };
        let result = execute_read_query(&test_db.pool, sql, &parsed, &config).await;
        assert!(result.is_err(), "query should have timed out");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("timed out") || err.contains("timeout"),
            "error should mention timeout, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_duplicate_column_names_deduped() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SELECT 1 AS a, 2 AS a", 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(row.contains_key("a"), "first 'a' column should be present");
        assert!(row.contains_key("a_2"), "second a should be renamed to a_2");
        assert_eq!(row["a"], serde_json::json!(1), "first a should be 1");
        assert_eq!(row["a_2"], serde_json::json!(2), "second a should be 2");
    }

    #[tokio::test]
    async fn test_triple_duplicate_column_names_deduped() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SELECT 1 AS a, 2 AS a, 3 AS a", 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(row.contains_key("a"), "first 'a' column should be present");
        assert!(row.contains_key("a_2"), "second a should be renamed to a_2");
        assert!(row.contains_key("a_3"), "third a should be renamed to a_3");
        assert_eq!(row["a"], serde_json::json!(1), "first a should be 1");
        assert_eq!(row["a_2"], serde_json::json!(2), "second a should be 2");
        assert_eq!(row["a_3"], serde_json::json!(3), "third a should be 3");
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL read integration tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "postgres"))]
mod pg_integration_tests {
    use super::*;
    use crate::sql_parser::parse_sql;
    use crate::test_helpers::setup_pg_test_db;

    async fn pg_read_query(
        pool: &crate::backend::PoolHandle,
        sql: &str,
        max_rows: u32,
        hints: &str,
        slow_ms: u64,
    ) -> anyhow::Result<QueryResult> {
        let parsed = parse_sql(sql, "PostgreSQL").map_err(|e| anyhow::anyhow!(e))?;
        let config = crate::config::PoolConfig {
            max_rows,
            performance_hints: hints.to_string(),
            slow_query_threshold_ms: slow_ms,
            ..Default::default()
        };
        execute_read_query_pool(pool, sql, &parsed, &config).await
    }

    #[tokio::test]
    async fn test_pg_select_basic() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let result = pg_read_query(&test_db.pool_handle, "SELECT 1 AS one", 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0]["one"], serde_json::json!(1));
    }

    #[tokio::test]
    async fn test_pg_null_values() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let result = pg_read_query(
            &test_db.pool_handle,
            "SELECT NULL AS null_col ",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.rows[0]["null_col"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_pg_empty_result_set() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let result = pg_read_query(&test_db.pool_handle, "SELECT 1 WHERE 1=0", 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 0);
    }

    #[tokio::test]
    async fn test_pg_execution_time_tracked() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let result = pg_read_query(&test_db.pool_handle, "SELECT 1", 0, "none", 0)
            .await
            .unwrap();
        assert!(result.execution_time_ms < 5000);
    }

    #[tokio::test]
    async fn test_pg_datetime_serialization() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let result = pg_read_query(
            &test_db.pool_handle,
            "SELECT NOW() AS now, CURRENT_DATE AS today, CURRENT_TIME AS t ",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        eprintln!(
            "DEBUG now={:?} today={:?} t={:?}",
            row.get("now"),
            row.get("today"),
            row.get("t")
        );
        assert!(
            row["now"].is_string(),
            "NOW() must serialize as string, got {:?}",
            row["now"]
        );
        assert!(
            row["today"].is_string(),
            "CURRENT_DATE must serialize as string, got {:?}",
            row["today"]
        );
        assert!(
            row["t"].is_string(),
            "CURRENT_TIME must serialize as string, got {:?}",
            row["t"]
        );
    }

    #[tokio::test]
    async fn test_pg_duplicate_column_names_deduped() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let result = pg_read_query(&test_db.pool_handle, "SELECT 1 AS a, 2 AS a", 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(row.contains_key("a"), "first 'a' column should be present");
        assert!(row.contains_key("a_2"), "second a should be renamed to a_2");
        assert_eq!(row["a"], serde_json::json!(1), "first a should be 1");
        assert_eq!(row["a_2"], serde_json::json!(2), "second a should be 2");
    }

    #[tokio::test]
    async fn test_pg_triple_duplicate_column_names_deduped() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let result = pg_read_query(
            &test_db.pool_handle,
            "SELECT 1 AS a, 2 AS a, 3 AS a",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(row.contains_key("a"), "first 'a' column should be present");
        assert!(row.contains_key("a_2"), "second a should be renamed to a_2");
        assert!(row.contains_key("a_3"), "third a should be renamed to a_3");
        assert_eq!(row["a"], serde_json::json!(1), "first a should be 1");
        assert_eq!(row["a_2"], serde_json::json!(2), "second a should be 2");
        assert_eq!(row["a_3"], serde_json::json!(3), "third a should be 3");
    }

    #[tokio::test]
    async fn test_pg_various_type_serialization() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        // Test various PG types: int4, int8, varchar, bool, jsonb, numeric, timestamp, date
        let result = pg_read_query(
            &test_db.pool_handle,
            "SELECT \
                42::int4 AS int4_col, \
                9999999999::int8 AS int8_col, \
                'hello'::varchar AS varchar_col, \
                true::bool AS bool_col, \
                '{\"key\": \"val\"}'::jsonb AS jsonb_col, \
                123.45::numeric AS numeric_col, \
                '2025-01-15 10:30:00'::timestamp AS ts_col, \
                '2025-01-15'::date AS date_col",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert_eq!(row["int4_col"], serde_json::json!(42));
        assert_eq!(row["int8_col"], serde_json::json!(9999999999i64));
        assert_eq!(row["varchar_col"], serde_json::json!("hello"));
        assert_eq!(row["bool_col"], serde_json::json!(true));
        // jsonb is parsed into JSON value
        assert_eq!(row["jsonb_col"]["key"], serde_json::json!("val"));
        // numeric is serialized as string to preserve precision
        assert_eq!(row["numeric_col"], serde_json::json!("123.45"));
        assert!(row["ts_col"].is_string());
        assert!(row["date_col"].is_string());
    }

    #[tokio::test]
    async fn test_pg_boolean_serialization() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let result = pg_read_query(
            &test_db.pool_handle,
            "SELECT true AS t, false AS f, NULL::bool AS null_bool ",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0]["t"], serde_json::json!(true));
        assert_eq!(result.rows[0]["f"], serde_json::json!(false));
        assert_eq!(result.rows[0]["null_bool"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_pg_array_serialization() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let result = pg_read_query(
            &test_db.pool_handle,
            "SELECT ARRAY[1, 2, 3]::int4[] AS int_arr, ARRAY['a', 'b']::text[] AS txt_arr ",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(row["int_arr"].is_array());
        assert_eq!(row["int_arr"].as_array().unwrap().len(), 3);
        assert!(row["txt_arr"].is_array());
    }
}

// ---------------------------------------------------------------------------
// SQLite read integration tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "sqlite"))]
mod sqlite_integration_tests {
    use super::*;
    use crate::sql_parser::parse_sql;
    use crate::test_helpers::setup_sqlite_test_db;

    async fn sqlite_read_query(
        pool: &crate::backend::PoolHandle,
        sql: &str,
        max_rows: u32,
        hints: &str,
        slow_ms: u64,
    ) -> anyhow::Result<QueryResult> {
        let parsed = parse_sql(sql, "sqlite").map_err(|e| anyhow::anyhow!(e))?;
        let config = crate::config::PoolConfig {
            max_rows,
            performance_hints: hints.to_string(),
            slow_query_threshold_ms: slow_ms,
            ..Default::default()
        };
        execute_read_query_pool(pool, sql, &parsed, &config).await
    }

    #[tokio::test]
    async fn test_sqlite_select_basic() {
        let db = setup_sqlite_test_db().await;
        // Use a typed column from the seeded table (SQLite type info works correctly
        // for columns with declared types; untyped literals like `SELECT 1` return
        // type_info name="" and the backend returns Null for them).
        let result =
            sqlite_read_query(&db.pool, "SELECT id, name FROM users LIMIT 1", 0, "none", 0)
                .await
                .unwrap();
        assert_eq!(result.row_count, 1);
        assert!(result.rows[0]["id"].is_number(), "id should be numeric ");
        assert!(result.rows[0]["name"].is_string(), "name should be text ");
    }

    #[tokio::test]
    async fn test_sqlite_null_values() {
        let db = setup_sqlite_test_db().await;
        let result = sqlite_read_query(&db.pool, "SELECT NULL AS null_col ", 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0]["null_col"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_sqlite_empty_result_set() {
        let db = setup_sqlite_test_db().await;
        let result = sqlite_read_query(&db.pool, "SELECT 1 WHERE 1=0", 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 0);
    }

    #[tokio::test]
    async fn test_sqlite_execution_time_tracked() {
        let db = setup_sqlite_test_db().await;
        let result = sqlite_read_query(&db.pool, "SELECT 1", 0, "none", 0)
            .await
            .unwrap();
        assert!(result.execution_time_ms < 5000);
    }

    #[tokio::test]
    async fn test_sqlite_duplicate_column_names_deduped() {
        let db = setup_sqlite_test_db().await;
        // Use CAST to give columns declared types (untyped literals return Null in SQLite backend)
        let result = sqlite_read_query(
            &db.pool,
            "SELECT CAST(1 AS INTEGER) AS a, CAST(2 AS INTEGER) AS a ",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(row.contains_key("a"), "first a column should be present ");
        assert!(
            row.contains_key("a_2"),
            "second a should be renamed to a_2 "
        );
        assert_eq!(row["a"], serde_json::json!(1), "first a should be 1 ");
        assert_eq!(row["a_2"], serde_json::json!(2), "second a should be 2 ");
    }

    #[tokio::test]
    async fn test_sqlite_triple_duplicate_column_names_deduped() {
        let db = setup_sqlite_test_db().await;
        let result = sqlite_read_query(
            &db.pool,
            "SELECT CAST(1 AS INTEGER) AS a, CAST(2 AS INTEGER) AS a, CAST(3 AS INTEGER) AS a ",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(row.contains_key("a"), "first a column should be present ");
        assert!(
            row.contains_key("a_2"),
            "second a should be renamed to a_2 "
        );
        assert!(row.contains_key("a_3"), "third a should be renamed to a_3 ");
        assert_eq!(row["a"], serde_json::json!(1), "first a should be 1 ");
        assert_eq!(row["a_2"], serde_json::json!(2), "second a should be 2 ");
        assert_eq!(row["a_3"], serde_json::json!(3), "third a should be 3 ");
    }

    #[tokio::test]
    async fn test_sqlite_dynamic_type_system() {
        let db = setup_sqlite_test_db().await;
        // SQLite's dynamic type system: storing "hello" in an INTEGER column
        // should still return it as a string.
        db.execute("CREATE TABLE dynamic_test (val INTEGER)")
            .await
            .unwrap();
        db.execute("INSERT INTO dynamic_test (val) VALUES ('hello')")
            .await
            .unwrap();

        let result = sqlite_read_query(&db.pool, "SELECT val FROM dynamic_test ", 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 1);
        let val = &result.rows[0]["val"];
        // SQLite stored "hello" in an INTEGER column — should come back as string
        // because the fallback path tries string first
        assert!(
            val.is_string(),
            "dynamic type: string in INTEGER column should return as string, got {:?}",
            val
        );
        assert_eq!(val.as_str().unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_sqlite_blob_columns() {
        let db = setup_sqlite_test_db().await;
        // Products table already has BLOB data seeded
        let result = sqlite_read_query(
            &db.pool,
            r"SELECT name, data FROM products WHERE name = 'Widget'",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let data = &result.rows[0]["data"];
        // The BLOB X'48454C4C4F' = "HELLO" in UTF-8, so it should come back as a string
        assert!(
            data.is_string(),
            "valid UTF-8 BLOB should be returned as string "
        );
        assert_eq!(data.as_str().unwrap(), "HELLO");

        // Test non-UTF-8 BLOB: X'DEADBEEF' is not valid UTF-8
        let result = sqlite_read_query(
            &db.pool,
            "SELECT name, data FROM products WHERE name = 'Doohickey'",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let data = &result.rows[0]["data"];
        // Should be hex-encoded since 0xDEADBEEF is not valid UTF-8
        assert!(
            data.is_string(),
            "non-UTF-8 BLOB should be hex-encoded as string "
        );
        assert_eq!(data.as_str().unwrap(), "deadbeef");

        // NULL BLOB
        let result = sqlite_read_query(
            &db.pool,
            "SELECT name, data FROM products WHERE name = 'Gadget'",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0]["data"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_sqlite_datetime_strings() {
        let db = setup_sqlite_test_db().await;
        // SQLite datetime functions return untyped expressions; use CAST to get TEXT type
        let result = sqlite_read_query(
            &db.pool,
            r"SELECT CAST(datetime('now') AS TEXT) AS dt, CAST(date('now') AS TEXT) AS d, CAST(time('now') AS TEXT) AS t ",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(
            row["dt"].is_string(),
            "datetime should be string, got {:?}",
            row["dt"]
        );
        assert!(
            row["d"].is_string(),
            "date should be string, got {:?}",
            row["d"]
        );
        assert!(
            row["t"].is_string(),
            "time should be string, got {:?}",
            row["t"]
        );
    }

    #[tokio::test]
    async fn test_sqlite_boolean_serialization() {
        let db = setup_sqlite_test_db().await;
        db.execute("CREATE TABLE bool_test (flag BOOLEAN)")
            .await
            .unwrap();
        db.execute("INSERT INTO bool_test (flag) VALUES (1), (0)")
            .await
            .unwrap();

        let result = sqlite_read_query(
            &db.pool,
            "SELECT flag FROM bool_test ORDER BY flag DESC ",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 2);
        // SQLite stores booleans as 0/1 — should come back as bool
        assert_eq!(result.rows[0]["flag"], serde_json::json!(true));
        assert_eq!(result.rows[1]["flag"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn test_sqlite_real_serialization() {
        let db = setup_sqlite_test_db().await;
        // Use CAST to declare REAL type (untyped literals return Null in SQLite backend)
        let result = sqlite_read_query(
            &db.pool,
            "SELECT CAST(3.14 AS REAL) AS pi, CAST(-0.001 AS REAL) AS neg, CAST(1e10 AS REAL) AS big ",
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(row["pi"].is_number());
        assert!(row["neg"].is_number());
        assert!(row["big"].is_number());
    }
}
