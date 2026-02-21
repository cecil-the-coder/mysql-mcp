use std::time::Instant;
use anyhow::Result;
use sqlx::{MySqlPool, Row, Column, TypeInfo, Acquire};
use serde_json::{Value, Map};
use crate::sql_parser::{StatementType, ParsedStatement};

pub struct QueryResult {
    pub rows: Vec<Map<String, Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    pub serialization_time_ms: u64,
    pub capped: bool,
    pub parse_warnings: Vec<String>,
    pub plan: Option<Value>,
}

/// Execute a read query.
///
/// Uses a 4-RTT read-only transaction if:
///   - `force_readonly_transaction` is true (paranoia mode), OR
///   - the statement type is not definitively read-only (i.e., not SELECT/SHOW/EXPLAIN/DESCRIBE)
///
/// Uses a 1-RTT bare fetch_all for SELECT, SHOW, EXPLAIN, and Describe (mapped to Explain
/// by the parser) when `force_readonly_transaction` is false.
///
/// If `max_rows > 0` and the SQL does not already contain a LIMIT clause, a
/// `LIMIT {max_rows}` is appended automatically and `QueryResult::capped` is set to `true`.
pub async fn execute_read_query(
    pool: &MySqlPool,
    sql: &str,
    stmt_type: &StatementType,
    force_readonly_transaction: bool,
    max_rows: u32,
    performance_hints: &str,
    slow_query_threshold_ms: u64,
) -> Result<QueryResult> {
    // Compute parse-time warnings before the DB phase so the field is always present.
    let warnings = if performance_hints != "none" {
        // Reconstruct a ParsedStatement from the SQL string and known type so we can
        // call parse_warnings without re-running the full parse pipeline externally.
        let pseudo_parsed = ParsedStatement {
            statement_type: stmt_type.clone(),
            target_schema: None,
            target_table: None,
            sql: sql.to_string(),
        };
        crate::sql_parser::parse_warnings(&pseudo_parsed)
    } else {
        vec![]
    };

    let use_transaction = force_readonly_transaction || !matches!(
        stmt_type,
        StatementType::Select | StatementType::Show | StatementType::Explain
    );

    // Apply max_rows cap: append LIMIT only for SELECT statements (SHOW/EXPLAIN do not
    // support LIMIT in MySQL). Track whether we injected a LIMIT so we can determine
    // after execution whether the result was actually truncated.
    let added_limit = max_rows > 0
        && matches!(stmt_type, StatementType::Select)
        && !sql.to_uppercase().contains("LIMIT");
    let effective_sql: std::borrow::Cow<str> = if added_limit {
        std::borrow::Cow::Owned(format!("{} LIMIT {}", sql, max_rows))
    } else {
        std::borrow::Cow::Borrowed(sql)
    };
    let effective_sql_ref = effective_sql.as_ref();

    // DB phase
    let db_start = Instant::now();
    let rows: Vec<sqlx::mysql::MySqlRow> = if use_transaction {
        // 4-RTT path: SET TRANSACTION READ ONLY → BEGIN → SQL → COMMIT
        // For MySQL, SET TRANSACTION READ ONLY must be called before BEGIN.
        let mut conn = pool.acquire().await?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *conn)
            .await?;
        let mut tx = conn.begin().await?;
        let rows = sqlx::query(effective_sql_ref).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        rows
    } else {
        // 1-RTT path: bare fetch_all, no transaction overhead
        sqlx::query(effective_sql_ref).fetch_all(pool).await?
    };
    let db_elapsed = db_start.elapsed().as_millis() as u64;

    // Serialization phase
    let ser_start = Instant::now();
    let json_rows: Vec<Map<String, Value>> = rows.iter().map(row_to_json).collect();
    let ser_elapsed = ser_start.elapsed().as_millis() as u64;

    let row_count = json_rows.len();
    // was_capped is true only when we injected a LIMIT *and* the result set hit that
    // exact limit — meaning there may be more rows beyond the cap.
    let was_capped = added_limit && row_count == max_rows as usize;

    // EXPLAIN phase: decide whether to run based on performance_hints and elapsed time.
    // Only run EXPLAIN for SELECT statements (EXPLAIN doesn't support SHOW/EXPLAIN/etc.)
    let run_explain = matches!(stmt_type, StatementType::Select) && match performance_hints {
        "always" => true,
        "auto" => db_elapsed >= slow_query_threshold_ms,
        _ => false,
    };

    let plan: Option<Value> = if run_explain {
        match crate::query::explain::run_explain(pool, sql).await {
            Ok(mut explain_result) => {
                // Compute tier based on MySQL's own rows_examined_estimate and
                // full_table_scan — NOT wall-clock time, which includes network RTT
                // and tells the LLM nothing actionable about query efficiency.
                explain_result.tier = if explain_result.full_table_scan && explain_result.rows_examined_estimate > 10_000 {
                    "very_slow".to_string()
                } else if explain_result.full_table_scan || explain_result.rows_examined_estimate > 1_000 {
                    "slow".to_string()
                } else {
                    "fast".to_string()
                };
                // Compute efficiency: rows_returned / rows_examined
                explain_result.efficiency = if explain_result.rows_examined_estimate > 0 {
                    f64::min(
                        1.0,
                        (row_count as f64) / (explain_result.rows_examined_estimate as f64),
                    )
                } else {
                    1.0
                };
                Some(serde_json::json!({
                    "full_table_scan": explain_result.full_table_scan,
                    "index_used": explain_result.index_used,
                    "rows_examined_estimate": explain_result.rows_examined_estimate,
                    "filtered_pct": explain_result.filtered_pct,
                    "efficiency": explain_result.efficiency,
                    "extra_flags": explain_result.extra_flags,
                    "tier": explain_result.tier,
                    "db_elapsed_ms": db_elapsed,
                }))
            }
            Err(_) => None,
        }
    } else {
        None
    };

    Ok(QueryResult {
        rows: json_rows,
        row_count,
        execution_time_ms: db_elapsed,
        serialization_time_ms: ser_elapsed,
        capped: was_capped,
        parse_warnings: warnings,
        plan,
    })
}

fn row_to_json(row: &sqlx::mysql::MySqlRow) -> Map<String, Value> {
    let mut map = Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let col_name = col.name().to_string();
        let val = column_to_json(row, i, col);
        map.insert(col_name, val);
    }
    map
}

fn column_to_json(row: &sqlx::mysql::MySqlRow, idx: usize, col: &sqlx::mysql::MySqlColumn) -> Value {
    let type_name = col.type_info().name();
    match type_name {
        "TINYINT(1)" | "BOOLEAN" | "BOOL" => {
            if let Ok(v) = row.try_get::<bool, _>(idx) {
                return Value::Bool(v);
            }
        }
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
            if let Ok(v) = row.try_get::<i64, _>(idx) {
                return Value::Number(v.into());
            }
        }
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED" | "BIGINT UNSIGNED" => {
            if let Ok(v) = row.try_get::<u64, _>(idx) {
                return serde_json::json!(v);
            }
        }
        "FLOAT" | "DOUBLE" => {
            if let Ok(v) = row.try_get::<f64, _>(idx) {
                return serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .unwrap_or(Value::Null);
            }
        }
        // MySQL sends DECIMAL/NUMERIC as text over the wire even in binary protocol.
        // sqlx's type compatibility check rejects String decode for DECIMAL column types,
        // so use try_get_unchecked to bypass it and decode the raw text value.
        "DECIMAL" | "NUMERIC" | "NEWDECIMAL" => {
            if let Ok(Some(s)) = row.try_get_unchecked::<Option<String>, _>(idx) {
                if let Ok(f) = s.parse::<f64>() {
                    return serde_json::Number::from_f64(f)
                        .map(Value::Number)
                        .unwrap_or(Value::String(s));
                }
                return Value::String(s);
            }
            return Value::Null;
        }
        _ => {}
    }
    // Try string for everything else (VARCHAR, TEXT, CHAR, DATE, TIME, DATETIME, etc.)
    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return v.map(Value::String).unwrap_or(Value::Null);
    }
    // Try bytes as last resort — attempt UTF-8 decode first (handles SHOW DATABASES etc.
    // where MySQL returns string columns as binary blobs), fall back to hex only for
    // genuinely binary data.
    if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
        return v
            .map(|b| {
                String::from_utf8(b)
                    .map(Value::String)
                    .unwrap_or_else(|e| {
                        let hex: String = e.into_bytes().iter().map(|byte| format!("{:02x}", byte)).collect();
                        Value::String(format!("0x{}", hex))
                    })
            })
            .unwrap_or(Value::Null);
    }
    Value::Null
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::test_helpers::setup_test_db;

    #[tokio::test]
    async fn test_select_basic() {
        let Some(test_db) = setup_test_db().await else { return; };
        let result = execute_read_query(&test_db.pool, "SELECT 1 AS one", &StatementType::Select, false, 0, "none", 0).await;
        assert!(result.is_ok(), "SELECT should succeed: {:?}", result.err());
        assert_eq!(result.unwrap().row_count, 1);
    }

    #[tokio::test]
    async fn test_null_values() {
        let Some(test_db) = setup_test_db().await else { return; };
        let result = execute_read_query(&test_db.pool, "SELECT NULL AS null_col", &StatementType::Select, false, 0, "none", 0).await.unwrap();
        assert_eq!(result.rows[0]["null_col"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_empty_result_set() {
        let Some(test_db) = setup_test_db().await else { return; };
        let result = execute_read_query(&test_db.pool, "SELECT 1 WHERE 1=0", &StatementType::Select, false, 0, "none", 0).await.unwrap();
        assert_eq!(result.row_count, 0);
    }

    #[tokio::test]
    async fn test_show_tables() {
        let Some(test_db) = setup_test_db().await else { return; };
        let result = execute_read_query(&test_db.pool, "SHOW TABLES", &StatementType::Show, false, 0, "none", 0).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execution_time_tracked() {
        let Some(test_db) = setup_test_db().await else { return; };
        let result = execute_read_query(&test_db.pool, "SELECT 1", &StatementType::Select, false, 0, "none", 0).await.unwrap();
        assert!(result.execution_time_ms < 5000);
    }
}
