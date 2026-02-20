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
) -> Result<QueryResult> {
    // Compute parse-time warnings before the DB phase so the field is always present.
    let warnings = if performance_hints != "none" {
        // Reconstruct a ParsedStatement from the SQL string and known type so we can
        // call parse_warnings without re-running the full parse pipeline externally.
        let pseudo_parsed = ParsedStatement {
            statement_type: stmt_type.clone(),
            target_schema: None,
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

    // Apply max_rows cap: append LIMIT if none present and max_rows > 0.
    let was_capped;
    let effective_sql: std::borrow::Cow<str> = if max_rows > 0 && !sql.to_uppercase().contains("LIMIT") {
        was_capped = true;
        std::borrow::Cow::Owned(format!("{} LIMIT {}", sql, max_rows))
    } else {
        was_capped = false;
        std::borrow::Cow::Borrowed(sql)
    };
    let sql = effective_sql.as_ref();

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
        let rows = sqlx::query(sql).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        rows
    } else {
        // 1-RTT path: bare fetch_all, no transaction overhead
        sqlx::query(sql).fetch_all(pool).await?
    };
    let db_elapsed = db_start.elapsed().as_millis() as u64;

    // Serialization phase
    let ser_start = Instant::now();
    let json_rows: Vec<Map<String, Value>> = rows.iter().map(row_to_json).collect();
    let ser_elapsed = ser_start.elapsed().as_millis() as u64;

    let row_count = json_rows.len();
    Ok(QueryResult {
        rows: json_rows,
        row_count,
        execution_time_ms: db_elapsed,
        serialization_time_ms: ser_elapsed,
        capped: was_capped,
        parse_warnings: warnings,
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
        "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" => {
            if let Ok(v) = row.try_get::<f64, _>(idx) {
                return serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .unwrap_or(Value::Null);
            }
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
        let test_db = setup_test_db().await;
        let result = execute_read_query(&test_db.pool, "SELECT 1 AS one", &StatementType::Select, false, 0, "none").await;
        assert!(result.is_ok(), "SELECT should succeed: {:?}", result.err());
        assert_eq!(result.unwrap().row_count, 1);
    }

    #[tokio::test]
    async fn test_null_values() {
        let test_db = setup_test_db().await;
        let result = execute_read_query(&test_db.pool, "SELECT NULL AS null_col", &StatementType::Select, false, 0, "none").await.unwrap();
        assert_eq!(result.rows[0]["null_col"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_empty_result_set() {
        let test_db = setup_test_db().await;
        let result = execute_read_query(&test_db.pool, "SELECT 1 WHERE 1=0", &StatementType::Select, false, 0, "none").await.unwrap();
        assert_eq!(result.row_count, 0);
    }

    #[tokio::test]
    async fn test_show_tables() {
        let test_db = setup_test_db().await;
        let result = execute_read_query(&test_db.pool, "SHOW TABLES", &StatementType::Show, false, 0, "none").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execution_time_tracked() {
        let test_db = setup_test_db().await;
        let result = execute_read_query(&test_db.pool, "SELECT 1", &StatementType::Select, false, 0, "none").await.unwrap();
        assert!(result.execution_time_ms < 5000);
    }
}
