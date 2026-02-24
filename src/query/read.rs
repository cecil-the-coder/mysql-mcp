use crate::sql_parser::{ParsedStatement, StatementType};
use anyhow::Result;
use serde_json::{Map, Value};
use sqlx::{Acquire, Column, MySqlPool, Row, TypeInfo};
use std::time::Instant;

/// Binary columns with invalid UTF-8 are hex-encoded. Cap the output at 512 KB
/// of raw bytes (→ ~1 MB hex string) to prevent OOM on unexpectedly large BLOBs.
const MAX_BINARY_DISPLAY_BYTES: usize = 512 * 1024;

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
#[allow(clippy::too_many_arguments)]
pub async fn execute_read_query(
    pool: &MySqlPool,
    sql: &str,
    parsed: &ParsedStatement,
    force_readonly_transaction: bool,
    max_rows: u32,
    performance_hints: &str,
    slow_query_threshold_ms: u64,
    query_timeout_ms: u64,
) -> Result<QueryResult> {
    let stmt_type = &parsed.statement_type;

    // Compute parse-time warnings before the DB phase so the field is always present.
    // warnings are pre-cached in ParsedStatement — no re-parse needed
    // for the has_limit / has_where / has_wildcard checks.
    let warnings = if performance_hints != "none" {
        parsed.warnings.clone()
    } else {
        vec![]
    };

    // Apply max_rows cap: append LIMIT only for SELECT statements (SHOW/EXPLAIN do not
    // support LIMIT in MySQL). Use the pre-cached has_limit from ParsedStatement — no
    // re-parse needed here.
    let added_limit =
        max_rows > 0 && matches!(stmt_type, StatementType::Select) && !parsed.has_limit;
    let effective_sql;
    let effective_sql_ref = if added_limit {
        // Inject LIMIT max_rows+1 (one extra row) so we can detect whether the result
        // was actually truncated. If MySQL returns more than max_rows rows, there are
        // additional rows beyond the cap and we discard the probe row before returning.
        // Without the +1, a table with exactly max_rows rows would falsely show capped=true.
        effective_sql = format!("{} LIMIT {}", sql, max_rows as u64 + 1);
        effective_sql.as_str()
    } else {
        sql
    };

    // DB phase
    let db_start = Instant::now();
    let db_fut = async {
        if force_readonly_transaction {
            // 4-RTT path: SET TRANSACTION READ ONLY → BEGIN → SQL → COMMIT
            // For MySQL, SET TRANSACTION READ ONLY must be called before BEGIN.
            let mut conn = pool.acquire().await?;
            sqlx::query("SET TRANSACTION READ ONLY")
                .execute(&mut *conn)
                .await?;
            let mut tx = conn.begin().await?;
            let rows = sqlx::query(effective_sql_ref).fetch_all(&mut *tx).await?;
            tx.commit().await?;
            Ok::<Vec<sqlx::mysql::MySqlRow>, anyhow::Error>(rows)
        } else {
            // 1-RTT path: bare fetch_all, no transaction overhead
            Ok(sqlx::query(effective_sql_ref).fetch_all(pool).await?)
        }
    };
    let rows: Vec<sqlx::mysql::MySqlRow> = if query_timeout_ms > 0 {
        match tokio::time::timeout(std::time::Duration::from_millis(query_timeout_ms), db_fut).await
        {
            Ok(result) => result?,
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "Query timed out after {}ms. Set MYSQL_QUERY_TIMEOUT to adjust.",
                    query_timeout_ms
                ))
            }
        }
    } else {
        db_fut.await?
    };
    let db_elapsed = db_start.elapsed().as_millis() as u64;

    // Serialization phase
    let ser_start = Instant::now();
    let mut warnings = warnings; // make mutable so row_to_json can push serialization warnings
    let mut json_rows: Vec<Map<String, Value>> = rows
        .iter()
        .map(|row| row_to_json(row, &mut warnings))
        .collect();
    let ser_elapsed = ser_start.elapsed().as_millis() as u64;

    // If we injected LIMIT max_rows+1 and got more than max_rows rows back, the result
    // was truncated. Discard the extra probe row before returning.
    let over_limit = added_limit && json_rows.len() > max_rows as usize;
    if over_limit {
        json_rows.truncate(max_rows as usize);
    }

    // For SHOW statements (which don't support LIMIT), cap post-fetch if needed.
    let show_capped = matches!(stmt_type, StatementType::Show)
        && max_rows > 0
        && json_rows.len() > max_rows as usize;
    if show_capped {
        json_rows.truncate(max_rows as usize);
    }

    let row_count = json_rows.len();
    // was_capped is true when we hit the injected LIMIT (over_limit), or when we
    // truncated a SHOW result post-fetch — either way, there may be more rows.
    let was_capped = over_limit || show_capped;

    // EXPLAIN phase: decide whether to run based on performance_hints and elapsed time.
    // Only run EXPLAIN for SELECT statements (EXPLAIN doesn't support SHOW/EXPLAIN/etc.)
    let run_explain = matches!(stmt_type, StatementType::Select)
        && match performance_hints {
            "always" => true,
            "auto" => db_elapsed >= slow_query_threshold_ms,
            _ => false,
        };

    let (plan, explain_error): (Option<Value>, Option<String>) = if run_explain {
        match crate::query::explain::run_explain(pool, sql).await {
            Ok(explain_result) => {
                // Tier is computed inside parse_v2 based on full_table_scan and
                // rows_examined_estimate — not wall-clock time, which includes
                // network RTT and tells the LLM nothing about query efficiency.
                (serde_json::to_value(explain_result).ok(), None)
            }
            Err(e) => {
                let msg = e.to_string();
                // Walk back to a valid UTF-8 char boundary before slicing.
                let mut preview_end = sql.len().min(200);
                while preview_end > 0 && !sql.is_char_boundary(preview_end) {
                    preview_end -= 1;
                }
                tracing::warn!(sql = %&sql[..preview_end], error = %msg, "EXPLAIN failed; continuing without plan");
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

fn row_to_json(row: &sqlx::mysql::MySqlRow, warnings: &mut Vec<String>) -> Map<String, Value> {
    let mut map = Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let base = col.name().to_string();
        // Disambiguate duplicate column names (e.g. SELECT t1.a, t2.a FROM …)
        // so the second value is not silently overwritten.
        let key = if !map.contains_key(&base) {
            base
        } else {
            let mut n = 2u64;
            loop {
                let candidate = format!("{}_{}", base, n);
                if !map.contains_key(&candidate) {
                    break candidate;
                }
                // Overflow guard: in practice this is unreachable (SQL length limits
                // column counts to thousands, not quintillions), but prevents a debug-
                // mode panic if somehow u64::MAX is reached.
                n = match n.checked_add(1) {
                    Some(next) => next,
                    None => break format!("{}_dup", base),
                };
            }
        };
        map.insert(key, column_to_json(row, i, col, warnings));
    }
    map
}

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
                        // Signed BIGINT values whose absolute value exceeds 2^53 cannot be
                        // represented exactly as a JSON number by some consumers (e.g. JS).
                        // Serialize as a JSON string, consistent with BIGINT UNSIGNED handling.
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
        // BIGINT UNSIGNED can exceed 2^53 (the largest integer exactly representable
        // as f64). serde_json serialises u64 via f64, so values above 9_007_199_254_740_992
        // would silently lose precision. Return those as JSON strings instead.
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
                            "Column '{}' contains a non-finite float value (NaN or Infinity); \
                                 serialized as null",
                            col.name()
                        ));
                        Value::Null
                    });
            }
            Ok(None) => return Value::Null,
            Err(_) => {}
        },
        // MySQL sends DECIMAL/NUMERIC as text over the wire even in binary protocol.
        // sqlx's type compatibility check rejects String decode for DECIMAL column types,
        // so use try_get_unchecked to bypass it and decode the raw text value.
        // Return as String to preserve full precision — f64 only has ~15 significant digits.
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
            // BIT columns: decode as u64 bitmask
            if let Ok(v) = row.try_get::<Option<u64>, _>(idx) {
                return v.map(|n| serde_json::json!(n)).unwrap_or(Value::Null);
            }
            // single-bit BIT(1): try bool
            if let Ok(v) = row.try_get::<Option<bool>, _>(idx) {
                return v.map(Value::Bool).unwrap_or(Value::Null);
            }
        }
        "YEAR" => {
            // YEAR(4): return as number
            if let Ok(v) = row.try_get::<Option<u16>, _>(idx) {
                return v.map(|y| Value::Number(y.into())).unwrap_or(Value::Null);
            }
        }
        "JSON" => {
            // JSON columns: parse and return as structured JSON value
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
            // ENUM and SET: return as string (already the default fallback,
            // but make explicit here for clarity)
            if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }
        // Temporal types: MySQL binary protocol sends these as binary-encoded bytes, not text.
        // String decoding fails even with try_get_unchecked. Use chrono types (already in Cargo.toml
        // via sqlx "chrono" feature) which have registered binary decoders.
        //
        // Use try_get_unchecked (same pattern as DECIMAL) to bypass sqlx's accepts() check,
        // which may reject computed-expression columns (e.g. NOW()) in MySQL 9.6+ where the
        // column type ID doesn't exactly match the expected ColumnType::Datetime enum variant.
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
    // Try string for everything else (VARCHAR, TEXT, CHAR, etc.)
    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return v.map(Value::String).unwrap_or(Value::Null);
    }
    // Try bytes as last resort — attempt UTF-8 decode first (handles SHOW DATABASES etc.
    // where MySQL returns string columns as binary blobs), fall back to hex only for
    // genuinely binary data.
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
    Value::Null
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::sql_parser::parse_sql;
    use crate::test_helpers::setup_test_db;

    /// Helper: parse SQL and call execute_read_query with the full ParsedStatement.
    async fn read_query(
        pool: &sqlx::MySqlPool,
        sql: &str,
        force_ro: bool,
        max_rows: u32,
        hints: &str,
        slow_ms: u64,
    ) -> anyhow::Result<QueryResult> {
        let parsed = parse_sql(sql).map_err(|e| anyhow::anyhow!(e))?;
        execute_read_query(pool, sql, &parsed, force_ro, max_rows, hints, slow_ms, 0).await
    }

    #[tokio::test]
    async fn test_select_basic() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SELECT 1 AS one", false, 0, "none", 0).await;
        assert!(result.is_ok(), "SELECT should succeed: {:?}", result.err());
        assert_eq!(result.unwrap().row_count, 1);
    }

    #[tokio::test]
    async fn test_null_values() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(
            &test_db.pool,
            "SELECT NULL AS null_col",
            false,
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.rows[0]["null_col"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_empty_result_set() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SELECT 1 WHERE 1=0", false, 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 0);
    }

    #[tokio::test]
    async fn test_show_tables() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SHOW TABLES", false, 0, "none", 0).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execution_time_tracked() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = read_query(&test_db.pool, "SELECT 1", false, 0, "none", 0)
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
            false,
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
        let parsed = parse_sql(sql).map_err(|e| anyhow::anyhow!(e)).unwrap();
        // 1 ms timeout — SLEEP(5) will never complete in time.
        let result = execute_read_query(
            &test_db.pool,
            sql,
            &parsed,
            false, // no readonly transaction
            0,     // no max_rows cap
            "none",
            0,
            1, // query_timeout_ms = 1
        )
        .await;
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
        // Both columns are aliased "a"; the second should become "a_2".
        let result = read_query(&test_db.pool, "SELECT 1 AS a, 2 AS a", false, 0, "none", 0)
            .await
            .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(row.contains_key("a"), "first 'a' column should be present");
        assert!(
            row.contains_key("a_2"),
            "second 'a' should be renamed to 'a_2'"
        );
        assert_eq!(row["a"], serde_json::json!(1), "first a should be 1");
        assert_eq!(row["a_2"], serde_json::json!(2), "second a should be 2");
    }

    #[tokio::test]
    async fn test_triple_duplicate_column_names_deduped() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        // All three columns are aliased "a"; they should become "a", "a_2", "a_3".
        let result = read_query(
            &test_db.pool,
            "SELECT 1 AS a, 2 AS a, 3 AS a",
            false,
            0,
            "none",
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.row_count, 1);
        let row = &result.rows[0];
        assert!(row.contains_key("a"), "first 'a' column should be present");
        assert!(
            row.contains_key("a_2"),
            "second 'a' should be renamed to 'a_2'"
        );
        assert!(
            row.contains_key("a_3"),
            "third 'a' should be renamed to 'a_3'"
        );
        assert_eq!(row["a"], serde_json::json!(1), "first a should be 1");
        assert_eq!(row["a_2"], serde_json::json!(2), "second a should be 2");
        assert_eq!(row["a_3"], serde_json::json!(3), "third a should be 3");
    }
}
