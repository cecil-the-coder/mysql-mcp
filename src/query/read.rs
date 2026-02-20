use std::time::Instant;
use anyhow::Result;
use sqlx::{MySqlPool, Row, Column, TypeInfo, Acquire};
use serde_json::{Value, Map};

pub struct QueryResult {
    pub rows: Vec<Map<String, Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
}

pub async fn execute_read_query(pool: &MySqlPool, sql: &str) -> Result<QueryResult> {
    let start = Instant::now();

    // For MySQL, SET TRANSACTION READ ONLY must be called before BEGIN.
    // We acquire a connection, issue SET TRANSACTION READ ONLY, then begin a transaction.
    let mut conn = pool.acquire().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *conn)
        .await?;
    let mut tx = conn.begin().await?;
    let rows: Vec<sqlx::mysql::MySqlRow> = sqlx::query(sql).fetch_all(&mut *tx).await?;
    tx.commit().await?;

    let elapsed = start.elapsed().as_millis() as u64;

    let json_rows: Vec<Map<String, Value>> = rows
        .iter()
        .map(row_to_json)
        .collect();

    let row_count = json_rows.len();
    Ok(QueryResult {
        rows: json_rows,
        row_count,
        execution_time_ms: elapsed,
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
    // Try bytes as last resort
    if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
        return v
            .map(|b| Value::String(format!("0x{:02x?}", b).replace(", ", "").replace('[', "").replace(']', "")))
            .unwrap_or(Value::Null);
    }
    Value::Null
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    fn mysql_url() -> Option<String> {
        let host = std::env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("MYSQL_PORT").unwrap_or_else(|_| "3306".to_string());
        let user = std::env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_string());
        let pass = std::env::var("MYSQL_PASS").unwrap_or_default();
        let db = std::env::var("MYSQL_DB").unwrap_or_else(|_| "testdb".to_string());

        use std::time::Duration;
        use std::net::TcpStream;
        let addr = format!("{}:{}", host, port);
        match TcpStream::connect_timeout(
            &addr.parse::<std::net::SocketAddr>().ok()?,
            Duration::from_secs(2),
        ) {
            Ok(_) => Some(format!("mysql://{}:{}@{}:{}/{}", user, pass, host, port, db)),
            Err(_) => None,
        }
    }

    #[tokio::test]
    async fn test_select_basic() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();

        let result = execute_read_query(&pool, "SELECT 1 AS one").await;
        assert!(result.is_ok(), "SELECT should succeed: {:?}", result.err());
        let result = result.unwrap();
        assert_eq!(result.row_count, 1);
    }

    #[tokio::test]
    async fn test_null_values() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
        let result = execute_read_query(&pool, "SELECT NULL AS null_col").await.unwrap();
        assert_eq!(result.rows[0]["null_col"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_empty_result_set() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
        let result = execute_read_query(&pool, "SELECT 1 WHERE 1=0").await.unwrap();
        assert_eq!(result.row_count, 0);
    }

    #[tokio::test]
    async fn test_show_tables() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
        let result = execute_read_query(&pool, "SHOW TABLES").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execution_time_tracked() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
        let result = execute_read_query(&pool, "SELECT 1").await.unwrap();
        assert!(result.execution_time_ms < 5000);
    }
}
