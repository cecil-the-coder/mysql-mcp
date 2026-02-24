use crate::sql_parser::ParsedStatement;
use super::retry::retry_on_transient_error;
use super::with_timeout;
use anyhow::Result;
use sqlx::MySqlPool;
use std::time::Instant;

pub struct WriteResult {
    pub rows_affected: u64,
    pub last_insert_id: Option<u64>,
    pub execution_time_ms: u64,
    pub parse_warnings: Vec<String>,
}

impl WriteResult {
    fn from_query_result(
        result: sqlx::mysql::MySqlQueryResult,
        elapsed: u64,
        parse_warnings: Vec<String>,
    ) -> Self {
        let last_insert_id = result.last_insert_id();
        Self {
            rows_affected: result.rows_affected(),
            last_insert_id: (last_insert_id > 0).then_some(last_insert_id),
            execution_time_ms: elapsed,
            parse_warnings,
        }
    }
}

/// Execute a DML write statement (INSERT, UPDATE, DELETE) in a transaction.
///
/// `parsed` is the already-parsed statement from the caller; warnings are
/// derived from it directly without re-invoking the SQL parser.
///
/// Retries on transient network errors (connection reset, broken pipe, timeout) up to
/// `retry_attempts` times with exponential backoff.
pub async fn execute_write_query(
    pool: &MySqlPool,
    sql: &str,
    parsed: &ParsedStatement,
    query_timeout_ms: u64,
    retry_attempts: u32,
) -> Result<WriteResult> {
    // Derive safety warnings from the already-parsed statement.
    let parse_warnings = crate::sql_parser::parse_write_warnings(parsed);

    let start = Instant::now();

    let pool_clone = pool.clone();
    let sql_owned = sql.to_string();

    let write_fut = retry_on_transient_error(
        move || {
            let pool = pool_clone.clone();
            let sql = sql_owned.clone();
            async move {
                let mut tx = pool.begin().await?;
                let result = sqlx::query(&sql).execute(&mut *tx).await?;
                tx.commit().await?;
                Ok::<sqlx::mysql::MySqlQueryResult, anyhow::Error>(result)
            }
        },
        retry_attempts,
        "write_query",
    );

    let result = with_timeout(query_timeout_ms, "Query", write_fut).await?;

    let elapsed = start.elapsed().as_millis() as u64;
    Ok(WriteResult::from_query_result(
        result,
        elapsed,
        parse_warnings,
    ))
}

/// Execute a DDL statement (CREATE, ALTER, DROP, TRUNCATE).
/// DDL auto-commits in MySQL, so we don't wrap in explicit transaction.
///
/// Note: TRUNCATE produces a safety warning; callers should call `parse_write_warnings`
/// on the parsed statement and include the result in the response.
///
/// Retries on transient network errors (connection reset, broken pipe, timeout) up to
/// `retry_attempts` times with exponential backoff.
pub async fn execute_ddl_query(
    pool: &MySqlPool,
    sql: &str,
    query_timeout_ms: u64,
    retry_attempts: u32,
) -> Result<WriteResult> {
    let start = Instant::now();

    let pool_clone = pool.clone();
    let sql_owned = sql.to_string();

    // DDL auto-commits; just execute directly
    let ddl_fut = retry_on_transient_error(
        move || {
            let pool = pool_clone.clone();
            let sql = sql_owned.clone();
            async move { sqlx::query(&sql).execute(&pool).await.map_err(Into::into) }
        },
        retry_attempts,
        "ddl_query",
    );

    let result = with_timeout(query_timeout_ms, "Query", ddl_fut).await?;
    let elapsed = start.elapsed().as_millis() as u64;
    Ok(WriteResult::from_query_result(result, elapsed, vec![]))
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::test_helpers::setup_test_db;

    #[tokio::test]
    async fn test_insert_and_rollback() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let pool = &test_db.pool;

        sqlx::query("CREATE TABLE IF NOT EXISTS test_write_ops (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(50))")
            .execute(pool)
            .await
            .unwrap();

        let insert_sql = "INSERT INTO test_write_ops (val) VALUES ('hello')";
        let insert_parsed = crate::sql_parser::parse_sql(insert_sql).unwrap();
        let result = execute_write_query(pool, insert_sql, &insert_parsed, 0, 0).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.rows_affected, 1);
        assert!(result.last_insert_id.is_some());

        let update_sql = "UPDATE test_write_ops SET val='world' WHERE val='hello'";
        let update_parsed = crate::sql_parser::parse_sql(update_sql).unwrap();
        let update_result = execute_write_query(pool, update_sql, &update_parsed, 0, 0).await;
        assert!(update_result.is_ok());
        assert_eq!(update_result.unwrap().rows_affected, 1);

        let delete_sql = "DELETE FROM test_write_ops WHERE val='world'";
        let delete_parsed = crate::sql_parser::parse_sql(delete_sql).unwrap();
        let delete_result = execute_write_query(pool, delete_sql, &delete_parsed, 0, 0).await;
        assert!(delete_result.is_ok());

        sqlx::query("DROP TABLE IF EXISTS test_write_ops")
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    async fn test_ddl_create_and_drop() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let pool = &test_db.pool;

        let create_sql = "CREATE TABLE IF NOT EXISTS test_ddl_temp (id INT)";
        let result = execute_ddl_query(pool, create_sql, 0, 0).await;
        assert!(result.is_ok());

        let drop_sql = "DROP TABLE IF EXISTS test_ddl_temp";
        let drop_result = execute_ddl_query(pool, drop_sql, 0, 0).await;
        assert!(drop_result.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_sql_returns_error() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let sql = "INSERT INTO nonexistent_table_xyz VALUES (1)";
        let parsed = crate::sql_parser::parse_sql(sql).unwrap();
        let result = execute_write_query(&test_db.pool, sql, &parsed, 0, 0).await;
        assert!(result.is_err());
    }
}
