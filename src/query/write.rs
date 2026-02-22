use std::time::Instant;
use anyhow::Result;
use sqlx::MySqlPool;
use crate::sql_parser::ParsedStatement;

pub struct WriteResult {
    pub rows_affected: u64,
    pub last_insert_id: Option<u64>,
    pub execution_time_ms: u64,
    pub parse_warnings: Vec<String>,
}

/// Execute a DML write statement (INSERT, UPDATE, DELETE) in a transaction.
///
/// `parsed` is the already-parsed statement from the caller; warnings are
/// derived from it directly without re-invoking the SQL parser.
pub async fn execute_write_query(
    pool: &MySqlPool,
    sql: &str,
    parsed: &ParsedStatement,
) -> Result<WriteResult> {
    // Derive safety warnings from the already-parsed statement.
    let parse_warnings = crate::sql_parser::parse_write_warnings(parsed);

    let start = Instant::now();

    let mut tx = pool.begin().await?;
    let result = sqlx::query(sql).execute(&mut *tx).await?;
    tx.commit().await?;

    let elapsed = start.elapsed().as_millis() as u64;

    Ok(WriteResult {
        rows_affected: result.rows_affected(),
        last_insert_id: (result.last_insert_id() > 0).then(|| result.last_insert_id()),
        execution_time_ms: elapsed,
        parse_warnings,
    })
}

/// Execute a DDL statement (CREATE, ALTER, DROP, TRUNCATE).
/// DDL auto-commits in MySQL, so we don't wrap in explicit transaction.
///
/// DDL statements produce no parse warnings, so no `ParsedStatement` is needed.
pub async fn execute_ddl_query(
    pool: &MySqlPool,
    sql: &str,
) -> Result<WriteResult> {
    let start = Instant::now();
    // DDL auto-commits; just execute directly
    let result = sqlx::query(sql).execute(pool).await?;
    let elapsed = start.elapsed().as_millis() as u64;
    Ok(WriteResult {
        rows_affected: result.rows_affected(),
        last_insert_id: (result.last_insert_id() > 0).then(|| result.last_insert_id()),
        execution_time_ms: elapsed,
        parse_warnings: vec![],
    })
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::test_helpers::setup_test_db;

    #[tokio::test]
    async fn test_insert_and_rollback() {
        let Some(test_db) = setup_test_db().await else { return; };
        let pool = &test_db.pool;

        sqlx::query("CREATE TABLE IF NOT EXISTS test_write_ops (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(50))")
            .execute(pool)
            .await
            .unwrap();

        let insert_sql = "INSERT INTO test_write_ops (val) VALUES ('hello')";
        let insert_parsed = crate::sql_parser::parse_sql(insert_sql).unwrap();
        let result = execute_write_query(pool, insert_sql, &insert_parsed).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.rows_affected, 1);
        assert!(result.last_insert_id.is_some());

        let update_sql = "UPDATE test_write_ops SET val='world' WHERE val='hello'";
        let update_parsed = crate::sql_parser::parse_sql(update_sql).unwrap();
        let update_result = execute_write_query(pool, update_sql, &update_parsed).await;
        assert!(update_result.is_ok());
        assert_eq!(update_result.unwrap().rows_affected, 1);

        let delete_sql = "DELETE FROM test_write_ops WHERE val='world'";
        let delete_parsed = crate::sql_parser::parse_sql(delete_sql).unwrap();
        let delete_result = execute_write_query(pool, delete_sql, &delete_parsed).await;
        assert!(delete_result.is_ok());

        sqlx::query("DROP TABLE IF EXISTS test_write_ops").execute(pool).await.ok();
    }

    #[tokio::test]
    async fn test_ddl_create_and_drop() {
        let Some(test_db) = setup_test_db().await else { return; };
        let pool = &test_db.pool;

        let create_sql = "CREATE TABLE IF NOT EXISTS test_ddl_temp (id INT)";
        let result = execute_ddl_query(pool, create_sql).await;
        assert!(result.is_ok());

        let drop_sql = "DROP TABLE IF EXISTS test_ddl_temp";
        let drop_result = execute_ddl_query(pool, drop_sql).await;
        assert!(drop_result.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_sql_returns_error() {
        let Some(test_db) = setup_test_db().await else { return; };
        let sql = "INSERT INTO nonexistent_table_xyz VALUES (1)";
        let parsed = crate::sql_parser::parse_sql(sql).unwrap();
        let result = execute_write_query(&test_db.pool, sql, &parsed).await;
        assert!(result.is_err());
    }

    // mysql-mcp-t69: timezone handling - requires special MySQL server config
    #[tokio::test]
    async fn test_timezone_handling_stub() {
        eprintln!("TODO: requires special setup (MySQL server with non-UTC timezone configured)");
    }

    // mysql-mcp-1hq: Unix socket connection - requires special environment
    #[tokio::test]
    async fn test_unix_socket_connection_stub() {
        eprintln!("TODO: requires special setup (Unix socket path configured in environment)");
    }
}
