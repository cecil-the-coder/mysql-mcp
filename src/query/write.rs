//! Write and DDL query execution (INSERT, UPDATE, DELETE, CREATE, ALTER, DROP).
//!
//! This module handles execution of SQL statements that modify data or schema:
//!
//! - **DML operations** ([`execute_write_query`]): INSERT, UPDATE, DELETE statements
//!   executed within an explicit transaction that is committed on success. If the
//!   operation fails or the connection drops, MySQL rolls back automatically.
//!
//! - **DDL operations** ([`execute_ddl_query`]): CREATE, ALTER, DROP, TRUNCATE
//!   statements executed without an explicit transaction wrapper, since MySQL
//!   implicitly commits DDL statements.

use super::retry::retry_on_transient_error;
use super::with_timeout;
use crate::backend::{ExecuteResult, PoolHandle};
use crate::sql_parser::ParsedStatement;
use anyhow::Result;
#[cfg(feature = "mysql")]
use sqlx::MySqlPool;
use std::sync::Arc;
use std::time::Instant;

pub struct WriteResult {
    pub rows_affected: u64,
    pub last_insert_id: Option<u64>,
    pub execution_time_ms: u64,
    pub parse_warnings: Vec<String>,
}

impl WriteResult {
    pub fn from_execute_result(
        result: ExecuteResult,
        elapsed: u64,
        parse_warnings: Vec<String>,
    ) -> Self {
        Self {
            rows_affected: result.rows_affected,
            last_insert_id: result.last_insert_id,
            execution_time_ms: elapsed,
            parse_warnings,
        }
    }

    #[cfg(feature = "mysql")]
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

// ---------------------------------------------------------------------------
// Legacy MySqlPool interface (kept for backward compatibility with tests)
// ---------------------------------------------------------------------------
#[cfg(feature = "mysql")]
/// Execute a DML write statement (INSERT, UPDATE, DELETE) in a transaction using MySqlPool.
pub async fn execute_write_query(
    pool: &MySqlPool,
    sql: &str,
    parsed: &ParsedStatement,
    query_timeout_ms: u64,
    retry_attempts: u32,
) -> Result<WriteResult> {
    let parse_warnings = crate::sql_parser::parse_write_warnings(parsed);

    let start = Instant::now();

    let pool_clone = pool.clone();
    let sql_arc = Arc::<str>::from(sql);

    let write_fut = retry_on_transient_error(
        move || {
            let pool = pool_clone.clone();
            let sql = sql_arc.clone();
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

#[cfg(feature = "mysql")]
/// Execute a DDL statement (CREATE, ALTER, DROP, TRUNCATE) using MySqlPool.
pub async fn execute_ddl_query(
    pool: &MySqlPool,
    sql: &str,
    query_timeout_ms: u64,
    retry_attempts: u32,
) -> Result<WriteResult> {
    let start = Instant::now();

    let pool_clone = pool.clone();
    let sql_arc = Arc::<str>::from(sql);

    let ddl_fut = retry_on_transient_error(
        move || {
            let pool = pool_clone.clone();
            let sql = sql_arc.clone();
            async move { sqlx::query(&sql).execute(&pool).await.map_err(Into::into) }
        },
        retry_attempts,
        "ddl_query",
    );

    let result = with_timeout(query_timeout_ms, "Query", ddl_fut).await?;
    let elapsed = start.elapsed().as_millis() as u64;
    Ok(WriteResult::from_query_result(result, elapsed, vec![]))
}

// ---------------------------------------------------------------------------
// PoolHandle interface (used by the server handlers)
// ---------------------------------------------------------------------------

/// Execute a DML write statement (INSERT, UPDATE, DELETE) in a transaction using PoolHandle.
pub async fn execute_write_query_pool(
    pool: &PoolHandle,
    sql: &str,
    parsed: &ParsedStatement,
    query_timeout_ms: u64,
    retry_attempts: u32,
) -> Result<WriteResult> {
    let parse_warnings = crate::sql_parser::parse_write_warnings(parsed);

    let start = Instant::now();

    let pool_clone = pool.clone();
    let sql_owned = sql.to_string();

    let write_fut = retry_on_transient_error(
        move || {
            let pool = pool_clone.clone();
            let sql = sql_owned.clone();
            async move { pool.execute_in_transaction(&sql).await }
        },
        retry_attempts,
        "write_query",
    );

    let result = with_timeout(query_timeout_ms, "Query", write_fut).await?;

    let elapsed = start.elapsed().as_millis() as u64;
    Ok(WriteResult::from_execute_result(
        result,
        elapsed,
        parse_warnings,
    ))
}

/// Execute a DDL statement (CREATE, ALTER, DROP, TRUNCATE) using PoolHandle.
/// DDL auto-commits in MySQL, so we don't wrap in explicit transaction.
pub async fn execute_ddl_query_pool(
    pool: &PoolHandle,
    sql: &str,
    query_timeout_ms: u64,
    retry_attempts: u32,
) -> Result<WriteResult> {
    let start = Instant::now();

    let pool_clone = pool.clone();
    let sql_owned = sql.to_string();

    let ddl_fut = retry_on_transient_error(
        move || {
            let pool = pool_clone.clone();
            let sql = sql_owned.clone();
            async move { pool.execute(&sql).await }
        },
        retry_attempts,
        "ddl_query",
    );

    let result = with_timeout(query_timeout_ms, "Query", ddl_fut).await?;
    let elapsed = start.elapsed().as_millis() as u64;
    Ok(WriteResult::from_execute_result(result, elapsed, vec![]))
}

#[cfg(all(test, feature = "mysql"))]
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
        let insert_parsed = crate::sql_parser::parse_sql(insert_sql, "MySQL").unwrap();
        let result = execute_write_query(pool, insert_sql, &insert_parsed, 0, 0).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.rows_affected, 1);
        assert!(result.last_insert_id.is_some());

        let update_sql = "UPDATE test_write_ops SET val='world' WHERE val='hello'";
        let update_parsed = crate::sql_parser::parse_sql(update_sql, "MySQL").unwrap();
        let update_result = execute_write_query(pool, update_sql, &update_parsed, 0, 0).await;
        assert!(update_result.is_ok());
        assert_eq!(update_result.unwrap().rows_affected, 1);

        let delete_sql = "DELETE FROM test_write_ops WHERE val='world'";
        let delete_parsed = crate::sql_parser::parse_sql(delete_sql, "MySQL").unwrap();
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
        let parsed = crate::sql_parser::parse_sql(sql, "MySQL").unwrap();
        let result = execute_write_query(&test_db.pool, sql, &parsed, 0, 0).await;
        assert!(result.is_err());
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL write integration tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "postgres"))]
mod pg_integration_tests {
    use super::*;
    use crate::test_helpers::setup_pg_test_db;

    #[tokio::test]
    async fn test_pg_insert_update_delete() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };

        // Create test table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pg_test_write_ops (id SERIAL PRIMARY KEY, val VARCHAR(50))",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();

        // Insert
        let insert_sql = "INSERT INTO pg_test_write_ops (val) VALUES ('hello')";
        let insert_parsed = crate::sql_parser::parse_sql(insert_sql, "PostgreSQL").unwrap();
        let result =
            execute_write_query_pool(&test_db.pool_handle, insert_sql, &insert_parsed, 0, 0).await;
        assert!(result.is_ok(), "INSERT should succeed: {:?}", result.err());
        let result = result.unwrap();
        assert_eq!(result.rows_affected, 1);
        // PostgreSQL does not have last_insert_id via PoolOps
        assert!(
            result.last_insert_id.is_none(),
            "PG should not return last_insert_id"
        );

        // Update
        let update_sql = "UPDATE pg_test_write_ops SET val='world' WHERE val='hello'";
        let update_parsed = crate::sql_parser::parse_sql(update_sql, "PostgreSQL").unwrap();
        let update_result =
            execute_write_query_pool(&test_db.pool_handle, update_sql, &update_parsed, 0, 0).await;
        assert!(update_result.is_ok());
        assert_eq!(update_result.unwrap().rows_affected, 1);

        // Delete
        let delete_sql = "DELETE FROM pg_test_write_ops WHERE val='world'";
        let delete_parsed = crate::sql_parser::parse_sql(delete_sql, "PostgreSQL").unwrap();
        let delete_result =
            execute_write_query_pool(&test_db.pool_handle, delete_sql, &delete_parsed, 0, 0).await;
        assert!(delete_result.is_ok());

        sqlx::query("DROP TABLE IF EXISTS pg_test_write_ops")
            .execute(&test_db.pool)
            .await
            .ok();
    }

    #[tokio::test]
    async fn test_pg_ddl_create_and_drop() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };

        let create_sql = "CREATE TABLE IF NOT EXISTS pg_test_ddl_temp (id SERIAL PRIMARY KEY)";
        let result = execute_ddl_query_pool(&test_db.pool_handle, create_sql, 0, 0).await;
        assert!(
            result.is_ok(),
            "CREATE TABLE should succeed: {:?}",
            result.err()
        );

        let drop_sql = "DROP TABLE IF EXISTS pg_test_ddl_temp";
        let drop_result = execute_ddl_query_pool(&test_db.pool_handle, drop_sql, 0, 0).await;
        assert!(
            drop_result.is_ok(),
            "DROP TABLE should succeed: {:?}",
            drop_result.err()
        );
    }

    #[tokio::test]
    async fn test_pg_invalid_sql_returns_error() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let sql = "INSERT INTO nonexistent_table_xyz VALUES (1)";
        let parsed = crate::sql_parser::parse_sql(sql, "PostgreSQL").unwrap();
        let result = execute_write_query_pool(&test_db.pool_handle, sql, &parsed, 0, 0).await;
        assert!(result.is_err(), "invalid SQL should fail");
    }

    #[tokio::test]
    async fn test_pg_insert_rows_affected() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pg_test_rows (id SERIAL PRIMARY KEY, val VARCHAR(50))",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();

        // Multi-row insert
        let sql = "INSERT INTO pg_test_rows (val) VALUES ('a'), ('b'), ('c')";
        let parsed = crate::sql_parser::parse_sql(sql, "PostgreSQL").unwrap();
        let result = execute_write_query_pool(&test_db.pool_handle, sql, &parsed, 0, 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().rows_affected, 3);

        sqlx::query("DROP TABLE IF EXISTS pg_test_rows")
            .execute(&test_db.pool)
            .await
            .ok();
    }
}

// ---------------------------------------------------------------------------
// SQLite write integration tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "sqlite"))]
mod sqlite_integration_tests {
    use super::*;
    use crate::test_helpers::setup_sqlite_test_db;

    #[tokio::test]
    async fn test_sqlite_insert_and_last_insert_id() {
        let db = setup_sqlite_test_db().await;

        db.execute("CREATE TABLE test_write_ops (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
            .await
            .unwrap();

        // Insert and check last_insert_id
        let insert_sql = "INSERT INTO test_write_ops (val) VALUES ('hello')";
        let insert_parsed = crate::sql_parser::parse_sql(insert_sql, "sqlite").unwrap();
        let result = execute_write_query_pool(&db.pool, insert_sql, &insert_parsed, 0, 0).await;
        assert!(result.is_ok(), "INSERT should succeed: {:?}", result.err());
        let result = result.unwrap();
        assert_eq!(result.rows_affected, 1);
        assert!(
            result.last_insert_id.is_some(),
            "SQLite should return last_insert_id"
        );
        assert_eq!(result.last_insert_id.unwrap(), 1);

        // Second insert should get id 2
        let insert_sql2 = "INSERT INTO test_write_ops (val) VALUES ('world')";
        let insert_parsed2 = crate::sql_parser::parse_sql(insert_sql2, "sqlite").unwrap();
        let result2 = execute_write_query_pool(&db.pool, insert_sql2, &insert_parsed2, 0, 0)
            .await
            .unwrap();
        assert_eq!(result2.rows_affected, 1);
        assert_eq!(result2.last_insert_id.unwrap(), 2);

        // Update
        let update_sql = "UPDATE test_write_ops SET val='updated' WHERE val='hello'";
        let update_parsed = crate::sql_parser::parse_sql(update_sql, "sqlite").unwrap();
        let update_result =
            execute_write_query_pool(&db.pool, update_sql, &update_parsed, 0, 0).await;
        assert!(update_result.is_ok());
        assert_eq!(update_result.unwrap().rows_affected, 1);

        // Delete
        let delete_sql = "DELETE FROM test_write_ops WHERE val='updated'";
        let delete_parsed = crate::sql_parser::parse_sql(delete_sql, "sqlite").unwrap();
        let delete_result =
            execute_write_query_pool(&db.pool, delete_sql, &delete_parsed, 0, 0).await;
        assert!(delete_result.is_ok());

        // Cleanup
        db.execute("DROP TABLE IF EXISTS test_write_ops").await.ok();
    }

    #[tokio::test]
    async fn test_sqlite_ddl_create_alter_drop() {
        let db = setup_sqlite_test_db().await;

        // CREATE TABLE
        let create_sql = "CREATE TABLE test_ddl_temp (id INTEGER PRIMARY KEY, val TEXT)";
        let result = execute_ddl_query_pool(&db.pool, create_sql, 0, 0).await;
        assert!(
            result.is_ok(),
            "CREATE TABLE should succeed: {:?}",
            result.err()
        );

        // ALTER TABLE ADD COLUMN (SQLite supports this)
        let alter_sql = "ALTER TABLE test_ddl_temp ADD COLUMN extra INTEGER DEFAULT 0";
        let alter_result = execute_ddl_query_pool(&db.pool, alter_sql, 0, 0).await;
        assert!(
            alter_result.is_ok(),
            "ALTER TABLE ADD COLUMN should succeed: {:?}",
            alter_result.err()
        );

        // Verify the column was added
        let rows = db
            .fetch_all("PRAGMA table_info(\"test_ddl_temp\")")
            .await
            .unwrap();
        let col_names: Vec<&str> = rows
            .iter()
            .filter_map(|r| {
                r.columns
                    .iter()
                    .find(|(n, _)| n == "name")
                    .and_then(|(_, v)| v.as_str())
            })
            .collect();
        assert!(
            col_names.contains(&"extra"),
            "ALTER should have added 'extra' column"
        );

        // DROP TABLE
        let drop_sql = "DROP TABLE IF EXISTS test_ddl_temp";
        let drop_result = execute_ddl_query_pool(&db.pool, drop_sql, 0, 0).await;
        assert!(
            drop_result.is_ok(),
            "DROP TABLE should succeed: {:?}",
            drop_result.err()
        );
    }

    #[tokio::test]
    async fn test_sqlite_invalid_sql_returns_error() {
        let db = setup_sqlite_test_db().await;
        let sql = "INSERT INTO nonexistent_table_xyz VALUES (1)";
        let parsed = crate::sql_parser::parse_sql(sql, "sqlite").unwrap();
        let result = execute_write_query_pool(&db.pool, sql, &parsed, 0, 0).await;
        assert!(result.is_err(), "invalid SQL should fail");
    }

    #[tokio::test]
    async fn test_sqlite_insert_with_explicit_rowid() {
        let db = setup_sqlite_test_db().await;

        db.execute("CREATE TABLE test_rowid (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();

        // Insert with explicit rowid
        let sql = "INSERT INTO test_rowid (id, val) VALUES (42, 'custom_id')";
        let parsed = crate::sql_parser::parse_sql(sql, "sqlite").unwrap();
        let result = execute_write_query_pool(&db.pool, sql, &parsed, 0, 0).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.rows_affected, 1);
        assert_eq!(result.last_insert_id.unwrap(), 42);

        // Insert another row and verify it gets the next rowid
        let sql2 = "INSERT INTO test_rowid (val) VALUES ('auto_id')";
        let parsed2 = crate::sql_parser::parse_sql(sql2, "sqlite").unwrap();
        let result2 = execute_write_query_pool(&db.pool, sql2, &parsed2, 0, 0)
            .await
            .unwrap();
        assert_eq!(result2.last_insert_id.unwrap(), 43);

        db.execute("DROP TABLE IF EXISTS test_rowid").await.ok();
    }

    #[tokio::test]
    async fn test_sqlite_multi_row_insert() {
        let db = setup_sqlite_test_db().await;

        db.execute("CREATE TABLE test_multi (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
            .await
            .unwrap();

        let sql = "INSERT INTO test_multi (val) VALUES ('a'), ('b'), ('c')";
        let parsed = crate::sql_parser::parse_sql(sql, "sqlite").unwrap();
        let result = execute_write_query_pool(&db.pool, sql, &parsed, 0, 0).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.rows_affected, 3);
        // last_insert_id returns the id of the last inserted row
        assert_eq!(result.last_insert_id.unwrap(), 3);

        db.execute("DROP TABLE IF EXISTS test_multi").await.ok();
    }

    #[tokio::test]
    async fn test_sqlite_update_no_match() {
        let db = setup_sqlite_test_db().await;

        db.execute("CREATE TABLE test_update_none (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();

        let sql = "UPDATE test_update_none SET val='changed' WHERE val='nonexistent'";
        let parsed = crate::sql_parser::parse_sql(sql, "sqlite").unwrap();
        let result = execute_write_query_pool(&db.pool, sql, &parsed, 0, 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().rows_affected, 0);

        db.execute("DROP TABLE IF EXISTS test_update_none")
            .await
            .ok();
    }
}
