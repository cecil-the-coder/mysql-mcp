use std::time::Instant;
use anyhow::Result;
use sqlx::MySqlPool;

pub struct WriteResult {
    pub rows_affected: u64,
    pub last_insert_id: Option<u64>,
    pub execution_time_ms: u64,
}

pub async fn execute_write_query(pool: &MySqlPool, sql: &str) -> Result<WriteResult> {
    let start = Instant::now();

    let mut tx = pool.begin().await?;
    let result = sqlx::query(sql).execute(&mut *tx).await?;
    tx.commit().await?;

    let elapsed = start.elapsed().as_millis() as u64;

    Ok(WriteResult {
        rows_affected: result.rows_affected(),
        last_insert_id: {
            let id = result.last_insert_id();
            if id > 0 { Some(id) } else { None }
        },
        execution_time_ms: elapsed,
    })
}

/// Execute a DDL statement (CREATE, ALTER, DROP, TRUNCATE).
/// DDL auto-commits in MySQL, so we don't wrap in explicit transaction.
pub async fn execute_ddl_query(pool: &MySqlPool, sql: &str) -> Result<WriteResult> {
    let start = Instant::now();
    // DDL auto-commits; just execute directly
    let result = sqlx::query(sql).execute(pool).await?;
    let elapsed = start.elapsed().as_millis() as u64;
    Ok(WriteResult {
        rows_affected: result.rows_affected(),
        last_insert_id: {
            let id = result.last_insert_id();
            if id > 0 { Some(id) } else { None }
        },
        execution_time_ms: elapsed,
    })
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
    async fn test_insert_and_rollback() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();

        // Setup
        sqlx::query("CREATE TABLE IF NOT EXISTS test_write_ops (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(50))")
            .execute(&pool)
            .await
            .unwrap();

        // Insert
        let result = execute_write_query(&pool, "INSERT INTO test_write_ops (val) VALUES ('hello')").await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.rows_affected, 1);
        assert!(result.last_insert_id.is_some());

        // Update
        let update_result = execute_write_query(&pool, "UPDATE test_write_ops SET val='world' WHERE val='hello'").await;
        assert!(update_result.is_ok());
        assert_eq!(update_result.unwrap().rows_affected, 1);

        // Delete
        let delete_result = execute_write_query(&pool, "DELETE FROM test_write_ops WHERE val='world'").await;
        assert!(delete_result.is_ok());

        // Cleanup
        sqlx::query("DROP TABLE IF EXISTS test_write_ops").execute(&pool).await.ok();
    }

    #[tokio::test]
    async fn test_ddl_create_and_drop() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();

        let result = execute_ddl_query(&pool, "CREATE TABLE IF NOT EXISTS test_ddl_temp (id INT)").await;
        assert!(result.is_ok());

        let drop_result = execute_ddl_query(&pool, "DROP TABLE IF EXISTS test_ddl_temp").await;
        assert!(drop_result.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_sql_returns_error() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();

        let result = execute_write_query(&pool, "INSERT INTO nonexistent_table_xyz VALUES (1)").await;
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
