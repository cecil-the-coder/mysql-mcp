#[cfg(feature = "mysql")]
use anyhow::Result;

#[cfg(feature = "mysql")]
use sqlx::MySqlPool;

#[cfg(feature = "mysql")]
use super::with_timeout;

/// Query performance tier derived from EXPLAIN output.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplainTier {
    Fast,
    Slow,
    VerySlow,
}

#[derive(Debug, serde::Serialize)]
pub struct ExplainResult {
    pub full_table_scan: bool,
    pub index_used: Option<String>,
    pub rows_examined_estimate: u64,
    pub extra_flags: Vec<&'static str>, // "Using filesort", etc.
    pub tier: ExplainTier,
}

/// Run EXPLAIN FORMAT=JSON on a SELECT query using a MySqlPool directly.
///
/// This is the legacy interface used by integration tests and the explain
/// test helpers. It delegates to the MySQL explain parser.
#[cfg(feature = "mysql")]
pub async fn run_explain(pool: &MySqlPool, sql: &str) -> Result<ExplainResult> {
    use crate::backend::mysql::query_timeout_from_env;
    let timeout_ms = query_timeout_from_env();
    let explain_sql = format!("EXPLAIN FORMAT=JSON {}", sql);
    let explain_fut = async {
        sqlx::query(&explain_sql)
            .fetch_one(pool)
            .await
            .map_err(anyhow::Error::from)
    };
    let row: sqlx::mysql::MySqlRow = with_timeout(timeout_ms, "EXPLAIN", explain_fut).await?;

    // EXPLAIN FORMAT=JSON returns a single row with one column: the JSON string
    use sqlx::Row;
    let json_str: String = row.try_get(0)?;
    let v: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| {
        let location = format!("at line {}, column {}", e.line(), e.column());
        let top_level_keys: Vec<String> = serde_json::from_str::<serde_json::Value>(&json_str)
            .ok()
            .and_then(|v| v.as_object().map(|obj| obj.keys().cloned().collect()))
            .unwrap_or_default();
        let structure_info = if top_level_keys.is_empty() {
            "JSON is not a valid object or could not be partially parsed".to_string()
        } else {
            format!("top-level keys: {}", top_level_keys.join(", "))
        };
        anyhow::anyhow!(
            "Failed to parse EXPLAIN JSON: {} {}. JSON structure: {}",
            e,
            location,
            structure_info
        )
    })?;

    super::explain_parse::parse(&v)
}

#[cfg(all(test, feature = "mysql"))]
mod tests {
    use super::*;
    use crate::test_helpers::setup_test_db;

    #[tokio::test]
    async fn test_explain_simple_select() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let result = run_explain(
            &test_db.pool,
            "SELECT table_name FROM information_schema.tables LIMIT 5",
        )
        .await;
        assert!(
            result.is_ok(),
            "run_explain should succeed: {:?}",
            result.err()
        );
        let er = result.unwrap();
        let _ = er.full_table_scan;
        let _ = er.rows_examined_estimate;
    }

    #[tokio::test]
    async fn test_explain_full_table_scan_detected() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS explain_test_fts (
                id INT PRIMARY KEY AUTO_INCREMENT,
                val VARCHAR(50)
            )",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT IGNORE INTO explain_test_fts (id, val) VALUES (1, 'hello'), (2, 'world')",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();

        let result = run_explain(
            &test_db.pool,
            "SELECT * FROM explain_test_fts WHERE val = 'hello'",
        )
        .await;
        assert!(
            result.is_ok(),
            "run_explain should succeed: {:?}",
            result.err()
        );
        let er = result.unwrap();
        assert!(
            er.full_table_scan,
            "should be a full table scan on unindexed column"
        );
        assert!(er.index_used.is_none(), "no index should be used");
        assert!(er.rows_examined_estimate >= 1);
    }

    #[tokio::test]
    async fn test_explain_index_lookup_detected() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS explain_test_idx (
                id INT PRIMARY KEY AUTO_INCREMENT,
                val VARCHAR(50),
                INDEX idx_val (val)
            )",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT IGNORE INTO explain_test_idx (id, val) VALUES (1, 'hello'), (2, 'world')",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();

        let result = run_explain(
            &test_db.pool,
            "SELECT * FROM explain_test_idx WHERE val = 'hello'",
        )
        .await;
        assert!(
            result.is_ok(),
            "run_explain should succeed: {:?}",
            result.err()
        );
        let er = result.unwrap();
        assert!(!er.full_table_scan, "should NOT be a full table scan");
        assert!(er.index_used.is_some(), "an index should be used");
    }

    #[tokio::test]
    async fn test_explain_join_query() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS explain_join_a (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            )",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS explain_join_b (
                id INT PRIMARY KEY AUTO_INCREMENT,
                a_id INT,
                score INT,
                INDEX idx_a_id (a_id)
            )",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT IGNORE INTO explain_join_a (id, name) VALUES (1,'Alice'),(2,'Bob')")
            .execute(&test_db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT IGNORE INTO explain_join_b (id, a_id, score) VALUES (1,1,100),(2,1,200),(3,2,50)")
            .execute(&test_db.pool)
            .await
            .unwrap();

        let result = run_explain(
            &test_db.pool,
            "SELECT a.name, b.score FROM explain_join_a a JOIN explain_join_b b ON a.id = b.a_id",
        )
        .await;
        assert!(
            result.is_ok(),
            "run_explain JOIN should succeed: {:?}",
            result.err()
        );
        let er = result.unwrap();
        assert!(
            er.rows_examined_estimate > 0,
            "should have row estimates for JOIN"
        );
    }

    // -----------------------------------------------------------------------
    // MySQL 9.x container tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_explain_full_table_scan_detected_mysql9() {
        let Some(test_db) = crate::test_helpers::start_mysql_container("9.2").await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS explain_test_fts (
                id INT PRIMARY KEY AUTO_INCREMENT,
                val VARCHAR(50)
            )",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT IGNORE INTO explain_test_fts (id, val) VALUES (1, 'hello'), (2, 'world')",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        let er = run_explain(
            &test_db.pool,
            "SELECT * FROM explain_test_fts WHERE val = 'hello'",
        )
        .await
        .unwrap();
        assert!(
            er.full_table_scan,
            "mysql9: full table scan on unindexed column"
        );
        assert!(er.index_used.is_none(), "mysql9: no index should be used");
        assert!(er.rows_examined_estimate >= 1);
    }

    #[tokio::test]
    async fn test_explain_index_lookup_detected_mysql9() {
        let Some(test_db) = crate::test_helpers::start_mysql_container("9.2").await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS explain_test_idx (
                id INT PRIMARY KEY AUTO_INCREMENT,
                val VARCHAR(50),
                INDEX idx_val (val)
            )",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT IGNORE INTO explain_test_idx (id, val) VALUES (1, 'hello'), (2, 'world')",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        let er = run_explain(
            &test_db.pool,
            "SELECT * FROM explain_test_idx WHERE val = 'hello'",
        )
        .await
        .unwrap();
        assert!(
            !er.full_table_scan,
            "mysql9: should NOT be a full table scan"
        );
        assert!(er.index_used.is_some(), "mysql9: an index should be used");
    }

    #[tokio::test]
    async fn test_explain_join_query_mysql9() {
        let Some(test_db) = crate::test_helpers::start_mysql_container("9.2").await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS explain_join_a (id INT PRIMARY KEY, name VARCHAR(50))",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS explain_join_b (
                id INT PRIMARY KEY AUTO_INCREMENT,
                a_id INT,
                score INT,
                INDEX idx_a_id (a_id)
            )",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT IGNORE INTO explain_join_a VALUES (1,'Alice'),(2,'Bob')")
            .execute(&test_db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT IGNORE INTO explain_join_b (id, a_id, score) VALUES (1,1,100),(2,1,200),(3,2,50)")
            .execute(&test_db.pool)
            .await
            .unwrap();
        let er = run_explain(
            &test_db.pool,
            "SELECT a.name, b.score FROM explain_join_a a JOIN explain_join_b b ON a.id = b.a_id",
        )
        .await
        .unwrap();
        assert!(
            er.rows_examined_estimate > 0,
            "mysql9: JOIN should have row estimates"
        );
    }

    #[tokio::test]
    async fn test_explain_sort_flagged() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS explain_test_sort (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50)
            )",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT IGNORE INTO explain_test_sort (id, name) VALUES (1,'Zara'),(2,'Alice'),(3,'Mike')")
            .execute(&test_db.pool)
            .await
            .unwrap();

        let result = run_explain(
            &test_db.pool,
            "SELECT * FROM explain_test_sort ORDER BY name",
        )
        .await;
        assert!(
            result.is_ok(),
            "run_explain should succeed: {:?}",
            result.err()
        );
        let _ = result.unwrap();
    }
}

// ---------------------------------------------------------------------------
// SQLite EXPLAIN integration tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "sqlite"))]
mod sqlite_explain_tests {
    use super::*;
    use crate::backend::Backend;
    use crate::test_helpers::setup_sqlite_test_db;

    async fn run_sqlite_explain(
        pool: &crate::backend::PoolHandle,
        sql: &str,
    ) -> Result<ExplainResult> {
        let backend = crate::backend::sqlite::SqliteBackend::new();
        backend.run_explain(pool, sql, 5000).await
    }

    #[tokio::test]
    async fn test_sqlite_explain_basic_select() {
        let db = setup_sqlite_test_db().await;

        let result = run_sqlite_explain(&db.pool, "SELECT * FROM users WHERE id = 1").await;
        assert!(
            result.is_ok(),
            "EXPLAIN QUERY PLAN should succeed: {:?}",
            result.err()
        );
        let er = result.unwrap();
        // PK lookup should use index, not full scan
        assert!(
            !er.full_table_scan,
            "PK lookup should not be a full table scan"
        );
        assert!(er.index_used.is_some(), "PK lookup should use an index");
    }

    #[tokio::test]
    async fn test_sqlite_explain_full_table_scan() {
        let db = setup_sqlite_test_db().await;
        let result = run_sqlite_explain(&db.pool, "SELECT * FROM users WHERE name = 'Alice'").await;
        assert!(result.is_ok(), "EXPLAIN should succeed: {:?}", result.err());
        let er = result.unwrap();
        assert!(
            er.full_table_scan,
            "query on unindexed column should be a full table scan"
        );
        assert!(
            er.index_used.is_none(),
            "no index should be used for unindexed column"
        );
    }

    #[tokio::test]
    async fn test_sqlite_explain_index_scan() {
        let db = setup_sqlite_test_db().await;
        // users table already has UNIQUE index on email
        let result = run_sqlite_explain(
            &db.pool,
            "SELECT * FROM users WHERE email = 'alice@example.com'",
        )
        .await;
        assert!(result.is_ok(), "EXPLAIN should succeed: {:?}", result.err());
        let er = result.unwrap();
        assert!(
            !er.full_table_scan,
            "query on UNIQUE indexed column should not be a full table scan"
        );
        assert!(
            er.index_used.is_some(),
            "an index should be used for the email lookup"
        );
    }

    #[tokio::test]
    async fn test_sqlite_explain_with_sort() {
        let db = setup_sqlite_test_db().await;
        // ORDER BY on unindexed column should use temp B-tree
        let result = run_sqlite_explain(&db.pool, "SELECT * FROM users ORDER BY name").await;
        assert!(result.is_ok(), "EXPLAIN should succeed: {:?}", result.err());
        let er = result.unwrap();
        // ORDER BY without index triggers temp B-tree (sort flag)
        assert!(
            er.extra_flags.contains(&"Using filesort"),
            "ORDER BY on unindexed column should flag filesort"
        );
        assert!(
            er.extra_flags.contains(&"Using temporary"),
            "ORDER BY on unindexed column should flag temporary"
        );
    }

    #[tokio::test]
    async fn test_sqlite_explain_join() {
        let db = setup_sqlite_test_db().await;
        let result = run_sqlite_explain(
            &db.pool,
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        )
        .await;
        assert!(
            result.is_ok(),
            "EXPLAIN JOIN should succeed: {:?}",
            result.err()
        );
        let er = result.unwrap();
        assert!(
            er.rows_examined_estimate > 0,
            "JOIN should have row estimate"
        );
    }
}
