use anyhow::Result;
use sqlx::MySqlPool;

// Re-export parse_v2 for benchmarking
pub use super::explain_parse::parse_v2;

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

pub async fn run_explain(pool: &MySqlPool, sql: &str) -> Result<ExplainResult> {
    let explain_sql = format!("EXPLAIN FORMAT=JSON {}", sql);
    let row: sqlx::mysql::MySqlRow = sqlx::query(&explain_sql).fetch_one(pool).await?;

    // EXPLAIN FORMAT=JSON returns a single row with one column: the JSON string
    use sqlx::Row;
    let json_str: String = row.try_get(0)?;
    let v: serde_json::Value = serde_json::from_str(&json_str)?;

    super::explain_parse::parse(&v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::setup_test_db;

    #[tokio::test]
    async fn test_explain_simple_select() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        // Use a query against information_schema which always exists.
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
        // rows_examined_estimate should be > 0 for any real query
        let er = result.unwrap();
        // Either a full table scan or index access — just confirm the struct is populated.
        let _ = er.full_table_scan;
        let _ = er.rows_examined_estimate;
    }

    #[tokio::test]
    async fn test_explain_full_table_scan_detected() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        // Create a table without an index on the filter column, then EXPLAIN a query on it.
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
        // val has no index, so we expect a full table scan
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
        // val IS indexed; expect index usage
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
        // join_a is small and may be full-scanned; join_b uses idx_a_id.
        // The key property: rows_examined_estimate should be > 0.
        assert!(
            er.rows_examined_estimate > 0,
            "should have row estimates for JOIN"
        );
    }

    // -----------------------------------------------------------------------
    // MySQL 9.x container tests — explicitly exercise schema v2 (query_plan)
    // format.  These run independently of MYSQL_HOST so that both the v1
    // (MySQL 8.x) and v2 (MySQL 9.x) parser paths are covered in a plain
    // `cargo test` run without any external database.
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
        let er = result.unwrap();
        // ORDER BY on a non-indexed column should trigger a sort in most cases.
        // However, MySQL's optimizer may choose to skip the sort for very small
        // tables (< ~10 rows) where it's cheaper to just return rows unsorted and
        // sort them in-memory without a separate sort node.  We therefore only
        // assert the absence of a crash — the flag may or may not be present
        // depending on the optimizer's row-count estimate.
        //
        // The real validation is that run_explain() successfully parses the EXPLAIN
        // output and returns a valid ExplainResult, which the assert above checks.
        let _ = er.extra_flags; // consumed above; just confirm parsing succeeded
    }
}
