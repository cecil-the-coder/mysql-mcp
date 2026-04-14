//! PostgreSQL EXPLAIN (FORMAT JSON) parsing.
//!
//! PostgreSQL's `EXPLAIN (FORMAT JSON)` returns a JSON **array** of plan nodes
//! (one per top-level plan). This module walks that array, extracts relevant
//! information (full table scans, index usage, row estimates, sorts, etc.),
//! and maps it to the shared `ExplainResult` struct.

use anyhow::Result;
use serde_json::Value;

use super::explain::{ExplainResult, ExplainTier};

const VERY_SLOW_ROW_THRESHOLD: u64 = 10_000;
const SLOW_ROW_THRESHOLD: u64 = 1_000;

#[derive(Default)]
struct PlanStats {
    has_full_table_scan: bool,
    index_name: Option<String>,
    total_estimated_rows: f64,
    has_sort: bool,
    has_temporary: bool,
}

/// Walk a single PG explain plan node (which may have nested `Plans` children).
fn walk_pg_node(node: &Value, stats: &mut PlanStats) {
    let node_type = node["Node Type"].as_str().unwrap_or("");

    match node_type {
        // Full table scans
        "Seq Scan" => {
            stats.has_full_table_scan = true;
            stats.total_estimated_rows += node["Plan Rows"].as_f64().unwrap_or(0.0);
        }

        // Index-based scans — capture the index name
        "Index Scan" | "Index Only Scan" | "Bitmap Heap Scan" => {
            if stats.index_name.is_none() {
                stats.index_name =
                    node["Index Name"].as_str().map(str::to_string);
            }
            stats.total_estimated_rows += node["Plan Rows"].as_f64().unwrap_or(0.0);
        }

        // Bitmap index scan (feeds into Bitmap Heap Scan)
        "Bitmap Index Scan" => {
            if stats.index_name.is_none() {
                stats.index_name =
                    node["Index Name"].as_str().map(str::to_string);
            }
        }

        // Joins
        "Nested Loop" | "Merge Join" | "Hash Join" => {
            stats.total_estimated_rows += node["Plan Rows"].as_f64().unwrap_or(0.0);
        }

        // Aggregation (may use hash aggregate = temporary)
        "Aggregate" | "GroupAggregate" | "HashAggregate" => {
            stats.total_estimated_rows += node["Plan Rows"].as_f64().unwrap_or(0.0);
            if node_type == "HashAggregate" {
                stats.has_temporary = true;
            }
        }

        // Sorts
        "Sort" | "Incremental Sort" => {
            stats.has_sort = true;
            stats.total_estimated_rows += node["Plan Rows"].as_f64().unwrap_or(0.0);
        }

        // Materialize / Subquery Scan
        "Materialize" | "Subquery Scan" | "CTE Scan" | "Function Scan"
        | "Values Scan" | "Table Function Scan" | "Foreign Scan" => {
            stats.total_estimated_rows += node["Plan Rows"].as_f64().unwrap_or(0.0);
        }

        // Unique / SetOp / Window / Limit
        "Unique" | "SetOp" | "WindowAgg" | "Limit" | "Append" | "Merge Append"
        | "Recursive Union" => {
            stats.total_estimated_rows += node["Plan Rows"].as_f64().unwrap_or(0.0);
        }

        // ModifyTable (INSERT/UPDATE/DELETE)
        "ModifyTable" | "Update" | "Insert" | "Delete" => {
            stats.total_estimated_rows += node["Plan Rows"].as_f64().unwrap_or(0.0);
        }

        _ => {
            // Unknown node type — still try to grab row estimate
            stats.total_estimated_rows += node["Plan Rows"].as_f64().unwrap_or(0.0);
        }
    }

    // Check for actual rows (from ANALYZE) — use if available
    if let Some(actual) = node.get("Actual Rows").and_then(|v| v.as_f64()) {
        // If we have actual rows, they are more accurate than estimated.
        // We don't replace the estimate here since we want the estimated plan
        // cost analysis, but this is available for future enhancement.
        let _ = actual; // suppress unused warning
    }

    // Check for Sort operations in the node's extra info
    if let Some(sort_keys) = node.get("Sort Key").and_then(|v| v.as_array()) {
        if !sort_keys.is_empty() {
            stats.has_sort = true;
        }
    }

    // Walk child plans (PostgreSQL uses "Plans" for nested sub-plans)
    if let Some(plans) = node["Plans"].as_array() {
        for child in plans {
            walk_pg_node(child, stats);
        }
    }
}

fn make_result(stats: PlanStats) -> Result<ExplainResult> {
    let full_table_scan = stats.has_full_table_scan;
    let rows_examined_estimate = if stats.total_estimated_rows.is_finite() {
        stats.total_estimated_rows.ceil() as u64
    } else {
        tracing::warn!(
            estimated_rows = stats.total_estimated_rows,
            "EXPLAIN row count estimate is non-finite (Infinity or NaN); reporting as 0"
        );
        0
    };
    let mut extra_flags = vec![];
    if stats.has_sort {
        extra_flags.push("Using filesort");
    }
    if stats.has_temporary {
        extra_flags.push("Using temporary");
    }

    let tier = if rows_examined_estimate > VERY_SLOW_ROW_THRESHOLD {
        ExplainTier::VerySlow
    } else if rows_examined_estimate > SLOW_ROW_THRESHOLD {
        ExplainTier::Slow
    } else {
        ExplainTier::Fast
    };

    Ok(ExplainResult {
        full_table_scan,
        index_used: stats.index_name,
        rows_examined_estimate,
        extra_flags,
        tier,
    })
}

/// Parse `EXPLAIN (FORMAT JSON)` output from PostgreSQL.
///
/// PostgreSQL returns a JSON array where each element is `{"Plan": {...}}`.
/// The top-level `walk_pg_node` expects a node with "Node Type" at the top
/// level, so we unwrap the "Plan" key from each array element.
pub fn parse_postgres_explain(arr: &Value) -> Result<ExplainResult> {
    let plans = arr
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL EXPLAIN JSON must be an array"))?;

    if plans.is_empty() {
        anyhow::bail!("PostgreSQL EXPLAIN JSON array is empty");
    }

    let mut stats = PlanStats::default();
    for plan in plans {
        // Each top-level element is {"Plan": {...}}; unwrap to get the node.
        let node = plan.get("Plan").unwrap_or(plan);
        walk_pg_node(node, &mut stats);
    }
    make_result(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::explain::ExplainTier;
    use serde_json::json;

    #[test]
    fn test_seq_scan_detected() {
        let arr = json!([{
            "Node Type": "Seq Scan",
            "Relation Name": "users",
            "Alias": "users",
            "Startup Cost": 0.0,
            "Total Cost": 35.5,
            "Plan Rows": 1000,
            "Plan Width": 40
        }]);
        let result = parse_postgres_explain(&arr).unwrap();
        assert!(result.full_table_scan, "Seq Scan should be flagged as full table scan");
        assert!(result.index_used.is_none());
        assert_eq!(result.rows_examined_estimate, 1000);
    }

    #[test]
    fn test_index_scan_detected() {
        let arr = json!([{
            "Node Type": "Index Scan",
            "Relation Name": "users",
            "Alias": "users",
            "Index Name": "idx_users_email",
            "Startup Cost": 0.28,
            "Total Cost": 8.30,
            "Plan Rows": 1,
            "Plan Width": 40
        }]);
        let result = parse_postgres_explain(&arr).unwrap();
        assert!(!result.full_table_scan, "Index Scan should not be full table scan");
        assert_eq!(result.index_used.as_deref(), Some("idx_users_email"));
        assert_eq!(result.rows_examined_estimate, 1);
        assert_eq!(result.tier, ExplainTier::Fast);
    }

    #[test]
    fn test_index_only_scan() {
        let arr = json!([{
            "Node Type": "Index Only Scan",
            "Relation Name": "items",
            "Index Name": "items_pkey",
            "Plan Rows": 5,
            "Plan Width": 0
        }]);
        let result = parse_postgres_explain(&arr).unwrap();
        assert!(!result.full_table_scan);
        assert_eq!(result.index_used.as_deref(), Some("items_pkey"));
    }

    #[test]
    fn test_bitmap_scan() {
        let arr = json!([{
            "Node Type": "Bitmap Heap Scan",
            "Relation Name": "orders",
            "Index Name": "idx_orders_status",
            "Plan Rows": 50,
            "Plan Width": 20,
            "Plans": [{
                "Node Type": "Bitmap Index Scan",
                "Index Name": "idx_orders_status",
                "Plan Rows": 50
            }]
        }]);
        let result = parse_postgres_explain(&arr).unwrap();
        assert!(!result.full_table_scan);
        assert_eq!(result.index_used.as_deref(), Some("idx_orders_status"));
    }

    #[test]
    fn test_nested_loop_join() {
        let arr = json!([{
            "Node Type": "Nested Loop",
            "Plan Rows": 10,
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "users",
                    "Plan Rows": 100
                },
                {
                    "Node Type": "Index Scan",
                    "Relation Name": "orders",
                    "Index Name": "idx_orders_user_id",
                    "Plan Rows": 1
                }
            ]
        }]);
        let result = parse_postgres_explain(&arr).unwrap();
        assert!(result.full_table_scan, "Seq Scan child should be detected");
        assert_eq!(result.index_used.as_deref(), Some("idx_orders_user_id"));
        assert_eq!(result.rows_examined_estimate, 111);
    }

    #[test]
    fn test_sort_detected() {
        let arr = json!([{
            "Node Type": "Sort",
            "Sort Key": ["name"],
            "Plan Rows": 500,
            "Plans": [{
                "Node Type": "Seq Scan",
                "Relation Name": "items",
                "Plan Rows": 500
            }]
        }]);
        let result = parse_postgres_explain(&arr).unwrap();
        assert!(result.full_table_scan);
        assert!(
            result.extra_flags.contains(&"Using filesort"),
            "Sort node should be flagged"
        );
    }

    #[test]
    fn test_hash_aggregate_temp() {
        let arr = json!([{
            "Node Type": "HashAggregate",
            "Plan Rows": 10,
            "Plans": [{
                "Node Type": "Seq Scan",
                "Plan Rows": 1000
            }]
        }]);
        let result = parse_postgres_explain(&arr).unwrap();
        assert!(result.full_table_scan);
        assert!(
            result.extra_flags.contains(&"Using temporary"),
            "HashAggregate should be flagged as temporary"
        );
    }

    #[test]
    fn test_tier_fast() {
        let arr = json!([{
            "Node Type": "Index Scan",
            "Plan Rows": 5
        }]);
        let result = parse_postgres_explain(&arr).unwrap();
        assert_eq!(result.tier, ExplainTier::Fast);
    }

    #[test]
    fn test_tier_slow() {
        let arr = json!([{
            "Node Type": "Seq Scan",
            "Plan Rows": 2000
        }]);
        let result = parse_postgres_explain(&arr).unwrap();
        assert_eq!(result.tier, ExplainTier::Slow);
    }

    #[test]
    fn test_tier_very_slow() {
        let arr = json!([{
            "Node Type": "Seq Scan",
            "Plan Rows": 50000
        }]);
        let result = parse_postgres_explain(&arr).unwrap();
        assert_eq!(result.tier, ExplainTier::VerySlow);
    }

    #[test]
    fn test_empty_array_errors() {
        let arr = json!([]);
        let result = parse_postgres_explain(&arr);
        assert!(result.is_err());
    }

    #[test]
    fn test_non_array_errors() {
        let arr = json!({"Node Type": "Seq Scan"});
        let result = parse_postgres_explain(&arr);
        assert!(result.is_err());
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL EXPLAIN integration tests (live DB)
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "postgres"))]
mod pg_integration_tests {
    use crate::test_helpers::setup_pg_test_db;

    #[tokio::test]
    async fn test_pg_explain_simple_select() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        let result = test_db
            .backend
            .run_explain(
                &test_db.pool_handle,
                "SELECT table_name FROM information_schema.tables LIMIT 5",
                0,
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
    async fn test_pg_explain_full_table_scan_detected() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pg_explain_test_fts (id SERIAL PRIMARY KEY, val VARCHAR(50))",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO pg_explain_test_fts (val) VALUES ('hello'), ('world') ON CONFLICT DO NOTHING",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();

        // Analyze to update statistics so the planner has accurate data
        sqlx::query("ANALYZE pg_explain_test_fts")
            .execute(&test_db.pool)
            .await
            .unwrap();

        let result = test_db
            .backend
            .run_explain(
                &test_db.pool_handle,
                "SELECT * FROM pg_explain_test_fts WHERE val = 'hello'",
                0,
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
            "should be a full table scan (Seq Scan) on unindexed column"
        );
        assert!(er.index_used.is_none(), "no index should be used");
        assert!(er.rows_examined_estimate >= 1);

        sqlx::query("DROP TABLE IF EXISTS pg_explain_test_fts")
            .execute(&test_db.pool)
            .await
            .ok();
    }

    #[tokio::test]
    async fn test_pg_explain_index_scan_detected() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pg_explain_test_idx (id SERIAL PRIMARY KEY, val VARCHAR(50))",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();

        sqlx::query("CREATE INDEX idx_pg_explain_val ON pg_explain_test_idx (val)")
            .execute(&test_db.pool)
            .await
            .unwrap();

        // Insert enough rows so PG's planner prefers an index scan over a seq scan.
        // With very few rows, PG will choose seq scan regardless of the index.
        for batch in 0..10 {
            let values: Vec<String> = (0..1000)
                .map(|i| format!("('val_{}_{}')", batch, i))
                .collect();
            sqlx::query(&format!(
                "INSERT INTO pg_explain_test_idx (val) VALUES {} ON CONFLICT DO NOTHING",
                values.join(",")
            ))
            .execute(&test_db.pool)
            .await
            .unwrap();
        }
        // Insert the target value
        sqlx::query("INSERT INTO pg_explain_test_idx (val) VALUES ('hello') ON CONFLICT DO NOTHING")
            .execute(&test_db.pool)
            .await
            .unwrap();

        sqlx::query("ANALYZE pg_explain_test_idx")
            .execute(&test_db.pool)
            .await
            .unwrap();

        let result = test_db
            .backend
            .run_explain(
                &test_db.pool_handle,
                "SELECT * FROM pg_explain_test_idx WHERE val = 'hello'",
                0,
            )
            .await;
        assert!(
            result.is_ok(),
            "run_explain should succeed: {:?}",
            result.err()
        );
        let er = result.unwrap();
        assert!(
            !er.full_table_scan,
            "should NOT be a full table scan when index is available"
        );
        assert!(
            er.index_used.is_some(),
            "an index should be used: {:?}",
            er.index_used
        );

        sqlx::query("DROP TABLE IF EXISTS pg_explain_test_idx")
            .execute(&test_db.pool)
            .await
            .ok();
    }

    #[tokio::test]
    async fn test_pg_explain_join_query() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pg_explain_join_a (id SERIAL PRIMARY KEY, name VARCHAR(50))",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pg_explain_join_b (id SERIAL PRIMARY KEY, a_id INT, score INT)",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        sqlx::query("CREATE INDEX idx_pg_join_b_aid ON pg_explain_join_b (a_id)")
            .execute(&test_db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO pg_explain_join_a (name) VALUES ('Alice'), ('Bob')")
            .execute(&test_db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO pg_explain_join_b (a_id, score) VALUES (1, 100), (1, 200), (2, 50)")
            .execute(&test_db.pool)
            .await
            .unwrap();
        sqlx::query("ANALYZE pg_explain_join_a, pg_explain_join_b")
            .execute(&test_db.pool)
            .await
            .unwrap();

        let result = test_db
            .backend
            .run_explain(
                &test_db.pool_handle,
                "SELECT a.name, b.score FROM pg_explain_join_a a JOIN pg_explain_join_b b ON a.id = b.a_id",
                0,
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

        sqlx::query("DROP TABLE IF EXISTS pg_explain_join_a, pg_explain_join_b")
            .execute(&test_db.pool)
            .await
            .ok();
    }

    #[tokio::test]
    async fn test_pg_explain_sort_detected() {
        let Some(test_db) = setup_pg_test_db().await else {
            return;
        };
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pg_explain_sort (id SERIAL PRIMARY KEY, name VARCHAR(50))",
        )
        .execute(&test_db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO pg_explain_sort (name) VALUES ('Zara'), ('Alice'), ('Mike')")
            .execute(&test_db.pool)
            .await
            .unwrap();
        sqlx::query("ANALYZE pg_explain_sort")
            .execute(&test_db.pool)
            .await
            .unwrap();

        let result = test_db
            .backend
            .run_explain(
                &test_db.pool_handle,
                "SELECT * FROM pg_explain_sort ORDER BY name",
                0,
            )
            .await;
        assert!(
            result.is_ok(),
            "run_explain should succeed: {:?}",
            result.err()
        );
        let er = result.unwrap();
        assert!(
            er.extra_flags.contains(&"Using filesort"),
            "Sort node should produce filesort flag: {:?}",
            er.extra_flags
        );

        sqlx::query("DROP TABLE IF EXISTS pg_explain_sort")
            .execute(&test_db.pool)
            .await
            .ok();
    }
}
