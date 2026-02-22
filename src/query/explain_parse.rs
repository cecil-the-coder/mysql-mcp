use anyhow::Result;
use serde_json::Value;

use super::explain::ExplainResult;

const VERY_SLOW_ROW_THRESHOLD: u64 = 10_000;
const SLOW_ROW_THRESHOLD: u64 = 1_000;

#[derive(Default)]
struct PlanStats {
    has_full_table_scan: bool,
    /// First index name encountered during the tree walk, if any.
    index_name: Option<String>,
    total_estimated_rows: f64,
    has_sort: bool,
}

/// Recursively walk a query_plan node (MySQL 8.0 EXPLAIN FORMAT=JSON schema v2).
///
/// Each node has:
///   - "access_type": "table" | "index" | "join" | "filter" | "sort" | "limit" |
///                    "rows_fetched_before_execution" | ...
///   - "inputs": [ <child nodes> ]
///   - "estimated_rows": f64
///   - "index_name": str   (for index nodes)
fn walk_plan_node(node: &Value, stats: &mut PlanStats) {
    match node["access_type"].as_str().unwrap_or("") {
        "table" => {
            stats.has_full_table_scan = true;
            stats.total_estimated_rows += node["estimated_rows"].as_f64().unwrap_or(0.0);
        }
        "index" => {
            // Short-circuit: only record the first index name encountered.
            if stats.index_name.is_none() {
                stats.index_name = node["index_name"].as_str().map(str::to_string);
            }
            stats.total_estimated_rows += node["estimated_rows"].as_f64().unwrap_or(0.0);
        }
        "sort" => {
            stats.has_sort = true;
            // Rows for sort come from children; don't double-count here.
        }
        "filter" => {
            // Filter node wraps child scan; rows come from the child.
        }
        "rows_fetched_before_execution" => {
            // Constant / const-optimized query — rows resolved at parse time.
            stats.total_estimated_rows += node["estimated_rows"].as_f64().unwrap_or(1.0);
        }
        _ => {}
    }

    if let Some(inputs) = node["inputs"].as_array() {
        for child in inputs {
            walk_plan_node(child, stats);
        }
    }
}

/// Parse MySQL 8.0 EXPLAIN FORMAT=JSON schema v2.
///
/// Structure (abbreviated):
/// ```json
/// {
///   "query_plan": {
///     "access_type": "filter" | "join" | "sort" | "limit" | "table" | "index" | ...,
///     "estimated_rows": <f64>,
///     "inputs": [ <recursive nodes> ],
///     "index_name": "<name>"   -- only on index nodes
///   },
///   "query_type": "select",
///   "json_schema_version": "2.0"
/// }
/// ```
pub(crate) fn parse_v2(v: &Value) -> Result<ExplainResult> {
    let mut stats = PlanStats::default();
    walk_plan_node(&v["query_plan"], &mut stats);

    let full_table_scan = stats.has_full_table_scan;
    let rows_examined_estimate = stats.total_estimated_rows.ceil() as u64;
    let extra_flags = if stats.has_sort { vec!["Using filesort"] } else { vec![] };

    let tier = if full_table_scan && rows_examined_estimate > VERY_SLOW_ROW_THRESHOLD {
        "very_slow"
    } else if full_table_scan || rows_examined_estimate > SLOW_ROW_THRESHOLD {
        "slow"
    } else {
        "fast"
    };

    Ok(ExplainResult {
        full_table_scan,
        index_used: stats.index_name,
        rows_examined_estimate,
        extra_flags,
        tier,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_v2(query_plan: serde_json::Value) -> serde_json::Value {
        json!({
            "query": "SELECT ...",
            "query_plan": query_plan,
            "query_type": "select",
            "json_schema_version": "2.0"
        })
    }

    #[test]
    fn test_v2_full_table_scan() {
        // Filter wrapping a table scan — typical for WHERE on unindexed column
        let v = make_v2(json!({
            "inputs": [
                {
                    "operation": "Table scan on test_perf",
                    "table_name": "test_perf",
                    "access_type": "table",
                    "schema_name": "mcp_test",
                    "estimated_rows": 5.0,
                    "estimated_total_cost": 0.75
                }
            ],
            "condition": "(test_perf.category = 'A')",
            "operation": "Filter: (test_perf.category = 'A')",
            "access_type": "filter",
            "estimated_rows": 1.0,
            "estimated_total_cost": 0.75
        }));
        let result = parse_v2(&v).unwrap();
        assert!(result.full_table_scan, "should detect full table scan");
        assert!(result.index_used.is_none(), "no index expected");
        assert_eq!(result.rows_examined_estimate, 5);
        assert!(result.extra_flags.is_empty());
        assert_eq!(result.tier, "slow"); // full_table_scan && rows <= 10_000
    }

    #[test]
    fn test_v2_index_lookup() {
        // Primary key lookup (rows_fetched_before_execution) or index access
        let v = make_v2(json!({
            "alias": "p",
            "covering": false,
            "operation": "Single-row index lookup on p using PRIMARY (id = 1)",
            "index_name": "PRIMARY",
            "table_name": "test_perf",
            "access_type": "index",
            "key_columns": ["id"],
            "schema_name": "mcp_test",
            "estimated_rows": 1.0,
            "index_access_type": "index_lookup",
            "estimated_total_cost": 0.25
        }));
        let result = parse_v2(&v).unwrap();
        assert!(!result.full_table_scan, "no full table scan");
        assert_eq!(result.index_used.as_deref(), Some("PRIMARY"));
        assert_eq!(result.rows_examined_estimate, 1);
        assert_eq!(result.tier, "fast");
    }

    #[test]
    fn test_v2_sort() {
        // Sort wrapping a table scan — adds "Using filesort"
        let v = make_v2(json!({
            "inputs": [
                {
                    "operation": "Table scan on test_perf",
                    "table_name": "test_perf",
                    "access_type": "table",
                    "schema_name": "mcp_test",
                    "estimated_rows": 5.0,
                    "estimated_total_cost": 0.75
                }
            ],
            "operation": "Sort: test_perf.`name`",
            "access_type": "sort",
            "sort_fields": ["test_perf.`name`"],
            "estimated_rows": 5.0,
            "estimated_total_cost": 0.75
        }));
        let result = parse_v2(&v).unwrap();
        assert!(result.full_table_scan, "child table scan detected");
        assert!(result.extra_flags.iter().any(|&s| s == "Using filesort"), "should flag filesort");
    }

    #[test]
    fn test_v2_nested_loop_join() {
        // JOIN: two inputs, one is a filter+table, one is an index lookup
        let v = make_v2(json!({
            "inputs": [
                {
                    "inputs": [
                        {
                            "alias": "p",
                            "operation": "Table scan on p",
                            "table_name": "test_perf",
                            "access_type": "table",
                            "schema_name": "mcp_test",
                            "estimated_rows": 5.0,
                            "estimated_total_cost": 0.75
                        }
                    ],
                    "condition": "(p.category = 'A')",
                    "operation": "Filter: (p.category = 'A')",
                    "access_type": "filter",
                    "estimated_rows": 1.0,
                    "estimated_total_cost": 0.75
                },
                {
                    "alias": "t",
                    "covering": false,
                    "operation": "Index lookup on t using idx_perf_id (perf_id = p.id)",
                    "index_name": "idx_perf_id",
                    "table_name": "test_perf2",
                    "access_type": "index",
                    "estimated_rows": 1.0,
                    "estimated_total_cost": 0.35
                }
            ],
            "join_type": "inner join",
            "operation": "Nested loop inner join",
            "access_type": "join",
            "estimated_rows": 1.0,
            "join_algorithm": "nested_loop",
            "estimated_total_cost": 1.1
        }));
        let result = parse_v2(&v).unwrap();
        assert!(result.full_table_scan, "table scan in join branch detected");
        assert_eq!(result.index_used.as_deref(), Some("idx_perf_id"));
        // 5 rows from table scan + 1 row from index lookup
        assert_eq!(result.rows_examined_estimate, 6);
    }

    #[test]
    fn test_v2_rows_fetched_before_execution() {
        // Constant query — optimizer resolves at parse time, 0 rows scanned
        let v = make_v2(json!({
            "operation": "Rows fetched before execution",
            "access_type": "rows_fetched_before_execution",
            "estimated_rows": 1.0,
            "estimated_total_cost": 0.0,
            "estimated_first_row_cost": 0.0
        }));
        let result = parse_v2(&v).unwrap();
        assert!(!result.full_table_scan, "no table scan for constant query");
        assert!(result.index_used.is_none());
        assert_eq!(result.rows_examined_estimate, 1);
        assert_eq!(result.tier, "fast");
    }
}
