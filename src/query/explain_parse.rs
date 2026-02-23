use anyhow::Result;
use serde_json::Value;

use super::explain::{ExplainResult, ExplainTier};

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
///     "rows_fetched_before_execution" | ...
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

/// Convert accumulated PlanStats into a final ExplainResult.
fn make_result(stats: PlanStats) -> Result<ExplainResult> {
    let full_table_scan = stats.has_full_table_scan;
    // Guard against NaN / Infinity before casting to u64: non-finite values
    // produce u64::MAX (Infinity) or 0 (NaN) in Rust's as-cast, both wrong.
    let rows_examined_estimate = if stats.total_estimated_rows.is_finite() {
        stats.total_estimated_rows.ceil() as u64
    } else {
        tracing::warn!(
            estimated_rows = stats.total_estimated_rows,
            "EXPLAIN row count estimate is non-finite (Infinity or NaN); reporting as 0"
        );
        0
    };
    let extra_flags = if stats.has_sort {
        vec!["Using filesort"]
    } else {
        vec![]
    };

    // Tier is determined by estimated row count alone. The full_table_scan flag is
    // surfaced separately in the output; even a full scan of a 5-row table is fast.
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
    make_result(stats)
}

// ---------------------------------------------------------------------------
// Schema v1 parser — MySQL 5.7 / 8.0 EXPLAIN FORMAT=JSON
// ---------------------------------------------------------------------------

/// Walk a single `table` leaf in schema v1.
///
/// | Field                   | Meaning                                  |
/// |-------------------------|------------------------------------------|
/// | `access_type`           | "ALL" = full scan, others use an index   |
/// | `key`                   | index name when access uses one          |
/// | `rows_examined_per_scan`| row-count estimate for this table        |
/// | `using_filesort`        | true when filesort is applied            |
fn walk_v1_table(table: &Value, stats: &mut PlanStats) {
    if table["access_type"].as_str().unwrap_or("") == "ALL" {
        stats.has_full_table_scan = true;
    }
    if let Some(key) = table["key"].as_str() {
        if stats.index_name.is_none() {
            stats.index_name = Some(key.to_string());
        }
    }
    let rows = table["rows_examined_per_scan"]
        .as_f64()
        .or_else(|| table["rows"].as_f64())
        .unwrap_or_else(|| {
            tracing::warn!(
                "EXPLAIN v1 table node missing row count fields \
                 (rows_examined_per_scan, rows); treating as 0"
            );
            0.0
        });

    if table["using_filesort"].as_bool().unwrap_or(false) {
        stats.has_sort = true;
    }
    // Derived / materialized subquery inside a table node.
    // The outer table's `rows` is the size of the already-materialized result,
    // not real scan work.  Only count rows from inside the subquery to avoid
    // double-counting; if there is no subquery, count the rows normally.
    if let Some(mat) = table.get("materialized_from_subquery") {
        if let Some(qb) = mat.get("query_block") {
            walk_v1_block(qb, stats);
        }
    } else {
        stats.total_estimated_rows += rows;
    }
}

/// Recursively walk a schema v1 query_block node.
fn walk_v1_block(node: &Value, stats: &mut PlanStats) {
    if let Some(table) = node.get("table") {
        walk_v1_table(table, stats);
    }
    // JOIN: nested_loop is an array of per-table wrappers
    if let Some(nl) = node["nested_loop"].as_array() {
        for item in nl {
            walk_v1_block(item, stats);
        }
    }
    // ORDER BY (may have using_filesort at the operation level)
    if let Some(ordering) = node.get("ordering_operation") {
        if ordering["using_filesort"].as_bool().unwrap_or(false) {
            stats.has_sort = true;
        }
        walk_v1_block(ordering, stats);
    }
    // GROUP BY
    if let Some(grouping) = node.get("grouping_operation") {
        walk_v1_block(grouping, stats);
    }
    // UNION
    if let Some(union) = node.get("union_result") {
        if let Some(specs) = union["query_specifications"].as_array() {
            for spec in specs {
                if let Some(qb) = spec.get("query_block") {
                    walk_v1_block(qb, stats);
                }
            }
        }
    }
}

/// Parse MySQL 5.7 / 8.0 EXPLAIN FORMAT=JSON schema v1.
fn parse_v1(v: &Value) -> Result<ExplainResult> {
    let mut stats = PlanStats::default();
    if let Some(qb) = v.get("query_block") {
        walk_v1_block(qb, &mut stats);
    }
    make_result(stats)
}

// ---------------------------------------------------------------------------
// Public dispatcher
// ---------------------------------------------------------------------------

/// Parse `EXPLAIN FORMAT=JSON` output from any supported MySQL version.
///
/// Dispatches to schema v2 (`query_plan` key, MySQL 8.0.16+ / 9.x) or
/// schema v1 (`query_block` key, MySQL 5.7 / 8.0.x) based on JSON structure.
pub(crate) fn parse(v: &Value) -> Result<ExplainResult> {
    if v["query_plan"].is_object() {
        // MySQL 8.0.16+ / 9.x schema v2
        parse_v2(v)
    } else if v.get("query_block").is_some() {
        // MySQL 5.7 / 8.0 schema v1
        parse_v1(v)
    } else {
        // Neither structure found — return an error so the caller surfaces
        // explain_error instead of a misleading tier:fast / rows:0 result.
        anyhow::bail!(
            "Unrecognized EXPLAIN FORMAT=JSON structure \
             (has neither 'query_plan' nor 'query_block')"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::explain::ExplainTier;
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
        assert_eq!(result.tier, ExplainTier::Fast); // 5 rows is below SLOW_ROW_THRESHOLD
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
        assert_eq!(result.tier, ExplainTier::Fast);
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
        assert!(
            result.extra_flags.iter().any(|&s| s == "Using filesort"),
            "should flag filesort"
        );
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
        assert_eq!(result.tier, ExplainTier::Fast);
    }

    // -----------------------------------------------------------------------
    // Schema v1 unit tests (MySQL 5.7 / 8.0 query_block format)
    // -----------------------------------------------------------------------

    fn make_v1(query_block: serde_json::Value) -> serde_json::Value {
        json!({ "query_block": query_block })
    }

    #[test]
    fn test_v1_full_table_scan() {
        let v = make_v1(json!({
            "select_id": 1,
            "table": {
                "table_name": "t",
                "access_type": "ALL",
                "rows_examined_per_scan": 5,
                "rows_produced_per_join": 5,
                "filtered": "100.00"
            }
        }));
        let result = parse(&v).unwrap();
        assert!(result.full_table_scan, "ALL access_type → full table scan");
        assert!(result.index_used.is_none(), "no index for ALL scan");
        assert_eq!(result.rows_examined_estimate, 5);
    }

    #[test]
    fn test_v1_index_lookup() {
        let v = make_v1(json!({
            "select_id": 1,
            "table": {
                "table_name": "t",
                "access_type": "ref",
                "possible_keys": ["idx_val"],
                "key": "idx_val",
                "key_length": "203",
                "ref": ["const"],
                "rows_examined_per_scan": 1,
                "filtered": "100.00"
            }
        }));
        let result = parse(&v).unwrap();
        assert!(
            !result.full_table_scan,
            "ref access_type → no full table scan"
        );
        assert_eq!(result.index_used.as_deref(), Some("idx_val"));
        assert_eq!(result.rows_examined_estimate, 1);
    }

    #[test]
    fn test_v1_nested_loop_join() {
        // Typical JOIN: outer table is ALL, inner uses an index
        let v = make_v1(json!({
            "select_id": 1,
            "nested_loop": [
                {
                    "table": {
                        "table_name": "a",
                        "access_type": "ALL",
                        "rows_examined_per_scan": 2
                    }
                },
                {
                    "table": {
                        "table_name": "b",
                        "access_type": "ref",
                        "key": "idx_a_id",
                        "rows_examined_per_scan": 2
                    }
                }
            ]
        }));
        let result = parse(&v).unwrap();
        assert!(
            result.full_table_scan,
            "outer table is ALL → full table scan"
        );
        assert_eq!(result.index_used.as_deref(), Some("idx_a_id"));
        assert_eq!(result.rows_examined_estimate, 4);
    }

    #[test]
    fn test_v1_sort_with_filesort() {
        let v = make_v1(json!({
            "select_id": 1,
            "ordering_operation": {
                "using_filesort": true,
                "table": {
                    "table_name": "t",
                    "access_type": "ALL",
                    "rows_examined_per_scan": 3
                }
            }
        }));
        let result = parse(&v).unwrap();
        assert!(result.full_table_scan);
        assert!(
            result.extra_flags.iter().any(|&s| s == "Using filesort"),
            "filesort flagged"
        );
        assert_eq!(result.rows_examined_estimate, 3);
    }

    #[test]
    fn test_v1_primary_key_lookup() {
        // eq_ref on PRIMARY — very fast
        let v = make_v1(json!({
            "select_id": 1,
            "table": {
                "table_name": "t",
                "access_type": "eq_ref",
                "key": "PRIMARY",
                "rows_examined_per_scan": 1
            }
        }));
        let result = parse(&v).unwrap();
        assert!(!result.full_table_scan);
        assert_eq!(result.index_used.as_deref(), Some("PRIMARY"));
        assert_eq!(result.rows_examined_estimate, 1);
        assert_eq!(result.tier, ExplainTier::Fast);
    }

    #[test]
    fn test_dispatcher_picks_v2_when_query_plan_present() {
        // make_v2 produces a v2 envelope — dispatcher should route to parse_v2
        let v = make_v2(json!({
            "access_type": "table",
            "estimated_rows": 7.0,
            "operation": "Table scan on t",
            "table_name": "t"
        }));
        let result = parse(&v).unwrap();
        assert!(result.full_table_scan);
        assert_eq!(result.rows_examined_estimate, 7);
    }

    #[test]
    fn test_v1_materialized_subquery_no_double_count() {
        // A derived table (<derived2>) wraps a full scan of big_table.
        // The outer table's rows_examined_per_scan (100) is just the size of the
        // materialized result — it must NOT be added on top of big_table's 1_000_000.
        // Expected total: 1_000_000, not 1_000_100.
        let v = make_v1(json!({
            "select_id": 1,
            "nested_loop": [{
                "table": {
                    "table_name": "<derived2>",
                    "rows_examined_per_scan": 100,
                    "materialized_from_subquery": {
                        "query_block": {
                            "nested_loop": [{
                                "table": {
                                    "table_name": "big_table",
                                    "access_type": "ALL",
                                    "rows_examined_per_scan": 1000000
                                }
                            }]
                        }
                    }
                }
            }]
        }));
        let result = parse(&v).unwrap();
        assert!(
            result.full_table_scan,
            "full table scan inside subquery should be detected"
        );
        assert_eq!(
            result.rows_examined_estimate, 1_000_000,
            "outer derived rows must not be double-counted"
        );
    }

    #[test]
    fn test_parse_empty_object_returns_error() {
        let result = parse(&serde_json::json!({}));
        assert!(
            result.is_err(),
            "empty EXPLAIN JSON should return an error, not tier:fast"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("Unrecognized")
                || msg.contains("query_plan")
                || msg.contains("query_block"),
            "error message should mention the missing keys, got: {}",
            msg
        );
    }
}
