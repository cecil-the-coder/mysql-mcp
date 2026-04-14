//! SQLite EXPLAIN QUERY PLAN parsing.
//!
//! SQLite's `EXPLAIN QUERY PLAN` returns multiple text rows describing the query
//! execution strategy. This module parses those text lines, extracts relevant
//! information (full table scans, index usage, sorts, temporary tables, etc.),
//! and maps it to the shared `ExplainResult` struct.

use anyhow::Result;

use super::explain::{ExplainResult, ExplainTier};

const VERY_SLOW_ROW_THRESHOLD: u64 = 10_000;
const SLOW_ROW_THRESHOLD: u64 = 1_000;

#[derive(Default)]
struct PlanStats {
    has_full_table_scan: bool,
    index_name: Option<String>,
    /// SQLite EXPLAIN QUERY PLAN does not provide row estimates directly.
    /// We track the number of table references to derive a rough heuristic.
    table_scan_count: u64,
    has_sort: bool,
    has_temporary: bool,
    has_index_merge: bool,
}

/// Parse a single EXPLAIN QUERY PLAN line from SQLite.
///
/// Lines look like:
/// - `SCAN TABLE users`
/// - `SEARCH TABLE users USING INDEX idx_users_email (email=?)`
/// - `SEARCH TABLE users USING INTEGER PRIMARY KEY (rowid=?)`
/// - `USE TEMP B-TREE FOR ORDER BY`
/// - `USE TEMP B-TREE FOR GROUP BY`
/// - `MULTI-INDEX OR`
/// - `SCAN TABLE users USING COVERING INDEX idx_users_name`
/// - `SEARCH TABLE users USING AUTOMATIC COVERING INDEX (name=?)`
/// - `SCAN SUBQUERY 1`
/// - `MATERIALIZE 1`
/// - `CO-ROUTINE 1`
/// - `LIST SUBQUERY 1`
fn parse_line(line: &str, stats: &mut PlanStats) {
    let upper = line.to_uppercase();

    // Detect sort (ORDER BY)
    if upper.contains("FOR ORDER BY") {
        stats.has_sort = true;
    }

    // Detect temporary table usage
    // "USE TEMP B-TREE FOR ..." — this is the canonical pattern
    if upper.contains("USE TEMP") || upper.contains("TEMP B-TREE") {
        stats.has_temporary = true;
    }
    // MATERIALIZE also creates a temporary result
    if upper.starts_with("MATERIALIZE") {
        stats.has_temporary = true;
    }

    // Detect index merge
    if upper.contains("MULTI-INDEX OR") {
        stats.has_index_merge = true;
    }

    // Detect full table scan (SCAN without an index).
    // SQLite >= 3.52 uses "SCAN t" (without TABLE), older versions use "SCAN TABLE t".
    let is_scan = upper.contains("SCAN TABLE")
        || (upper.starts_with("SCAN ") && !upper.contains("SCAN SUBQUERY"));
    if is_scan && !upper.contains("USING COVERING INDEX") {
        stats.has_full_table_scan = true;
        stats.table_scan_count += 1;
    }

    // Detect SCAN ... USING COVERING INDEX — uses index, not a full scan
    if is_scan && upper.contains("USING COVERING INDEX") && stats.index_name.is_none() {
        // Extract index name from "SCAN TABLE t USING COVERING INDEX idx_name ..."
        if let Some(idx) = extract_index_name(&upper, "COVERING INDEX") {
            stats.index_name = Some(idx);
        }
    }

    // Detect index search.
    // SQLite >= 3.52 uses "SEARCH t USING ..." (without TABLE), older versions use "SEARCH TABLE t".
    let is_search = upper.contains("SEARCH TABLE")
        || (upper.starts_with("SEARCH ") && !upper.contains("SEARCH TABLE"));
    if is_search && stats.index_name.is_none() {
        // "SEARCH TABLE t USING INDEX idx_name (col=?)"
        if let Some(idx) = extract_index_name(&upper, "INDEX") {
            stats.index_name = Some(idx);
        }
        // "SEARCH t USING COVERING INDEX idx_name (col=?)" (SQLite 3.52+)
        if stats.index_name.is_none() {
            if let Some(idx) = extract_index_name(&upper, "COVERING INDEX") {
                stats.index_name = Some(idx);
            }
        }
        // "SEARCH TABLE t USING INTEGER PRIMARY KEY (rowid=?)" — rowid lookup
        if stats.index_name.is_none() && upper.contains("INTEGER PRIMARY KEY") {
            stats.index_name = Some("__primary_key__".to_string());
        }
        // "SEARCH TABLE t USING AUTOMATIC COVERING INDEX"
        if stats.index_name.is_none() && upper.contains("AUTOMATIC COVERING INDEX") {
            stats.index_name = Some("__automatic_index__".to_string());
        }
    }

    // SCAN SUBQUERY / MATERIALIZE / CO-ROUTINE / LIST SUBQUERY
    if upper.contains("SCAN SUBQUERY") || upper.contains("MATERIALIZE") {
        stats.table_scan_count += 1;
    }
}

/// Extract the index name from a line like "... USING INDEX idx_name ..."
/// or "... USING COVERING INDEX idx_name ..."
fn extract_index_name(upper: &str, keyword: &str) -> Option<String> {
    let pattern = format!("USING {} ", keyword);
    let start = upper.find(&pattern)?;
    let rest = &upper[start + pattern.len()..];

    // Index name ends at whitespace or '('
    let end = rest.find([' ', '(']).unwrap_or(rest.len());
    let name = rest[..end].to_string();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// Derive row estimate from table scan count.
///
/// SQLite EXPLAIN QUERY PLAN doesn't provide row estimates, so we use
/// the number of table/subquery scans as a rough heuristic.
fn estimate_rows(stats: &PlanStats) -> u64 {
    // With multiple table scans (joins), the estimated rows grow multiplicatively.
    // This is a very rough heuristic since SQLite doesn't provide estimates.
    // Thresholds: Fast <= 1000, Slow <= 10000, VerySlow > 10000
    match stats.table_scan_count {
        0 => 1,
        1 => 100,
        2 => 2_000,  // > SLOW_ROW_THRESHOLD (1_000) -> Slow
        3 => 20_000, // > VERY_SLOW_ROW_THRESHOLD (10_000) -> VerySlow
        _ => 100_000,
    }
}

fn make_result(stats: PlanStats) -> Result<ExplainResult> {
    let rows_examined_estimate = estimate_rows(&stats);

    let mut extra_flags = vec![];
    if stats.has_sort {
        extra_flags.push("Using filesort");
    }
    if stats.has_temporary {
        extra_flags.push("Using temporary");
    }
    if stats.has_index_merge {
        extra_flags.push("Using index merge");
    }

    let tier = if rows_examined_estimate > VERY_SLOW_ROW_THRESHOLD {
        ExplainTier::VerySlow
    } else if rows_examined_estimate > SLOW_ROW_THRESHOLD {
        ExplainTier::Slow
    } else {
        ExplainTier::Fast
    };

    Ok(ExplainResult {
        full_table_scan: stats.has_full_table_scan,
        index_used: stats.index_name,
        rows_examined_estimate,
        extra_flags,
        tier,
    })
}

/// Parse SQLite EXPLAIN QUERY PLAN output (multiple text lines).
///
/// Each line is a row from the `EXPLAIN QUERY PLAN ...` query result,
/// containing a `detail` column with the plan description.
pub fn parse_sqlite_explain(lines: &[String]) -> Result<ExplainResult> {
    let mut stats = PlanStats::default();
    for line in lines {
        parse_line(line, &mut stats);
    }
    make_result(stats)
}

/// Parse SQLite EXPLAIN QUERY PLAN from RowData (extracts "detail" column).
pub fn parse_sqlite_explain_from_rows(rows: &[crate::backend::RowData]) -> Result<ExplainResult> {
    let lines: Vec<String> = rows
        .iter()
        .filter_map(|row| {
            row.columns
                .iter()
                .find(|(name, _)| name == "detail")
                .and_then(|(_, v)| v.as_str().map(String::from))
        })
        .collect();

    if lines.is_empty() {
        anyhow::bail!("SQLite EXPLAIN QUERY PLAN returned no detail lines");
    }

    parse_sqlite_explain(&lines)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::explain::ExplainTier;

    #[test]
    fn test_full_table_scan() {
        let lines = vec!["SCAN TABLE users".to_string()];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert!(result.full_table_scan);
        assert!(result.index_used.is_none());
    }

    #[test]
    fn test_index_scan() {
        let lines = vec!["SEARCH TABLE users USING INDEX idx_users_email (email=?)".to_string()];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert!(!result.full_table_scan);
        assert_eq!(result.index_used.as_deref(), Some("IDX_USERS_EMAIL"));
    }

    #[test]
    fn test_covering_index_scan() {
        let lines = vec!["SCAN TABLE items USING COVERING INDEX idx_items_pkey".to_string()];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert!(!result.full_table_scan);
        assert_eq!(result.index_used.as_deref(), Some("IDX_ITEMS_PKEY"));
    }

    #[test]
    fn test_primary_key_lookup() {
        let lines = vec!["SEARCH TABLE users USING INTEGER PRIMARY KEY (rowid=?)".to_string()];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert!(!result.full_table_scan);
        assert_eq!(result.index_used.as_deref(), Some("__primary_key__"));
    }

    #[test]
    fn test_automatic_covering_index() {
        let lines = vec!["SEARCH TABLE t USING AUTOMATIC COVERING INDEX (name=?)".to_string()];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert!(!result.full_table_scan);
        assert_eq!(result.index_used.as_deref(), Some("__automatic_index__"));
    }

    #[test]
    fn test_sort_detected() {
        let lines = vec![
            "SCAN TABLE items".to_string(),
            "USE TEMP B-TREE FOR ORDER BY".to_string(),
        ];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert!(result.full_table_scan);
        assert!(result.extra_flags.contains(&"Using filesort"));
        assert!(result.extra_flags.contains(&"Using temporary"));
    }

    #[test]
    fn test_group_by_temp() {
        let lines = vec![
            "SCAN TABLE orders".to_string(),
            "USE TEMP B-TREE FOR GROUP BY".to_string(),
        ];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert!(result.extra_flags.contains(&"Using temporary"));
    }

    #[test]
    fn test_multi_index_or() {
        let lines = vec!["MULTI-INDEX OR".to_string()];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert!(result.extra_flags.contains(&"Using index merge"));
    }

    #[test]
    fn test_nested_loop_join() {
        let lines = vec![
            "SEARCH TABLE orders USING INDEX idx_orders_user_id (user_id=?)".to_string(),
            "SEARCH TABLE users USING INTEGER PRIMARY KEY (rowid=?)".to_string(),
        ];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert!(!result.full_table_scan);
        // First index found wins
        assert_eq!(result.index_used.as_deref(), Some("IDX_ORDERS_USER_ID"));
    }

    #[test]
    fn test_tier_fast() {
        let lines = vec!["SEARCH TABLE t USING INDEX idx_t (id=?)".to_string()];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert_eq!(result.tier, ExplainTier::Fast);
    }

    #[test]
    fn test_tier_slow() {
        // Two table scans -> estimate 1000 rows -> Slow
        let lines = vec!["SCAN TABLE a".to_string(), "SCAN TABLE b".to_string()];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert_eq!(result.tier, ExplainTier::Slow);
    }

    #[test]
    fn test_tier_very_slow() {
        // Three table scans -> estimate 10000 rows -> VerySlow
        let lines = vec![
            "SCAN TABLE a".to_string(),
            "SCAN TABLE b".to_string(),
            "SCAN TABLE c".to_string(),
        ];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert_eq!(result.tier, ExplainTier::VerySlow);
    }

    #[test]
    fn test_empty_lines_error() {
        let lines: Vec<String> = vec![];
        let result = parse_sqlite_explain(&lines).unwrap();
        // No scans detected, no index used — Fast tier
        assert!(!result.full_table_scan);
        assert_eq!(result.tier, ExplainTier::Fast);
    }

    #[test]
    fn test_materialize_and_subquery() {
        let lines = vec![
            "SCAN SUBQUERY 1".to_string(),
            "MATERIALIZE 1".to_string(),
            "SCAN TABLE items USING COVERING INDEX idx_items".to_string(),
        ];
        let result = parse_sqlite_explain(&lines).unwrap();
        assert!(!result.full_table_scan);
        assert!(result.extra_flags.contains(&"Using temporary"));
        assert_eq!(result.index_used.as_deref(), Some("IDX_ITEMS"));
    }
}
