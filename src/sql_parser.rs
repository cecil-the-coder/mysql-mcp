use anyhow::{bail, Result};
use sqlparser::ast::{Expr, FromTable, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Use};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;

/// The type of SQL statement parsed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementType {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Alter,
    Drop,
    Truncate,
    Use,
    Show,
    /// EXPLAIN, DESCRIBE, etc. - read-only informational
    Explain,
    /// SET statements
    Set,
    /// Other statements we don't explicitly categorize
    Other(String),
}

impl StatementType {
    /// Returns true if this is a read-only statement (SELECT, SHOW, EXPLAIN, etc.)
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            StatementType::Select | StatementType::Show | StatementType::Explain
        )
    }

    /// Returns true if this is a write statement (INSERT, UPDATE, DELETE)
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            StatementType::Insert | StatementType::Update | StatementType::Delete
        )
    }

    /// Returns true if this is a DDL statement
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            StatementType::Create
                | StatementType::Alter
                | StatementType::Drop
                | StatementType::Truncate
        )
    }

    /// Human-readable name for error messages
    pub fn name(&self) -> &str {
        match self {
            StatementType::Select => "SELECT",
            StatementType::Insert => "INSERT",
            StatementType::Update => "UPDATE",
            StatementType::Delete => "DELETE",
            StatementType::Create => "CREATE",
            StatementType::Alter => "ALTER",
            StatementType::Drop => "DROP",
            StatementType::Truncate => "TRUNCATE",
            StatementType::Use => "USE",
            StatementType::Show => "SHOW",
            StatementType::Explain => "EXPLAIN",
            StatementType::Set => "SET",
            StatementType::Other(s) => s.as_str(),
        }
    }
}

/// Result of parsing a SQL statement
#[derive(Debug, Clone)]
pub struct ParsedStatement {
    pub statement_type: StatementType,
    /// The target schema/database extracted from the statement (if applicable)
    pub target_schema: Option<String>,
    /// The primary FROM table name for SELECT statements (if extractable)
    pub target_table: Option<String>,
    /// The original SQL string (used for heuristic-based analysis)
    pub sql: String,
}

/// Parse a SQL string and return the statement type and target schema.
/// Returns an error if the SQL is invalid or cannot be parsed.
pub fn parse_sql(sql: &str) -> Result<ParsedStatement> {
    let dialect = MySqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| anyhow::anyhow!("SQL parse error: {}", e))?;

    if statements.is_empty() {
        bail!("Empty SQL statement");
    }

    if statements.len() > 1 {
        bail!("Multi-statement SQL is not supported. Send one statement at a time.");
    }

    let stmt = &statements[0];
    classify_statement(stmt, sql)
}

fn classify_statement(stmt: &Statement, sql: &str) -> Result<ParsedStatement> {
    let (statement_type, target_schema, target_table) = match stmt {
        Statement::Query(query) => {
            let tname = extract_first_from_table_name(query);
            (StatementType::Select, None, tname)
        }

        Statement::Insert(insert) => {
            let schema = extract_schema_from_object_name(&insert.table_name);
            (StatementType::Insert, schema, None)
        }

        Statement::Update { table, .. } => {
            let schema = extract_schema_from_table_factor(&table.relation);
            (StatementType::Update, schema, None)
        }

        Statement::Delete(delete) => {
            let schema = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables
                    .first()
                    .and_then(|t| extract_schema_from_table_factor(&t.relation)),
            };
            (StatementType::Delete, schema, None)
        }

        Statement::CreateTable(ct) => {
            let schema = extract_schema_from_object_name(&ct.name);
            (StatementType::Create, schema, None)
        }

        Statement::CreateDatabase { db_name, .. } => {
            let schema = Some(db_name.to_string());
            (StatementType::Create, schema, None)
        }

        Statement::AlterTable { name, .. } => {
            let schema = extract_schema_from_object_name(name);
            (StatementType::Alter, schema, None)
        }

        Statement::Drop { names, .. } => {
            let schema = names
                .first()
                .and_then(|n| extract_schema_from_object_name(n));
            (StatementType::Drop, schema, None)
        }

        Statement::Truncate { table_names, .. } => {
            let schema = table_names
                .first()
                .and_then(|t| extract_schema_from_object_name(&t.name));
            (StatementType::Truncate, schema, None)
        }

        Statement::Use(use_stmt) => {
            let schema = match use_stmt {
                Use::Object(name) => Some(name.to_string()),
                Use::Database(name) => Some(name.to_string()),
                Use::Schema(name) => Some(name.to_string()),
                Use::Catalog(name) => Some(name.to_string()),
                _ => None,
            };
            (StatementType::Use, schema, None)
        }

        Statement::ShowTables { show_options, .. } => {
            let schema = show_options
                .show_in
                .as_ref()
                .and_then(|si| si.parent_name.as_ref())
                .map(|n| n.to_string());
            (StatementType::Show, schema, None)
        }

        Statement::ShowColumns { show_options, .. } => {
            let schema = show_options
                .show_in
                .as_ref()
                .and_then(|si| si.parent_name.as_ref())
                .map(|n| n.to_string());
            (StatementType::Show, schema, None)
        }

        Statement::ShowDatabases { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowViews { .. } => (StatementType::Show, None, None),

        Statement::ShowCreate { obj_name, .. } => {
            let schema = extract_schema_from_object_name(obj_name);
            (StatementType::Show, schema, None)
        }

        Statement::Explain { .. } => (StatementType::Explain, None, None),

        Statement::SetVariable { .. }
        | Statement::SetNames { .. }
        | Statement::SetNamesDefault { .. }
        | Statement::SetTimeZone { .. } => (StatementType::Set, None, None),

        other => {
            let name = format!("{:?}", std::mem::discriminant(other));
            (StatementType::Other(name), None, None)
        }
    };

    Ok(ParsedStatement {
        statement_type,
        target_schema,
        target_table,
        sql: sql.to_string(),
    })
}

/// Inspect a parsed write statement and return safety warnings.
/// Detects dangerous patterns: UPDATE/DELETE without WHERE, TRUNCATE.
pub fn parse_write_warnings(parsed: &ParsedStatement) -> Vec<String> {
    let mut warnings = Vec::new();

    match &parsed.statement_type {
        StatementType::Truncate => {
            warnings.push(
                "TRUNCATE will delete ALL rows without transaction log — cannot be rolled back"
                    .to_string(),
            );
            return warnings;
        }
        StatementType::Update | StatementType::Delete => {}
        _ => return warnings,
    }

    // Re-parse to inspect the AST for WHERE clause presence
    let dialect = MySqlDialect {};
    let statements = match Parser::parse_sql(&dialect, &parsed.sql) {
        Ok(s) => s,
        Err(_) => {
            // Fallback: use string heuristics
            let upper = parsed.sql.to_uppercase();
            let trimmed = upper.trim();
            if parsed.statement_type == StatementType::Update {
                if !trimmed.contains("WHERE") {
                    warnings.push(
                        "UPDATE has no WHERE clause — this will affect ALL rows in the table"
                            .to_string(),
                    );
                }
            } else if parsed.statement_type == StatementType::Delete {
                if !trimmed.contains("WHERE") {
                    warnings.push(
                        "DELETE has no WHERE clause — this will delete ALL rows in the table"
                            .to_string(),
                    );
                }
            }
            return warnings;
        }
    };

    let stmt = match statements.into_iter().next() {
        Some(s) => s,
        None => return warnings,
    };

    match stmt {
        Statement::Update { selection, .. } => {
            if selection.is_none() {
                warnings.push(
                    "UPDATE has no WHERE clause — this will affect ALL rows in the table"
                        .to_string(),
                );
            }
        }
        Statement::Delete(delete) => {
            if delete.selection.is_none() {
                warnings.push(
                    "DELETE has no WHERE clause — this will delete ALL rows in the table"
                        .to_string(),
                );
            }
        }
        _ => {}
    }

    warnings
}

/// Inspect a parsed SELECT statement and return human-readable performance warnings.
/// Only meaningful for SELECT statements; returns empty vec for other statement types.
pub fn parse_warnings(parsed: &ParsedStatement) -> Vec<String> {
    if parsed.statement_type != StatementType::Select {
        return vec![];
    }

    let dialect = MySqlDialect {};
    let statements = match Parser::parse_sql(&dialect, &parsed.sql) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let stmt = match statements.into_iter().next() {
        Some(s) => s,
        None => return vec![],
    };

    let query = match stmt {
        Statement::Query(q) => q,
        _ => return vec![],
    };

    let select = match extract_select(&query) {
        Some(s) => s,
        None => return vec![],
    };

    let mut warnings = Vec::new();

    // Compute shared flags used by multiple checks below.
    let has_limit = query.limit.is_some();
    let has_named_table = select.from.iter().any(|t| is_named_table(&t.relation));
    let has_where = select.selection.is_some();

    // 1. SELECT * (wildcard projection) — only warn when querying a real table without LIMIT,
    //    because `SELECT * FROM t LIMIT 10` is a reasonable exploration query.
    let has_wildcard = select.projection.iter().any(|item| {
        matches!(item, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _))
    });
    if has_wildcard && has_named_table && !has_limit {
        warnings.push(
            "SELECT * returns all columns — specify needed columns for efficiency".to_string(),
        );
    }

    // 2. SELECT from a named table with no WHERE clause and no LIMIT — a LIMIT prevents
    //    unbounded data return, making the query acceptable for exploration.
    if has_named_table && !has_where && !has_limit {
        warnings.push(
            "No WHERE clause and no LIMIT — query will scan the entire table".to_string(),
        );
    }

    // 3. LIKE with a leading wildcard — use SQL string heuristic (handles all LIKE forms)
    if has_leading_wildcard_like(&parsed.sql) {
        warnings.push(
            "Leading wildcard in LIKE — index on this column cannot be used".to_string(),
        );
    }

    // 4. No LIMIT on a non-aggregate SELECT
    let has_group_by = !matches!(&select.group_by, sqlparser::ast::GroupByExpr::Expressions(exprs, _) if exprs.is_empty());
    let has_aggregate = has_aggregate_function(&select.projection);
    if !has_limit && !has_group_by && !has_aggregate {
        warnings.push("No LIMIT clause — query may return unbounded rows".to_string());
    }

    // 5. 3+ table joins
    let join_count = count_joins(&select.from);
    if join_count >= 3 {
        warnings.push(format!(
            "Query joins {} tables — ensure join columns are indexed",
            join_count
        ));
    }

    warnings
}

/// Extract the innermost Select from a Query (handles simple SELECT; ignores UNION etc.)
fn extract_select(query: &Query) -> Option<&Select> {
    match query.body.as_ref() {
        SetExpr::Select(select) => Some(select),
        _ => None,
    }
}

/// Returns true if the table factor refers to a concrete named table (not a subquery/function).
fn is_named_table(factor: &TableFactor) -> bool {
    matches!(factor, TableFactor::Table { .. })
}

/// Count distinct table references across all FROM clauses including JOIN targets.
/// Returns the total number of tables (base + joined).
fn count_joins(from: &[TableWithJoins]) -> usize {
    from.iter()
        .map(|twj| 1 + twj.joins.len())
        .sum()
}

/// Returns true if any aggregate function (COUNT, SUM, AVG, MIN, MAX) appears in the projection.
fn has_aggregate_function(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return false,
        };
        expr_has_aggregate(expr)
    })
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_uppercase();
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => expr_has_aggregate(expr),
        Expr::Nested(inner) => expr_has_aggregate(inner),
        _ => false,
    }
}

/// Heuristic: detect LIKE patterns with a leading wildcard (e.g. LIKE '%foo' or LIKE '%foo%').
/// Uses the raw SQL string since extracting string literal values from the AST is verbose.
fn has_leading_wildcard_like(sql: &str) -> bool {
    // Normalise to uppercase for case-insensitive matching
    let upper = sql.to_uppercase();
    // Find all occurrences of LIKE followed by a quoted string starting with %
    let mut rest = upper.as_str();
    while let Some(pos) = rest.find("LIKE") {
        rest = &rest[pos + 4..];
        // Skip whitespace
        let trimmed = rest.trim_start();
        // Accept both ' and " delimiters
        let inner = if trimmed.starts_with('\'') {
            &trimmed[1..]
        } else if trimmed.starts_with('"') {
            &trimmed[1..]
        } else {
            continue;
        };
        if inner.starts_with('%') {
            return true;
        }
    }
    false
}

fn extract_schema_from_object_name(name: &ObjectName) -> Option<String> {
    // MySQL fully-qualified: schema.table has 2 parts; the first is schema
    if name.0.len() >= 2 {
        Some(name.0[0].value.clone())
    } else {
        None
    }
}

fn extract_schema_from_table_factor(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { name, .. } => extract_schema_from_object_name(name),
        _ => None,
    }
}

/// Extract the primary FROM table name from a SELECT query AST.
/// Returns only the bare table name (last identifier), ignoring any schema prefix.
fn extract_first_from_table_name(query: &Query) -> Option<String> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    let first_from = select.from.first()?;
    match &first_from.relation {
        TableFactor::Table { name, .. } => {
            // Last part of a (possibly qualified) name is the table name
            name.0.last().map(|ident| ident.value.clone())
        }
        _ => None,
    }
}

/// Extract column names referenced in the WHERE clause of a SELECT statement.
/// Returns simple column names (last segment of compound identifiers).
/// Returns an empty vec if the SQL is not a SELECT, has no WHERE clause, or parsing fails.
pub fn extract_where_columns(parsed: &ParsedStatement) -> Vec<String> {
    if parsed.statement_type != StatementType::Select {
        return vec![];
    }
    let dialect = MySqlDialect {};
    let statements = match Parser::parse_sql(&dialect, &parsed.sql) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let stmt = match statements.into_iter().next() {
        Some(s) => s,
        None => return vec![],
    };
    let query = match stmt {
        Statement::Query(q) => q,
        _ => return vec![],
    };
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return vec![],
    };
    let selection = match &select.selection {
        Some(expr) => expr,
        None => return vec![],
    };
    let mut cols: Vec<String> = vec![];
    let mut seen: HashSet<String> = HashSet::new();
    collect_where_columns(selection, &mut cols, &mut seen);
    cols
}

/// Recursively walk an expression tree and collect column name identifiers.
fn collect_where_columns(expr: &Expr, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    match expr {
        Expr::Identifier(ident) => {
            let name = ident.value.clone();
            if seen.insert(name.to_lowercase()) {
                cols.push(name);
            }
        }
        Expr::CompoundIdentifier(parts) => {
            // Take the last part as the column name (e.g., table.column -> column)
            if let Some(last) = parts.last() {
                let name = last.value.clone();
                if seen.insert(name.to_lowercase()) {
                    cols.push(name);
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_where_columns(left, cols, seen);
            collect_where_columns(right, cols, seen);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_where_columns(expr, cols, seen);
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_where_columns(inner, cols, seen);
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            collect_where_columns(left, cols, seen);
            collect_where_columns(right, cols, seen);
        }
        Expr::Between { expr, low, high, .. } => {
            collect_where_columns(expr, cols, seen);
            collect_where_columns(low, cols, seen);
            collect_where_columns(high, cols, seen);
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            collect_where_columns(expr, cols, seen);
            collect_where_columns(pattern, cols, seen);
        }
        Expr::InList { expr, .. } => {
            collect_where_columns(expr, cols, seen);
        }
        Expr::Nested(inner) => {
            collect_where_columns(inner, cols, seen);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select() {
        let p = parse_sql("SELECT * FROM users").unwrap();
        assert_eq!(p.statement_type, StatementType::Select);
        assert!(p.statement_type.is_read_only());
    }

    #[test]
    fn test_select_with_schema() {
        let p = parse_sql("SELECT * FROM mydb.users").unwrap();
        assert_eq!(p.statement_type, StatementType::Select);
        // schema extraction is from INSERT/UPDATE/DELETE/DDL, not SELECT
    }

    #[test]
    fn test_insert() {
        let p = parse_sql("INSERT INTO mydb.users (id, name) VALUES (1, 'Alice')").unwrap();
        assert_eq!(p.statement_type, StatementType::Insert);
        assert_eq!(p.target_schema, Some("mydb".to_string()));
        assert!(p.statement_type.is_write());
    }

    #[test]
    fn test_update() {
        let p = parse_sql("UPDATE mydb.users SET name = 'Bob' WHERE id = 1").unwrap();
        assert_eq!(p.statement_type, StatementType::Update);
        assert_eq!(p.target_schema, Some("mydb".to_string()));
    }

    #[test]
    fn test_delete() {
        let p = parse_sql("DELETE FROM mydb.orders WHERE id = 5").unwrap();
        assert_eq!(p.statement_type, StatementType::Delete);
        assert_eq!(p.target_schema, Some("mydb".to_string()));
    }

    #[test]
    fn test_create_table() {
        let p = parse_sql("CREATE TABLE mydb.t (id INT)").unwrap();
        assert_eq!(p.statement_type, StatementType::Create);
        assert!(p.statement_type.is_ddl());
        assert_eq!(p.target_schema, Some("mydb".to_string()));
    }

    #[test]
    fn test_alter_table() {
        let p = parse_sql("ALTER TABLE mydb.users ADD COLUMN age INT").unwrap();
        assert_eq!(p.statement_type, StatementType::Alter);
        assert_eq!(p.target_schema, Some("mydb".to_string()));
    }

    #[test]
    fn test_drop_table() {
        let p = parse_sql("DROP TABLE mydb.users").unwrap();
        assert_eq!(p.statement_type, StatementType::Drop);
        assert_eq!(p.target_schema, Some("mydb".to_string()));
    }

    #[test]
    fn test_truncate() {
        let p = parse_sql("TRUNCATE TABLE mydb.logs").unwrap();
        assert_eq!(p.statement_type, StatementType::Truncate);
        assert_eq!(p.target_schema, Some("mydb".to_string()));
    }

    #[test]
    fn test_use() {
        let p = parse_sql("USE mydb").unwrap();
        assert_eq!(p.statement_type, StatementType::Use);
    }

    #[test]
    fn test_show_tables() {
        let p = parse_sql("SHOW TABLES").unwrap();
        assert_eq!(p.statement_type, StatementType::Show);
        assert!(p.statement_type.is_read_only());
    }

    #[test]
    fn test_explain() {
        let p = parse_sql("EXPLAIN SELECT * FROM users").unwrap();
        assert_eq!(p.statement_type, StatementType::Explain);
        assert!(p.statement_type.is_read_only());
    }

    #[test]
    fn test_set_variable() {
        let p = parse_sql("SET @x = 1").unwrap();
        assert_eq!(p.statement_type, StatementType::Set);
    }

    #[test]
    fn test_empty_sql() {
        assert!(parse_sql("").is_err());
    }

    #[test]
    fn test_is_ddl() {
        assert!(StatementType::Create.is_ddl());
        assert!(StatementType::Alter.is_ddl());
        assert!(StatementType::Drop.is_ddl());
        assert!(StatementType::Truncate.is_ddl());
        assert!(!StatementType::Select.is_ddl());
    }

    #[test]
    fn test_names() {
        assert_eq!(StatementType::Select.name(), "SELECT");
        assert_eq!(StatementType::Insert.name(), "INSERT");
        assert_eq!(StatementType::Other("FOO".to_string()).name(), "FOO");
    }

    #[test]
    fn test_insert_no_schema() {
        let parsed = parse_sql("INSERT INTO users (name) VALUES ('test')").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Insert);
        assert!(parsed.target_schema.is_none());
    }

    #[test]
    fn test_update_no_schema() {
        let parsed = parse_sql("UPDATE users SET name='new' WHERE id=1").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Update);
        assert!(parsed.target_schema.is_none());
    }

    #[test]
    fn test_delete_no_schema() {
        let parsed = parse_sql("DELETE FROM users WHERE id=1").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Delete);
        assert!(parsed.target_schema.is_none());
    }

    #[test]
    fn test_create_table_no_schema() {
        let parsed = parse_sql("CREATE TABLE foo (id INT PRIMARY KEY)").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Create);
        assert!(parsed.target_schema.is_none());
    }

    #[test]
    fn test_alter_table_no_schema() {
        let parsed = parse_sql("ALTER TABLE foo ADD COLUMN bar VARCHAR(255)").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Alter);
        assert!(parsed.target_schema.is_none());
    }

    #[test]
    fn test_drop_table_no_schema() {
        let parsed = parse_sql("DROP TABLE foo").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Drop);
        assert!(parsed.target_schema.is_none());
    }

    #[test]
    fn test_truncate_no_schema() {
        let parsed = parse_sql("TRUNCATE TABLE foo").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Truncate);
        assert!(parsed.target_schema.is_none());
    }

    #[test]
    fn test_select_no_schema() {
        let parsed = parse_sql("SELECT * FROM users").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Select);
        assert!(parsed.target_schema.is_none());
    }

    #[test]
    fn test_invalid_sql() {
        let result = parse_sql("NOT VALID SQL !!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_detection_qualified() {
        let parsed = parse_sql("INSERT INTO mydb.users (name) VALUES ('test')").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Insert);
        assert_eq!(parsed.target_schema, Some("mydb".to_string()));
    }

    #[test]
    fn test_is_read_only() {
        assert!(StatementType::Select.is_read_only());
        assert!(StatementType::Show.is_read_only());
        assert!(StatementType::Explain.is_read_only());
        assert!(!StatementType::Insert.is_read_only());
        assert!(!StatementType::Create.is_read_only());
        assert!(!StatementType::Update.is_read_only());
        assert!(!StatementType::Delete.is_read_only());
    }

    #[test]
    fn test_is_write() {
        assert!(StatementType::Insert.is_write());
        assert!(StatementType::Update.is_write());
        assert!(StatementType::Delete.is_write());
        assert!(!StatementType::Select.is_write());
        assert!(!StatementType::Show.is_write());
        assert!(!StatementType::Create.is_write());
    }

    #[test]
    fn test_is_ddl_comprehensive() {
        assert!(StatementType::Create.is_ddl());
        assert!(StatementType::Alter.is_ddl());
        assert!(StatementType::Drop.is_ddl());
        assert!(StatementType::Truncate.is_ddl());
        assert!(!StatementType::Select.is_ddl());
        assert!(!StatementType::Insert.is_ddl());
        assert!(!StatementType::Update.is_ddl());
        assert!(!StatementType::Delete.is_ddl());
        assert!(!StatementType::Show.is_ddl());
    }

    #[test]
    fn test_use_database() {
        // USE is supported - should parse as Use or similar
        let result = parse_sql("USE mydb");
        if let Ok(parsed) = result {
            assert!(matches!(parsed.statement_type, StatementType::Use | StatementType::Other(_)));
        }
        // If err, that's also acceptable - just don't panic
    }

    #[test]
    fn test_show_databases() {
        let parsed = parse_sql("SHOW DATABASES").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Show);
        assert!(parsed.statement_type.is_read_only());
    }

    #[test]
    fn test_explain_select() {
        let parsed = parse_sql("EXPLAIN SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Explain);
        assert!(parsed.statement_type.is_read_only());
    }

    #[test]
    fn test_set_statement() {
        let parsed = parse_sql("SET @x = 1").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Set);
    }

    #[test]
    fn test_create_database() {
        let parsed = parse_sql("CREATE DATABASE mydb").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Create);
    }

    #[test]
    fn test_drop_qualified_schema() {
        let parsed = parse_sql("DROP TABLE mydb.users").unwrap();
        assert_eq!(parsed.statement_type, StatementType::Drop);
        assert_eq!(parsed.target_schema, Some("mydb".to_string()));
    }

    // Tests for parse_write_warnings

    #[test]
    fn test_write_warnings_update_no_where() {
        let parsed = parse_sql("UPDATE users SET name = 'x'").unwrap();
        let warnings = parse_write_warnings(&parsed);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("WHERE"), "Expected WHERE warning, got: {}", warnings[0]);
    }

    #[test]
    fn test_write_warnings_update_with_where() {
        let parsed = parse_sql("UPDATE users SET name = 'x' WHERE id = 1").unwrap();
        let warnings = parse_write_warnings(&parsed);
        assert!(warnings.is_empty(), "Expected no warnings for UPDATE with WHERE");
    }

    #[test]
    fn test_write_warnings_delete_no_where() {
        let parsed = parse_sql("DELETE FROM users").unwrap();
        let warnings = parse_write_warnings(&parsed);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("WHERE"), "Expected WHERE warning, got: {}", warnings[0]);
    }

    #[test]
    fn test_write_warnings_delete_with_where() {
        let parsed = parse_sql("DELETE FROM users WHERE id = 1").unwrap();
        let warnings = parse_write_warnings(&parsed);
        assert!(warnings.is_empty(), "Expected no warnings for DELETE with WHERE");
    }

    #[test]
    fn test_write_warnings_truncate() {
        let parsed = parse_sql("TRUNCATE TABLE users").unwrap();
        let warnings = parse_write_warnings(&parsed);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("TRUNCATE"), "Expected TRUNCATE warning, got: {}", warnings[0]);
    }

    #[test]
    fn test_write_warnings_insert_no_warnings() {
        let parsed = parse_sql("INSERT INTO users (name) VALUES ('Alice')").unwrap();
        let warnings = parse_write_warnings(&parsed);
        assert!(warnings.is_empty(), "Expected no warnings for INSERT");
    }

    #[test]
    fn test_write_warnings_select_no_warnings() {
        let parsed = parse_sql("SELECT * FROM users").unwrap();
        let warnings = parse_write_warnings(&parsed);
        assert!(warnings.is_empty(), "Expected no warnings for SELECT");
    }

    #[test]
    fn test_multi_statement_rejected() {
        // A multi-statement input must be rejected, regardless of what the
        // first statement is — otherwise "SELECT 1; DROP TABLE t" would be
        // misclassified as a read-only SELECT while MySQL would execute both.
        let err = parse_sql("SELECT 1; DROP TABLE t").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Multi-statement"),
            "Expected multi-statement error, got: {msg}"
        );

        // Two harmless SELECTs are also forbidden.
        let err2 = parse_sql("SELECT 1; SELECT 2").unwrap_err();
        assert!(err2.to_string().contains("Multi-statement"));
    }
}
