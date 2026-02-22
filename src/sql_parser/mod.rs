use anyhow::{bail, Result};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

mod classify;
#[cfg(test)]
mod tests;

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

    // Cached AST-derived fields populated once during parse_sql().
    // These allow check_limit_presence() and related helpers to reuse parse results
    // rather than re-invoking the sqlparser crate.

    /// True if the outermost SELECT (or Query) has a LIMIT clause.
    /// Only meaningful for Select statements; false otherwise.
    pub has_limit: bool,
    /// True if the outermost SELECT has a WHERE clause.
    /// Only meaningful for Select/Update/Delete statements.
    pub has_where: bool,
    /// True if the SELECT projection contains a wildcard (* or table.*).
    /// Only meaningful for Select statements.
    pub has_wildcard: bool,
    /// Column names referenced in the WHERE clause (deduplicated, order preserved).
    /// Only meaningful for Select statements; empty otherwise.
    pub where_columns: Vec<String>,
    /// True if the WHERE clause contains a LIKE pattern with a leading '%'.
    /// Detected from the AST during parse; only meaningful for Select statements.
    pub has_leading_wildcard_like: bool,
    /// Performance/safety warnings pre-computed during parse_sql().
    pub warnings: Vec<String>,
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
    classify::classify_statement(stmt)
}

/// Inspect a parsed write statement and return safety warnings.
/// Detects dangerous patterns: UPDATE/DELETE without WHERE, TRUNCATE.
///
/// Uses pre-parsed `has_where` from `ParsedStatement` to avoid re-invoking the SQL parser.
pub fn parse_write_warnings(parsed: &ParsedStatement) -> Vec<String> {
    let mut warnings = Vec::new();

    match &parsed.statement_type {
        StatementType::Truncate => {
            warnings.push(
                "TRUNCATE will delete ALL rows without transaction log — cannot be rolled back"
                    .to_string(),
            );
        }
        StatementType::Update => {
            if !parsed.has_where {
                warnings.push(
                    "UPDATE has no WHERE clause — this will affect ALL rows in the table"
                        .to_string(),
                );
            }
        }
        StatementType::Delete => {
            if !parsed.has_where {
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


