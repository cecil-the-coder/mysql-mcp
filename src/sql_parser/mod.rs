//! SQL parsing and statement classification.
//!
//! This module parses SQL statements using the `sqlparser-rs` crate and classifies
//! them by type (SELECT, INSERT, UPDATE, DELETE, DDL, etc.). It extracts metadata
//! such as target schemas and tables, and detects potentially dangerous patterns
//! like missing WHERE clauses or leading wildcard LIKE patterns.
//!
//! # Key Types
//!
//! - [`StatementType`] - Enum categorizing SQL statement types
//! - [`ParsedStatement`] - Parsed result with type, target schema, and safety warnings
//!
//! # Key Functions
//!
//! - [`parse_sql`] - Parses a SQL string and returns a [`ParsedStatement`]
//! - [`parse_write_warnings`] - Generates safety warnings for write operations
//!
//! # Security: Safety Checks
//!
//! The parser enforces strict security boundaries to prevent data exfiltration,
//! privilege escalation, and injection attacks. These checks are security-critical
//! and should be documented for security auditors.
//!
//! ## 1. SELECT INTO OUTFILE/DUMPFILE
//!
//! `SELECT ... INTO OUTFILE` and `SELECT ... INTO DUMPFILE` are blocked.
//!
//! **Why**: These MySQL extensions write query results to files on the server
//! filesystem. This could allow attackers to:
//! - Exfiltrate sensitive data to files they can download
//! - Overwrite server configuration files
//! - Write malicious files to web-accessible directories
//!
//! **Alternative**: Retrieve data with a standard `SELECT` and export client-side.
//!
//! ## 2. SET GLOBAL/PERSIST
//!
//! `SET GLOBAL`, `SET PERSIST`, `SET PERSIST_ONLY`, and `@@GLOBAL.*`/`@@PERSIST.*`
//! variable assignments are blocked.
//!
//! **Why**: These affect server-wide configuration and could:
//! - Disable security settings (e.g., `sql_safe_updates`, authentication plugins)
//! - Expose sensitive data via configuration changes
//! - Persist malicious settings across server restarts
//! - Allow privilege escalation by relaxing security controls
//!
//! **Allowed**: Session-level `SET SESSION` and plain `SET` (session-scoped) are
//! permitted as they only affect the current connection.
//!
//! ## 3. Multi-Statement SQL
//!
//! SQL strings containing multiple statements separated by semicolons are rejected.
//!
//! **Why**: This prevents SQL injection attacks where an attacker might append
//! malicious statements to a legitimate query:
//! - `SELECT * FROM users WHERE id = 1; DROP TABLE users; --`
//! - Even with prepared statements, multi-statement injection can occur in some
//!   MySQL client configurations
//!
//! **Alternative**: Send one statement per request. The connection stays open
//! for subsequent queries.
//!
//! # Example
//!
//! ```ignore
//! use sql_parser::parse_sql;
//! let parsed = parse_sql("SELECT * FROM users WHERE id = 1")?;
//! assert!(parsed.statement_type.is_read_only());
//! ```

use anyhow::{bail, Result};
use sqlparser::dialect::{Dialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
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

    /// Permission category label used in denial messages.
    /// Groups DDL types together; returns "this operation" for uncategorized statements.
    pub fn permission_category(&self) -> &str {
        match self {
            StatementType::Insert => "INSERT",
            StatementType::Update => "UPDATE",
            StatementType::Delete => "DELETE",
            StatementType::Create
            | StatementType::Alter
            | StatementType::Drop
            | StatementType::Truncate => "DDL",
            _ => "this operation",
        }
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
    /// The target schema/database extracted from the statement (if applicable).
    /// For single-table statements this is the only schema. For multi-table
    /// DELETE statements, this is the first schema (kept for backward compat);
    /// use `all_target_schemas` for exhaustive permission checks.
    pub target_schema: Option<String>,
    /// All distinct target schemas referenced by the statement. For most
    /// statements this mirrors `target_schema` (0 or 1 entries). Multi-table
    /// DELETEs populate this with schemas from every referenced table.
    pub all_target_schemas: Vec<String>,
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
    /// The SQL statement re-serialized from the AST via `format!("{stmt}")`. This
    /// strips SQL comments, ensuring that injected clauses (e.g. LIMIT) are not
    /// swallowed by trailing line comments (`-- ...` or `# ...`).
    pub serialized_sql: String,
}

/// Parse a SQL string and return the statement type and target schema.
/// `dialect` selects the sqlparser dialect (e.g. `"MySQL"`, `"PostgreSQL"`, `"SQLite"`).
/// Unrecognised values fall back to MySQL.
/// Returns an error if the SQL is invalid or cannot be parsed.
pub fn parse_sql(sql: &str, dialect: &str) -> Result<ParsedStatement> {
    let dialect: Box<dyn Dialect> = match dialect {
        "PostgreSQL" | "postgres" | "postgresql" => Box::new(PostgreSqlDialect {}),
        "SQLite" | "sqlite" => Box::new(SQLiteDialect {}),
        _ => Box::new(MySqlDialect {}),
    };
    let statements =
        Parser::parse_sql(&*dialect, sql).map_err(|e| anyhow::anyhow!("SQL parse error: {}", e))?;

    if statements.is_empty() {
        bail!("Empty SQL statement");
    }

    if statements.len() > 1 {
        bail!("Multi-statement SQL is not supported. Send one statement at a time.");
    }

    let stmt = &statements[0];
    let mut parsed = classify::classify_statement(stmt)?;

    // Re-serialize the AST to a canonical SQL string (strips comments). This is used
    // for LIMIT injection so that trailing line comments cannot swallow the LIMIT,
    // and for safety checks below.
    let serialized = format!("{stmt}");
    parsed.serialized_sql = serialized.clone();

    // Post-parse safety check: SELECT INTO OUTFILE/DUMPFILE writes to the server
    // filesystem and must be blocked. sqlparser parses these but doesn't expose the
    // INTO OUTFILE target in an accessible AST field, so we scan the re-serialized
    // statement text. Using the AST Display (not raw `sql`) strips SQL comments so
    // that a comment like `-- INTO OUTFILE '/x'` doesn't cause a false rejection.
    // We strip single-quoted string literals before scanning so that a value like
    // SELECT 'INTO OUTFILE' FROM t doesn't cause a false positive.
    //
    // FOR UPDATE/SHARE locking reads are detected in classify_statement() via the
    // query.locks AST field — no raw-string scan needed here.
    if parsed.statement_type == StatementType::Select {
        // Remove single-quoted literals (sqlparser re-serializes strings with single
        // quotes, using '' for escaped quotes inside). This regex replaces each
        // '...' span with an empty placeholder so literals can't trigger the check.
        let stripped = strip_single_quoted_literals(&serialized);
        let normalized = stripped.to_ascii_uppercase();
        if normalized.contains("INTO OUTFILE") || normalized.contains("INTO DUMPFILE") {
            bail!(
                "SELECT INTO OUTFILE/DUMPFILE is not supported — \
                 retrieve data with SELECT and export it client-side"
            );
        }
    }

    // Block SET GLOBAL / SET PERSIST — they affect server-wide settings and could
    // change security-sensitive config. Session-level SET is still allowed.
    if parsed.statement_type == StatementType::Set {
        let normalized = serialized.to_ascii_uppercase();
        if normalized.starts_with("SET GLOBAL")
            || normalized.starts_with("SET PERSIST")
            || normalized.contains("@@GLOBAL.")
            || normalized.contains("@@PERSIST.")
            || normalized.contains("@@PERSIST_ONLY.")
        {
            bail!(
                "SET GLOBAL and SET PERSIST are not allowed — they affect server-wide settings. \
                 Use SET SESSION or SET (without scope) to change session-level variables."
            );
        }
    }

    Ok(parsed)
}

/// Replace single-quoted string literals with empty strings so that literal values
/// like `'INTO OUTFILE'` are not mistaken for SQL keywords during safety checks.
/// Handles escaped quotes (`''`) inside literals correctly.
fn strip_single_quoted_literals(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut i = 0;
    let bytes = s.as_bytes();

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            // Skip past the entire single-quoted literal
            i += 1; // skip opening quote
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    // Check if this is '' (escaped quote) or a closing quote
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2; // skip the escaped pair ''
                    } else {
                        i += 1; // skip closing quote and exit the literal
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else {
            // Copy non-quote characters directly
            let start = i;
            while i < bytes.len() && bytes[i] != b'\'' {
                i += 1;
            }
            // Since input is valid UTF-8 and we slice at ASCII boundaries, this is safe.
            // Using from_utf8_lossy as a defensive measure against future refactoring.
            result.push_str(&String::from_utf8_lossy(&bytes[start..i]));
        }
    }

    result
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
