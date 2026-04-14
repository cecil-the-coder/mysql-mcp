//! SQL parsing, statement classification, and security integration.
//!
//! This module provides the foundation for secure SQL statement processing by parsing
//! and classifying incoming SQL queries, extracting metadata, and generating safety warnings.
//! It serves as the primary interface for the permission system (`permissions` module) to
//! evaluate whether a statement should be allowed.
//!
//! # Architecture
//!
//! The module consists of three core components:
//!
//! 1. **SQL Parser** (`parse_sql`): Uses `sqlparser-rs` to parse SQL strings into an AST,
//!    then classifies the statement and extracts key metadata (schema, table, wildcard usage,
//!    etc.).
//!
//! 2. **Statement Classifier** (`StatementType`): Categorizes statements into types such as
//!    SELECT, INSERT, UPDATE, DELETE, DDL, SET, etc. Each type has specific security implications.
//!
//! 3. **Safety Analyzer** (`ParsedStatement`, `parse_write_warnings`): Detects dangerous patterns
//!    such as missing WHERE clauses, leading wildcard LIKEs, and SELECT INTO OUTFILE attempts.
//!    It also pre-computes metadata used by permission checks.
//!
//! # Integration with Permission Checks
//!
//! The parsed output is designed for direct consumption by the [`permissions`] module:
//!
//! - [`StatementType`] variants determine the base permission category (read, write, DDL, etc.).
//! - [`ParsedStatement::all_target_schemas`] provides the authoritative list of affected schemas
//!   for multi-table statements (e.g., multi-table DELETEs), which [`permissions::check_all_permissions`]
//!   uses to enforce schema-specific overrides.
//! - Safety warnings from [`parse_write_warnings`] complement permission denials by alerting users
//!   to potentially destructive operations even when permissions allow them.
//!
//! See the documentation for [`permissions`] for details on the policy evaluation logic.
//!
//! # Security Considerations
//!
//! The parser performs critical security validations:
//!
//! - Blocks `SELECT INTO OUTFILE/DUMPFILE` to prevent server-side file writes.
//! - Blocks `SET GLOBAL/PERSIST` to prevent configuration tampering.
//! - Rejects multi-statement SQL to prevent stacked queries attacks.
//!
//! These checks are security-critical; bypassing or misconfiguring them may lead to
//! data exfiltration, privilege escalation, or server compromise.
//!
//! # Statement Types
//!
//! The [`StatementType`] enum categorizes all supported SQL statements:
//!
//! | Type      | Description                                  | Write? | DDL? |
//! |-----------|----------------------------------------------|--------|------|
//! | `Select`  | Standard SELECT query                        | No     | No   |
//! | `Insert`  | INSERT statement                             | Yes    | No   |
//! | `Update`  | UPDATE statement                             | Yes    | No   |
//! | `Delete`  | DELETE statement                             | Yes    | No   |
//! | `Create`  | CREATE TABLE/INDEX/etc.                      | No     | Yes  |
//! | `Alter`   | ALTER TABLE                                  | No     | Yes  |
//! | `Drop`    | DROP TABLE/DATABASE                          | No     | Yes  |
//! | `Truncate`| TRUNCATE TABLE                               | No     | Yes  |
//! | `Use`     | USE database (unsupported with pooling)      | No     | No   |
//! | `Show`    | SHOW commands                                | No     | No   |
//! | `Explain` | EXPLAIN, DESCRIBE, etc.                      | No     | No   |
//! | `Set`     | SET (session-only allowed)                   | No*    | No   |
//! | `Other`   | Unsupported or unclassified statements       | Varies | Varies |
//!
//! Write operations require explicit enablement via environment variables
//! (see [`permissions`]), while SET is permitted to maintain session state.
//!
//! # Parsing Safety Guarantees
//!
//! The parser ensures several invariants:
//!
//! - Only single statements are accepted; multi-statement SQL is rejected.
//! - The re-serialized SQL (via `format!(\"{stmt}\")`) strips comments, preventing
//!   comment-based injection attacks.
//! - Leading wildcard LIKE patterns (`'%value'`) are detected and flagged.
//! - `has_where` and `has_limit` flags enable efficient query analysis.
//!
//! # Example
//!
//! ```ignore
//! use sql_parser::{parse_sql, ParsedStatement};
//!
//! # fn example() -> anyhow::Result<()> {
//! let parsed: ParsedStatement = parse_sql("SELECT * FROM users WHERE id = 1")?;
//! assert_eq!(parsed.statement_type, StatementType::Select);
//! assert_eq!(parsed.target_schema, Some("public".to_string()));
//! assert!(!parsed.has_where); // Actually would be true, this is just example
//! # Ok(())
//! # }
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
