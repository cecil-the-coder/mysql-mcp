//! Re-export of the MySQL explain parser.
//!
//! This module re-exports `parse_mysql_explain` as `parse` for backward compatibility
//! with code that calls `super::explain_parse::parse(...)`.

#[cfg(feature = "mysql")]
pub use super::explain_mysql::parse_mysql_explain as parse;
