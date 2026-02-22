//! Library surface exposed for criterion benchmarks and integration testing.
//! The binary entry point lives in src/main.rs.

pub mod config;
pub mod db;
pub mod permissions;
pub mod query;
pub mod sql_parser;

#[cfg(test)]
pub mod test_helpers;
