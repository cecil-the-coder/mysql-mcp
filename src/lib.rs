//! Library interface for embedding mysql-mcp.
//!
//! This module exposes the public API surface for programmatic use,
//! particularly for criterion benchmarks and integration testing.
//! The binary entry point lives in `src/main.rs`.
//!
//! # Modules
//!
//! - `config`: Configuration loading and validation (TOML, environment variables)
//! - `db`: Database connection pool management and connection lifecycle
//! - `permissions`: SQL permission checking and access control rules
//! - `query`: Query execution (read, write, EXPLAIN) with retry logic
//! - `schema`: Database schema introspection and metadata fetching
//! - `server`: MCP protocol server, request handlers, and session management
//! - `sql_parser`: SQL statement classification and parsing utilities
//! - `tunnel`: SSH tunnel support for secure database connections

pub mod config;
pub mod db;
pub mod permissions;
pub mod query;
pub mod schema;
pub mod server;
pub mod sql_parser;
pub mod tunnel;

#[cfg(test)]
pub mod test_helpers;
