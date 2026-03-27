//! # mysql-mcp
//!
//! A MySQL MCP (Model Context Protocol) server that exposes MySQL databases to LLM assistants.
//!
//! This crate provides the core library functionality for connecting to MySQL databases,
//! executing queries with permission checks, and exposing database operations via the MCP
//! protocol. The binary entry point is in `src/main.rs`.
//!
//! ## Module Structure
//!
//! The library is organized into the following public modules:
//!
//! - **[`config`]** — Configuration loading from environment variables and TOML files.
//!   Defines types like [`Config`](config::Config), [`ConnectionConfig`](config::ConnectionConfig),
//!   [`PoolConfig`](config::PoolConfig), [`SecurityConfig`](config::SecurityConfig), and
//!   [`SshConfig`](config::SshConfig) for controlling connection, pooling, and security settings.
//!
//! - **[`db`]** — Database connection pool creation and management.
//!   Provides [`build_pool`](db::build_pool) for the default connection and
//!   [`build_session_pool`](db::build_session_pool) for named sessions with custom credentials.
//!
//! - **[`permissions`]** — SQL statement permission enforcement.
//!   [`check_permission`](permissions::check_permission) validates statements against the
//!   configured security policy (read-only by default; INSERT/UPDATE/DELETE/DDL require opt-in).
//!
//! - **[`query`]** — SQL query execution and result processing.
//!   Handles SELECT queries with automatic EXPLAIN analysis, write operations with safety
//!   warnings, and retry logic for transient failures.
//!
//! - **[`schema`]** — Schema introspection and metadata caching.
//!   [`SchemaIntrospector`](schema::SchemaIntrospector) provides cached access to table columns,
//!   indexes, and foreign keys via `information_schema` queries.
//!
//! - **[`server`]** — MCP protocol server implementation.
//!   [`McpServer`](server::McpServer) implements the MCP protocol handlers for tools like
//!   `mysql_query`, `mysql_schema_info`, `mysql_connect`, and session management.
//!
//! - **[`sql_parser`]** — SQL parsing and statement classification.
//!   [`parse_sql`](sql_parser::parse_sql) returns a [`ParsedStatement`](sql_parser::ParsedStatement)
//!   with the [`StatementType`](sql_parser::StatementType) (SELECT, INSERT, etc.) and extracted
//!   metadata like target schema and LIMIT presence.
//!
//! - **[`tunnel`]** — SSH tunnel management for reaching databases behind bastion hosts.
//!   [`spawn_ssh_tunnel`](tunnel::spawn_ssh_tunnel) creates a tunnel and returns a
//!   [`TunnelHandle`](tunnel::TunnelHandle) that keeps the connection alive.
//!
//! ## Entry Points
//!
//! - **Integration tests and benchmarks**: Import this crate directly and use the public
//!   modules to build pools, parse SQL, or check permissions.
//!
//! - **Production server**: The binary in `src/main.rs` loads configuration, builds a
//!   connection pool (optionally through an SSH tunnel), and runs [`McpServer`](server::McpServer)
//!   over stdio transport.

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
