//! Database backend abstraction layer.
//!
//! This module defines the core traits and types that allow the MCP server
//! to work with multiple database backends (MySQL, PostgreSQL, SQLite).
//!
//! - `Backend` trait — async interface for all backend-specific operations
//! - `PoolOps` trait — async interface for pool-level query execution
//! - `PoolHandle` — cloneable wrapper around `Box<dyn PoolOps>`
//! - `BackendKind` — enum of supported backend types
//! - `RowData` / `ExecuteResult` — backend-agnostic data types

#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

use anyhow::Result;
use async_trait::async_trait;

use crate::config::{Config, SecurityConfig};
use crate::schema::{ColumnInfo, IndexDef, TableInfo};
use crate::tunnel::TunnelHandle;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Supported database backend types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    MySql,
    Postgres,
    Sqlite,
}

/// A single row returned from a database query, with named columns.
/// The backend is responsible for type-aware serialization into `serde_json::Value`.
#[derive(Debug, Clone)]
pub struct RowData {
    pub columns: Vec<(String, serde_json::Value)>,
}

/// Result of a write/DDL execution.
#[derive(Debug, Clone)]
pub struct ExecuteResult {
    pub rows_affected: u64,
    pub last_insert_id: Option<u64>,
}

/// Parameters for creating a session pool at runtime.
#[derive(Debug, Clone)]
pub struct SessionConnectParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: Option<String>,
    pub ssl: bool,
    pub ssl_accept_invalid_certs: bool,
    pub ssl_ca: Option<String>,
    pub connect_timeout_ms: u64,
}

// ---------------------------------------------------------------------------
// PoolOps — async trait for pool-level operations
// ---------------------------------------------------------------------------

/// Async trait for executing queries against a database pool.
/// Each backend (MySQL, PostgreSQL, SQLite) provides its own implementation.
#[async_trait]
pub trait PoolOps: Send + Sync {
    /// Execute a read query and return all matching rows.
    /// The backend is responsible for type-aware serialization.
    async fn fetch_all(&self, sql: &str) -> Result<Vec<RowData>>;

    /// Execute a write/DDL statement and return the result.
    async fn execute(&self, sql: &str) -> Result<ExecuteResult>;

    /// Execute a statement within a transaction.
    async fn execute_in_transaction(&self, sql: &str) -> Result<ExecuteResult>;

    /// Close all connections in the pool.
    async fn close(&self);

    /// Clone this pool handle as a boxed trait object.
    fn clone_box(&self) -> Box<dyn PoolOps>;
}

/// A cloneable handle to a database pool.
/// Wraps `Box<dyn PoolOps>` and implements `Clone` via `clone_box()`.
pub struct PoolHandle {
    inner: Box<dyn PoolOps>,
}

impl Clone for PoolHandle {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone_box(),
        }
    }
}

impl PoolHandle {
    pub fn new(inner: Box<dyn PoolOps>) -> Self {
        Self { inner }
    }
}

impl std::ops::Deref for PoolHandle {
    type Target = dyn PoolOps;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

// ---------------------------------------------------------------------------
// Backend trait — async trait for all backend-specific operations
// ---------------------------------------------------------------------------

/// Async trait that defines all backend-specific operations.
/// Each database backend (MySQL, PostgreSQL, SQLite) implements this trait.
#[async_trait]
pub trait Backend: Send + Sync {
    /// The kind of backend (MySQL, Postgres, Sqlite).
    fn kind(&self) -> BackendKind;

    /// Human-readable backend name for display in server info.
    fn backend_name(&self) -> &str;

    /// The default port for this backend.
    fn default_port(&self) -> u16;

    /// The SQL dialect name (used by sql_parser).
    fn sql_dialect(&self) -> &str;

    /// Quote an identifier for this backend's SQL dialect.
    fn quote_identifier(&self, name: &str) -> String;

    /// Whether this backend supports SSL/TLS connections.
    fn supports_ssl(&self) -> bool;

    /// Whether this backend supports SSH tunnels.
    fn supports_ssh_tunnel(&self) -> bool;

    /// Whether this backend supports network connections.
    fn supports_network(&self) -> bool;

    /// Create a connection pool from the application config.
    async fn create_pool(&self, config: &Config, security: &SecurityConfig) -> Result<PoolHandle>;

    /// Create a small pool for a named session.
    async fn create_session_pool(&self, params: &SessionConnectParams) -> Result<PoolHandle>;

    /// Create a small pool through an SSH tunnel.
    async fn create_tunnel_pool(
        &self,
        tunnel: &TunnelHandle,
        params: &SessionConnectParams,
    ) -> Result<(PoolHandle, TunnelHandle)>;

    /// Fetch table metadata from information_schema.
    async fn fetch_tables(
        &self,
        pool: &PoolHandle,
        database: Option<&str>,
    ) -> Result<Vec<TableInfo>>;

    /// Fetch column metadata for a table.
    async fn fetch_columns(
        &self,
        pool: &PoolHandle,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<ColumnInfo>>;

    /// Fetch composite index definitions for a table.
    async fn fetch_composite_indexes(
        &self,
        pool: &PoolHandle,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<IndexDef>>;

    /// Fetch the list of column names that have at least one index on a table.
    async fn fetch_indexed_columns(
        &self,
        pool: &PoolHandle,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<String>>;

    /// Fetch detailed schema info for a single table (indexes, FKs, size).
    async fn fetch_schema_details(
        &self,
        pool: &PoolHandle,
        table: &str,
        database: Option<&str>,
        indexes: bool,
        fks: bool,
        size: bool,
    ) -> Result<serde_json::Value>;

    /// Fetch server version and metadata.
    async fn fetch_server_info(
        &self,
        pool: &PoolHandle,
        security: &SecurityConfig,
    ) -> Result<serde_json::Value>;

    /// Run EXPLAIN on a SELECT query and return the parsed result.
    async fn run_explain(
        &self,
        pool: &PoolHandle,
        sql: &str,
        query_timeout_ms: u64,
    ) -> Result<crate::query::explain::ExplainResult>;

    /// Run a raw SQL query and return rows for the list_tables handler.
    async fn fetch_list_tables(&self, pool: &PoolHandle, database: &str) -> Result<Vec<String>>;
}

// ---------------------------------------------------------------------------
// Backend factory
// ---------------------------------------------------------------------------

/// Create a backend instance for the given kind.
///
/// Each variant is gated behind the corresponding feature flag so that
/// backends that are not compiled in produce a clear error at startup.
pub fn create_backend(kind: &BackendKind) -> std::sync::Arc<dyn Backend> {
    match kind {
        #[cfg(feature = "mysql")]
        BackendKind::MySql => std::sync::Arc::new(mysql::MySqlBackend::new()),
        #[cfg(not(feature = "mysql"))]
        BackendKind::MySql => {
            unreachable!("MySQL backend requested but 'mysql' feature is not enabled; check DB_BACKEND env var or config file")
        }
        #[cfg(feature = "postgres")]
        BackendKind::Postgres => std::sync::Arc::new(postgres::PgBackend::new()),
        #[cfg(not(feature = "postgres"))]
        BackendKind::Postgres => {
            unreachable!("PostgreSQL backend requested but 'postgres' feature is not enabled; check DB_BACKEND env var or config file")
        }
        #[cfg(feature = "sqlite")]
        BackendKind::Sqlite => std::sync::Arc::new(sqlite::SqliteBackend::new()),
        #[cfg(not(feature = "sqlite"))]
        BackendKind::Sqlite => {
            unreachable!("SQLite backend requested but 'sqlite' feature is not enabled; check DB_BACKEND env var or config file")
        }
    }
}
