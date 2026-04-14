//! SQLite-specific backend implementation.
//!
//! Implements `Backend` and `PoolOps` traits for SQLite databases.
//! All SQLite-specific code (pool creation, type serialization,
//! PRAGMA-based schema queries, EXPLAIN execution) lives here.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::{Column, Row, TypeInfo};
use std::str::FromStr;
use std::time::Duration;

use super::{
    Backend, BackendKind, ExecuteResult, PoolHandle, PoolOps, RowData, SessionConnectParams,
};
use crate::config::{Config, SecurityConfig};
use crate::query::explain::ExplainResult;
use crate::query::explain_sqlite::parse_sqlite_explain_from_rows;
use crate::query::with_timeout;
use crate::schema::{ColumnInfo, IndexDef, TableInfo};
use crate::tunnel::TunnelHandle;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Idle connection lifetime before it is closed and removed from the pool.
const POOL_IDLE_TIMEOUT_SECS: u64 = 300;

/// Maximum lifetime of any pooled connection before it is recycled.
const POOL_MAX_LIFETIME_SECS: u64 = 1800;

/// Binary columns with invalid UTF-8 are hex-encoded. Cap the output at 512 KB.
const MAX_BINARY_DISPLAY_BYTES: usize = 512 * 1024;

// ---------------------------------------------------------------------------
// SqlitePoolWrapper — wraps SqlitePool, implements PoolOps
// ---------------------------------------------------------------------------

/// Internal wrapper that holds a `SqlitePool` and implements `PoolOps`.
/// This is the concrete type behind `PoolHandle` for the SQLite backend.
#[derive(Clone)]
pub(crate) struct SqlitePoolWrapper {
    pool: sqlx::SqlitePool,
}

impl SqlitePoolWrapper {
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PoolOps for SqlitePoolWrapper {
    async fn fetch_all(&self, sql: &str) -> Result<Vec<RowData>> {
        let rows = sqlx::query(sql).fetch_all(&self.pool).await?;
        let mut warnings = Vec::new();
        let result: Vec<RowData> = rows
            .iter()
            .map(|row| sqlite_row_to_row_data(row, &mut warnings))
            .collect();
        for w in warnings {
            tracing::debug!("{}", w);
        }
        Ok(result)
    }

    async fn execute(&self, sql: &str) -> Result<ExecuteResult> {
        let trimmed = sql.trim().to_uppercase();
        let is_insert = trimmed.starts_with("INSERT");

        let result = sqlx::query(sql).execute(&self.pool).await?;
        let rows_affected = result.rows_affected();

        let last_insert_id = if is_insert {
            // SQLite doesn't return last_insert_id on query results — must query separately
            match sqlx::query_scalar::<_, i64>("SELECT last_insert_rowid()")
                .fetch_one(&self.pool)
                .await
            {
                Ok(id) if id > 0 => Some(id as u64),
                _ => None,
            }
        } else {
            None
        };

        Ok(ExecuteResult {
            rows_affected,
            last_insert_id,
        })
    }

    async fn execute_in_transaction(&self, sql: &str) -> Result<ExecuteResult> {
        let mut tx = self.pool.begin().await?;
        let trimmed = sql.trim().to_uppercase();
        let is_insert = trimmed.starts_with("INSERT");

        let result = sqlx::query(sql).execute(&mut *tx).await?;
        let rows_affected = result.rows_affected();

        let last_insert_id = if is_insert {
            match sqlx::query_scalar::<_, i64>("SELECT last_insert_rowid()")
                .fetch_one(&mut *tx)
                .await
            {
                Ok(id) if id > 0 => Some(id as u64),
                _ => None,
            }
        } else {
            None
        };

        tx.commit().await?;

        Ok(ExecuteResult {
            rows_affected,
            last_insert_id,
        })
    }

    async fn close(&self) {
        self.pool.close().await;
    }

    fn clone_box(&self) -> Box<dyn PoolOps> {
        Box::new(self.clone())
    }
}

// ---------------------------------------------------------------------------
// SQLite type-aware row serialization
// ---------------------------------------------------------------------------

/// Convert a SQLite row to `RowData`, performing type-aware serialization.
fn sqlite_row_to_row_data(row: &sqlx::sqlite::SqliteRow, warnings: &mut Vec<String>) -> RowData {
    let mut columns = Vec::with_capacity(row.columns().len());
    for (i, col) in row.columns().iter().enumerate() {
        let value = sqlite_column_to_json(row, i, col, warnings);
        columns.push((col.name().to_string(), value));
    }
    RowData { columns }
}

/// Convert a single SQLite column value to JSON based on its type name.
///
/// SQLite has a dynamic type system. sqlx reports the declared type affinity:
/// INTEGER, REAL, TEXT, BLOB, NUMERIC, BOOLEAN, DATE, DATETIME, TIME, NULL.
/// However, the actual stored value may differ from the declared affinity.
fn sqlite_column_to_json(
    row: &sqlx::sqlite::SqliteRow,
    idx: usize,
    col: &sqlx::sqlite::SqliteColumn,
    warnings: &mut Vec<String>,
) -> Value {
    let type_name = col.type_info().name();

    match type_name {
        // Boolean — SQLite stores booleans as 0/1 internally
        "BOOLEAN" | "BOOL" => {
            if let Ok(v) = row.try_get::<Option<bool>, _>(idx) {
                return v.map(Value::Bool).unwrap_or(Value::Null);
            }
            // Fallback: read as integer and convert
            if let Ok(v) = row.try_get::<Option<i64>, _>(idx) {
                return v
                    .map(|n| Value::Bool(n != 0))
                    .unwrap_or(Value::Null);
            }
        }

        // Integer types
        "INTEGER" | "INT" | "INT2" | "INT8" | "BIGINT" | "SMALLINT" | "TINYINT" | "MEDIUMINT"
        | "YEAR" => {
            if let Ok(v) = row.try_get::<Option<i64>, _>(idx) {
                return v
                    .map(|n| {
                        // Preserve precision for large integers
                        if !(-9_007_199_254_740_992i64..=9_007_199_254_740_992i64).contains(&n) {
                            Value::String(n.to_string())
                        } else {
                            Value::Number(n.into())
                        }
                    })
                    .unwrap_or(Value::Null);
            }
        }

        // Real / floating point
        "REAL" | "DOUBLE" | "FLOAT" | "FLOAT8" => {
            if let Ok(v) = row.try_get::<Option<f64>, _>(idx) {
                return v
                    .map(|n| {
                        serde_json::Number::from_f64(n)
                            .map(Value::Number)
                            .unwrap_or_else(|| {
                                warnings.push(format!(
                                    "Column '{}' contains NaN/Infinity value converted to NULL",
                                    col.name()
                                ));
                                Value::Null
                            })
                    })
                    .unwrap_or(Value::Null);
            }
        }

        // Text types
        "TEXT" | "VARCHAR" | "CHAR" | "CLOB" | "NCHAR" | "NVARCHAR" | "VARYING CHARACTER"
        | "NATIVE CHARACTER" => {
            if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        // Binary / BLOB
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" | "IMAGE" => {
            if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
                return match v {
                    None => Value::Null,
                    Some(b) => {
                        // Try to interpret as valid UTF-8 first
                        if let Ok(s) = String::from_utf8(b.clone()) {
                            return Value::String(s);
                        }
                        // Otherwise hex-encode
                        let total = b.len();
                        let display = &b[..total.min(MAX_BINARY_DISPLAY_BYTES)];
                        let mut hex = String::with_capacity(display.len() * 2);
                        for byte in display {
                            use std::fmt::Write as _;
                            let _ = write!(hex, "{:02x}", byte);
                        }
                        if total > MAX_BINARY_DISPLAY_BYTES {
                            warnings.push(format!(
                                "Binary column '{}' truncated: {} bytes total, displayed first {} bytes as hex",
                                col.name(),
                                total,
                                MAX_BINARY_DISPLAY_BYTES
                            ));
                        }
                        Value::String(hex)
                    }
                };
            }
        }

        // NUMERIC — SQLite NUMERIC affinity can store integers or reals
        "NUMERIC" | "DECIMAL" => {
            // Try integer first (more common for NUMERIC columns)
            if let Ok(v) = row.try_get::<Option<i64>, _>(idx) {
                return v
                    .map(|n| {
                        if !(-9_007_199_254_740_992i64..=9_007_199_254_740_992i64).contains(&n) {
                            Value::String(n.to_string())
                        } else {
                            Value::Number(n.into())
                        }
                    })
                    .unwrap_or(Value::Null);
            }
            // Then try float
            if let Ok(v) = row.try_get::<Option<f64>, _>(idx) {
                return v
                    .map(|n| {
                        serde_json::Number::from_f64(n)
                            .map(Value::Number)
                            .unwrap_or_else(|| Value::String(n.to_string()))
                    })
                    .unwrap_or(Value::Null);
            }
            // Then try string (for numeric strings)
            if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        // Date/Time types — stored as text in SQLite
        "DATE" => {
            if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        "DATETIME" | "TIMESTAMP" => {
            if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        "TIME" => {
            if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        "NULL" => {
            // A declared type of NULL means "no declared type" (e.g. EXPLAIN QUERY PLAN
            // output columns, or untyped literal expressions like SELECT 1 AS one).
            // Fall through to the generic fallback to try decoding the actual stored value.
        }

        _ => {
            // Unknown declared type — fall through to fallback
        }
    }

    // Fallback: try string first (most flexible for SQLite's dynamic typing)
    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return v.map(Value::String).unwrap_or(Value::Null);
    }

    // Fallback: try integer
    if let Ok(v) = row.try_get::<Option<i64>, _>(idx) {
        return v
            .map(|n| {
                if !(-9_007_199_254_740_992i64..=9_007_199_254_740_992i64).contains(&n) {
                    Value::String(n.to_string())
                } else {
                    Value::Number(n.into())
                }
            })
            .unwrap_or(Value::Null);
    }

    // Fallback: try float
    if let Ok(v) = row.try_get::<Option<f64>, _>(idx) {
        return v
            .map(|n| {
                serde_json::Number::from_f64(n)
                    .map(Value::Number)
                    .unwrap_or_else(|| Value::String(n.to_string()))
            })
            .unwrap_or(Value::Null);
    }

    // Fallback: try bytes as hex
    if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
        return match v {
            None => Value::Null,
            Some(b) => String::from_utf8(b).map(Value::String).unwrap_or_else(|e| {
                let bytes = e.into_bytes();
                let total = bytes.len();
                let display = &bytes[..total.min(MAX_BINARY_DISPLAY_BYTES)];
                let mut hex = String::with_capacity(display.len() * 2);
                for byte in display {
                    use std::fmt::Write as _;
                    let _ = write!(hex, "{:02x}", byte);
                }
                if total > MAX_BINARY_DISPLAY_BYTES {
                    warnings.push(format!(
                        "Binary column '{}' (type '{}') truncated: {} bytes total, displayed first {} bytes as hex",
                        col.name(),
                        type_name,
                        total,
                        MAX_BINARY_DISPLAY_BYTES
                    ));
                }
                Value::String(hex)
            }),
        };
    }

    warnings.push(format!(
        "Column '{}' (type '{}') could not be decoded, returning NULL",
        col.name(),
        type_name
    ));
    Value::Null
}

// ---------------------------------------------------------------------------
// Helper: escape a SQLite identifier for double-quote quoting
// ---------------------------------------------------------------------------

/// Escape a SQLite identifier for use in double-quote contexts.
/// Double-quotes are escaped by doubling them (`"id"` stays `"id"`, `"a""b"` for `a"b`).
pub(crate) fn escape_sqlite_identifier(name: &str) -> String {
    name.replace('"', "\"\"")
}

// ---------------------------------------------------------------------------
// Pool creation
// ---------------------------------------------------------------------------

/// Build a SqlitePool from the application config.
pub(crate) async fn build_pool(config: &Config) -> Result<sqlx::SqlitePool> {
    let conn = &config.connection;

    let db_path = resolve_sqlite_path(conn)?;

    let mut opts = sqlx::sqlite::SqliteConnectOptions::from_str(&format!(
        "sqlite:{}",
        db_path
    ))?;

    // Enable WAL mode for better concurrent read performance
    opts = opts.pragma("journal_mode", "WAL");
    // Set busy timeout to prevent immediate errors on lock contention
    opts = opts.pragma("busy_timeout", "5000");
    // Enable foreign keys
    opts = opts.pragma("foreign_keys", "ON");

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(config.pool.size)
        .acquire_timeout(Duration::from_millis(config.pool.connect_timeout_ms))
        .idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
        .max_lifetime(Duration::from_secs(POOL_MAX_LIFETIME_SECS))
        .connect_with(opts)
        .await?;

    Ok(pool)
}

/// Resolve the SQLite database path from the connection config.
///
/// Priority:
/// 1. `config.connection.path` (explicit file path)
/// 2. `config.connection.connection_string` if it starts with `sqlite://` or `sqlite:`
/// 3. In-memory database (`:memory:`) as a last resort
fn resolve_sqlite_path(conn: &crate::config::ConnectionConfig) -> Result<String> {
    if let Some(ref path) = conn.path {
        return Ok(path.clone());
    }

    if let Some(ref cs) = conn.connection_string {
        if cs.starts_with("sqlite://") {
            // sqlite:///absolute/path or sqlite://relative/path
            // "sqlite://" is 9 chars; the rest is the file path
            return Ok(cs[9..].to_string());
        }
        if cs.starts_with("sqlite:") {
            return Ok(cs[7..].to_string());
        }
        anyhow::bail!(
            "connection.connection_string must start with 'sqlite://' or 'sqlite:', got: '{}'",
            cs
        );
    }

    // Default to in-memory database
    Ok(":memory:".to_string())
}

// ---------------------------------------------------------------------------
// RowData helper functions — extract values from RowData (backend-agnostic)
// ---------------------------------------------------------------------------

fn get_str(row: &RowData, col: &str) -> String {
    row.columns
        .iter()
        .find(|(name, _)| name == col)
        .map(|(_, v)| {
            if v.is_string() {
                v.as_str().unwrap_or_default().to_string()
            } else if v.is_null() {
                String::new()
            } else {
                v.to_string()
            }
        })
        .unwrap_or_default()
}

fn get_opt_str(row: &RowData, col: &str) -> Option<String> {
    row.columns
        .iter()
        .find(|(name, _)| name == col)
        .and_then(|(_, v)| v.as_str().map(String::from))
        .filter(|s| !s.is_empty())
}

fn get_opt_i64(row: &RowData, col: &str) -> Option<i64> {
    row.columns
        .iter()
        .find(|(name, _)| name == col)
        .and_then(|(_, v)| v.as_i64())
}

// ---------------------------------------------------------------------------
// SqliteBackend — implements Backend trait
// ---------------------------------------------------------------------------

/// SQLite backend implementation.
pub struct SqliteBackend;

impl SqliteBackend {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SqliteBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Backend for SqliteBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::Sqlite
    }

    fn backend_name(&self) -> &str {
        "SQLite"
    }

    fn default_port(&self) -> u16 {
        0 // No port for SQLite
    }

    fn sql_dialect(&self) -> &str {
        "sqlite"
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", escape_sqlite_identifier(name))
    }

    fn supports_ssl(&self) -> bool {
        false
    }

    fn supports_ssh_tunnel(&self) -> bool {
        false
    }

    fn supports_network(&self) -> bool {
        false
    }

    async fn create_pool(&self, config: &Config, _security: &SecurityConfig) -> Result<PoolHandle> {
        let pool = build_pool(config).await?;
        Ok(PoolHandle::new(Box::new(SqlitePoolWrapper::new(pool))))
    }

    async fn create_session_pool(&self, _params: &SessionConnectParams) -> Result<PoolHandle> {
        anyhow::bail!("SQLite does not support runtime session connections (no network)")
    }

    async fn create_tunnel_pool(
        &self,
        _tunnel: &TunnelHandle,
        _params: &SessionConnectParams,
    ) -> Result<(PoolHandle, TunnelHandle)> {
        anyhow::bail!("SQLite does not support SSH tunnels")
    }

    async fn fetch_tables(
        &self,
        pool: &PoolHandle,
        _database: Option<&str>,
    ) -> Result<Vec<TableInfo>> {
        // SQLite doesn't have a concept of "database" — use sqlite_master
        let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name";
        let rows = pool.fetch_all(sql).await?;

        let mut tables = Vec::with_capacity(rows.len());
        for row in &rows {
            let table_name = get_str(row, "name");
            if table_name.is_empty() {
                continue;
            }

            // Get row count and size via additional queries
            let row_count = get_table_row_count(pool, &table_name).await.ok();
            let data_size_bytes = get_table_size(pool, &table_name).await.ok();

            tables.push(TableInfo {
                name: table_name,
                schema: String::new(), // SQLite has no schema concept
                row_count,
                data_size_bytes,
                create_time: None,  // SQLite doesn't track creation time
                update_time: None,  // SQLite doesn't track modification time
            });
        }

        Ok(tables)
    }

    async fn fetch_columns(
        &self,
        pool: &PoolHandle,
        table_name: &str,
        _database: Option<&str>,
    ) -> Result<Vec<ColumnInfo>> {
        // PRAGMA table_info doesn't support bound parameters — interpolate table name
        let escaped = escape_sqlite_identifier(table_name);
        let sql = format!("PRAGMA table_info(\"{}\")", escaped);
        let rows = pool.fetch_all(&sql).await?;

        let mut columns = Vec::with_capacity(rows.len());
        for row in &rows {
            let name = get_str(row, "name");
            if name.is_empty() {
                continue;
            }
            let data_type = get_str(row, "type");
            let notnull_str = get_str(row, "notnull");
            let dflt_value = get_opt_str(row, "dflt_value");
            let pk_str = get_str(row, "pk");

            // Determine column key
            let pk_num: i64 = pk_str.parse().unwrap_or(0);
            let column_key = if pk_num > 0 {
                Some("PRI".to_string())
            } else {
                // Check if column is part of a unique index (not PK)
                check_unique_column(pool, table_name, &name).await
            };

            columns.push(ColumnInfo {
                name,
                data_type: data_type.to_lowercase(),
                column_type: data_type,
                is_nullable: notnull_str != "1",
                column_default: dflt_value,
                column_key,
                extra: None, // SQLite has no auto_increment in the MySQL sense
            });
        }

        Ok(columns)
    }

    async fn fetch_composite_indexes(
        &self,
        pool: &PoolHandle,
        table_name: &str,
        _database: Option<&str>,
    ) -> Result<Vec<IndexDef>> {
        let escaped = escape_sqlite_identifier(table_name);

        // Get list of indexes
        let list_sql = format!("PRAGMA index_list(\"{}\")", escaped);
        let index_rows = pool.fetch_all(&list_sql).await?;

        let mut indexes = Vec::new();
        for irow in &index_rows {
            let idx_name = get_str(irow, "name");
            if idx_name.is_empty() || idx_name.starts_with("sqlite_autoindex_") {
                // Skip auto-indexes (automatically created by SQLite for UNIQUE/PRIMARY KEY constraints)
                continue;
            }
            let unique_str = get_str(irow, "unique");
            let is_unique = unique_str == "1";

            // Get columns in this index
            let info_sql = format!("PRAGMA index_info(\"{}\")", escape_sqlite_identifier(&idx_name));
            let info_rows = pool.fetch_all(&info_sql).await?;

            let cols: Vec<String> = info_rows
                .iter()
                .map(|r| get_str(r, "name"))
                .filter(|n| !n.is_empty())
                .collect();

            if !cols.is_empty() {
                indexes.push(IndexDef {
                    name: idx_name,
                    unique: is_unique,
                    columns: cols,
                });
            }
        }

        Ok(indexes)
    }

    async fn fetch_indexed_columns(
        &self,
        pool: &PoolHandle,
        table_name: &str,
        _database: Option<&str>,
    ) -> Result<Vec<String>> {
        let indexes = self.fetch_composite_indexes(pool, table_name, None).await?;

        let mut cols: Vec<String> = indexes
            .iter()
            .flat_map(|idx| idx.columns.clone())
            .collect();

        // Deduplicate while preserving order
        let mut seen = std::collections::HashSet::new();
        cols.retain(|c| seen.insert(c.clone()));

        Ok(cols)
    }

    async fn fetch_schema_details(
        &self,
        pool: &PoolHandle,
        table_name: &str,
        _database: Option<&str>,
        include_indexes: bool,
        include_fks: bool,
        include_size: bool,
    ) -> Result<serde_json::Value> {
        let columns = self.fetch_columns(pool, table_name, None).await?;
        let escaped = escape_sqlite_identifier(table_name);

        // Indexes
        let indexes_val: serde_json::Value = if include_indexes {
            let list_sql = format!("PRAGMA index_list(\"{}\")", escaped);
            let index_rows = pool.fetch_all(&list_sql).await?;

            let mut idx_arr = Vec::new();
            for irow in &index_rows {
                let idx_name = get_str(irow, "name");
                if idx_name.starts_with("sqlite_autoindex_") {
                    continue;
                }
                let unique_str = get_str(irow, "unique");
                let origin = get_str(irow, "origin");

                let info_sql = format!(
                    "PRAGMA index_info(\"{}\")",
                    escape_sqlite_identifier(&idx_name)
                );
                let info_rows = pool.fetch_all(&info_sql).await?;

                let col_details: Vec<serde_json::Value> = info_rows
                    .iter()
                    .map(|r| {
                        serde_json::json!({
                            "column": get_str(r, "name"),
                        })
                    })
                    .collect();

                idx_arr.push(serde_json::json!({
                    "name": idx_name,
                    "unique": unique_str == "1",
                    "origin": origin,
                    "columns": col_details,
                }));
            }
            serde_json::Value::Array(idx_arr)
        } else {
            serde_json::Value::Null
        };

        // Foreign keys
        let foreign_keys: serde_json::Value = if include_fks {
            let fk_sql = format!("PRAGMA foreign_key_list(\"{}\")", escaped);
            let fk_rows = pool.fetch_all(&fk_sql).await?;

            serde_json::Value::Array(
                fk_rows
                    .iter()
                    .map(|row| {
                        serde_json::json!({
                            "id":         get_str(row, "id"),
                            "seq":        get_str(row, "seq"),
                            "table":      get_str(row, "table"),
                            "from":       get_str(row, "from"),
                            "to":         get_str(row, "to"),
                            "on_update":  get_str(row, "on_update"),
                            "on_delete":  get_str(row, "on_delete"),
                            "match":      get_str(row, "match"),
                        })
                    })
                    .collect(),
            )
        } else {
            serde_json::Value::Null
        };

        // Table size
        let size: serde_json::Value = if include_size {
            let row_count = get_table_row_count(pool, table_name).await.unwrap_or(0);
            let data_bytes = get_table_size(pool, table_name).await.unwrap_or(0);
            serde_json::json!({
                "estimated_rows": row_count,
                "data_bytes":     data_bytes,
            })
        } else {
            serde_json::Value::Null
        };

        // Assemble result
        let cols_json: Vec<serde_json::Value> = columns
            .iter()
            .map(|c| {
                serde_json::json!({
                    "name": c.name, "type": c.column_type, "nullable": c.is_nullable,
                    "default": c.column_default, "key": c.column_key, "extra": c.extra,
                })
            })
            .collect();

        let mut result = serde_json::json!({ "table": table_name, "columns": cols_json });
        if !indexes_val.is_null() {
            result["indexes"] = indexes_val;
        }
        if !foreign_keys.is_null() {
            result["foreign_keys"] = foreign_keys;
        }
        if !size.is_null() {
            result["size"] = size;
        }
        Ok(result)
    }

    async fn fetch_server_info(
        &self,
        pool: &PoolHandle,
        security: &SecurityConfig,
    ) -> Result<serde_json::Value> {
        // SQLite version
        let version_rows =
            pool.fetch_all("SELECT sqlite_version() AS sqlite_version").await?;
        let sqlite_version = version_rows
            .first()
            .map(|r| get_str(r, "sqlite_version"))
            .unwrap_or_default();

        // Compile options (key ones)
        let compile_rows = pool
            .fetch_all(
                "SELECT compile_option AS opt FROM pragma_compile_options() LIMIT 20",
            )
            .await?;
        let compile_options: Vec<String> = compile_rows
            .iter()
            .map(|r| get_str(r, "opt"))
            .filter(|s| !s.is_empty())
            .collect();

        // Database file path
        let db_path_rows = pool
            .fetch_all("SELECT * FROM pragma_database_list()")
            .await?;
        let db_path = db_path_rows
            .first()
            .and_then(|r| get_opt_str(r, "file"))
            .unwrap_or_else(|| ":memory:".to_string());

        // Journal mode
        let journal_rows =
            pool.fetch_all("PRAGMA journal_mode").await?;
        let journal_mode = journal_rows
            .first()
            .map(|r| get_str(r, "journal_mode"))
            .unwrap_or_default();

        let mut accessible_features = vec!["SELECT", "SHOW", "EXPLAIN"];
        for (enabled, name) in [
            (security.allow_insert, "INSERT"),
            (security.allow_update, "UPDATE"),
            (security.allow_delete, "DELETE"),
            (security.allow_ddl, "DDL (CREATE/ALTER/DROP)"),
        ] {
            if enabled {
                accessible_features.push(name);
            }
        }

        let info = json!({
            "sqlite_version": sqlite_version,
            "database_path": db_path,
            "journal_mode": journal_mode,
            "compile_options": compile_options,
            "accessible_features": accessible_features,
        });

        let mut response = info;
        let warnings = security.security_warnings();
        if !warnings.is_empty() {
            response["security_warnings"] = json!(warnings);
        }

        Ok(response)
    }

    async fn run_explain(
        &self,
        pool: &PoolHandle,
        sql: &str,
        query_timeout_ms: u64,
    ) -> Result<ExplainResult> {
        let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
        let explain_fut = async { pool.fetch_all(&explain_sql).await };

        let rows = with_timeout(query_timeout_ms, "EXPLAIN", explain_fut).await?;

        parse_sqlite_explain_from_rows(&rows)
    }

    async fn fetch_list_tables(
        &self,
        pool: &PoolHandle,
        _database: &str,
    ) -> Result<Vec<String>> {
        let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name";
        let rows = pool.fetch_all(sql).await?;
        let tables: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                row.columns
                    .first()
                    .and_then(|(_, v)| v.as_str().map(String::from))
            })
            .collect();
        Ok(tables)
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Get approximate row count for a SQLite table.
async fn get_table_row_count(pool: &PoolHandle, table_name: &str) -> Result<i64> {
    let escaped = escape_sqlite_identifier(table_name);
    let sql = format!("SELECT COUNT(*) AS cnt FROM \"{}\"", escaped);
    let rows = pool.fetch_all(&sql).await?;
    Ok(rows.first().and_then(|r| get_opt_i64(r, "cnt")).unwrap_or(0))
}

/// Get approximate data size for a SQLite table in bytes.
async fn get_table_size(pool: &PoolHandle, table_name: &str) -> Result<i64> {
    // dbstat uses single-quote string comparison for the table name
    let sql = format!(
        "SELECT SUM(pgsize) AS total_size FROM dbstat WHERE name = '{}'",
        table_name.replace('\'', "''")
    );
    let rows = pool.fetch_all(&sql).await?;
    Ok(rows
        .first()
        .and_then(|r| get_opt_i64(r, "total_size"))
        .unwrap_or(0))
}

/// Check if a column is part of a unique index (not the primary key).
async fn check_unique_column(
    pool: &PoolHandle,
    table_name: &str,
    column_name: &str,
) -> Option<String> {
    let escaped = escape_sqlite_identifier(table_name);
    let list_sql = format!("PRAGMA index_list(\"{}\")", escaped);
    let index_rows = pool.fetch_all(&list_sql).await.ok()?;

    for irow in &index_rows {
        let idx_name = get_str(irow, "name");
        let unique_str = get_str(irow, "unique");
        if unique_str != "1" {
            continue;
        }

        let info_sql = format!(
            "PRAGMA index_info(\"{}\")",
            escape_sqlite_identifier(&idx_name)
        );
        let info_rows = pool.fetch_all(&info_sql).await.ok()?;

        for ir in &info_rows {
            let col = get_str(ir, "name");
            if col == column_name {
                return Some("UNI".to_string());
            }
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_backend_kind() {
        let backend = SqliteBackend::new();
        assert_eq!(backend.kind(), BackendKind::Sqlite);
        assert_eq!(backend.backend_name(), "SQLite");
        assert_eq!(backend.default_port(), 0);
        assert!(!backend.supports_ssl());
        assert!(!backend.supports_ssh_tunnel());
        assert!(!backend.supports_network());
    }

    #[test]
    fn test_sqlite_quote_identifier() {
        let backend = SqliteBackend::new();
        assert_eq!(backend.quote_identifier("id"), "\"id\"");
        assert_eq!(backend.quote_identifier("my table"), "\"my table\"");
        assert_eq!(
            backend.quote_identifier("col\"with\"quotes"),
            "\"col\"\"with\"\"quotes\""
        );
    }

    #[test]
    fn test_resolve_sqlite_path_explicit() {
        let conn = crate::config::ConnectionConfig {
            path: Some("/tmp/test.db".to_string()),
            ..Default::default()
        };
        assert_eq!(resolve_sqlite_path(&conn).unwrap(), "/tmp/test.db");
    }

    #[test]
    fn test_resolve_sqlite_path_connection_string_sqlite_slash() {
        let conn = crate::config::ConnectionConfig {
            connection_string: Some("sqlite:///tmp/other.db".to_string()),
            ..Default::default()
        };
        assert_eq!(resolve_sqlite_path(&conn).unwrap(), "/tmp/other.db");
    }

    #[test]
    fn test_resolve_sqlite_path_connection_string_sqlite_colon() {
        let conn = crate::config::ConnectionConfig {
            connection_string: Some("sqlite:/tmp/other.db".to_string()),
            ..Default::default()
        };
        assert_eq!(resolve_sqlite_path(&conn).unwrap(), "/tmp/other.db");
    }

    #[test]
    fn test_resolve_sqlite_path_default_memory() {
        let conn = crate::config::ConnectionConfig {
            ..Default::default()
        };
        assert_eq!(resolve_sqlite_path(&conn).unwrap(), ":memory:");
    }

    #[test]
    fn test_resolve_sqlite_path_invalid_scheme() {
        let conn = crate::config::ConnectionConfig {
            connection_string: Some("postgres://localhost/db".to_string()),
            ..Default::default()
        };
        assert!(resolve_sqlite_path(&conn).is_err());
    }

    #[test]
    fn test_escape_sqlite_identifier() {
        assert_eq!(escape_sqlite_identifier("id"), "id");
        assert_eq!(escape_sqlite_identifier("a\"b"), "a\"\"b");
        assert_eq!(escape_sqlite_identifier(""), "");
    }
}
