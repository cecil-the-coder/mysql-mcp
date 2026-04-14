//! MySQL-specific backend implementation.
//!
//! Implements `Backend` and `PoolOps` traits for MySQL/MariaDB databases.
//! All MySQL-specific code (pool creation, type serialization, information_schema
//! queries, EXPLAIN execution) lives here.

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
use crate::query::explain_mysql::parse_mysql_explain;
use crate::query::with_timeout;
use crate::schema::{ColumnInfo, IndexDef, TableInfo};
use crate::tunnel::TunnelHandle;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of prepared statements cached per connection.
pub(crate) const STATEMENT_CACHE_CAPACITY: usize = 100;

/// Idle connection lifetime before it is closed and removed from the pool.
const POOL_IDLE_TIMEOUT_SECS: u64 = 300;

/// Maximum lifetime of any pooled connection before it is recycled.
const POOL_MAX_LIFETIME_SECS: u64 = 1800;

/// Binary columns with invalid UTF-8 are hex-encoded. Cap the output at 512 KB
/// of raw bytes (-> ~1 MB hex string) to prevent OOM on unexpectedly large BLOBs.
const MAX_BINARY_DISPLAY_BYTES: usize = 512 * 1024;

// ---------------------------------------------------------------------------
// MySqlPoolWrapper — wraps MySqlPool, implements PoolOps
// ---------------------------------------------------------------------------

/// Internal wrapper that holds a `MySqlPool` and implements `PoolOps`.
/// This is the concrete type behind `PoolHandle` for MySQL backends.
#[derive(Clone)]
pub(crate) struct MySqlPoolWrapper {
    pool: sqlx::MySqlPool,
}

impl MySqlPoolWrapper {
    pub fn new(pool: sqlx::MySqlPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PoolOps for MySqlPoolWrapper {
    async fn fetch_all(&self, sql: &str) -> Result<Vec<RowData>> {
        let rows = sqlx::query(sql).fetch_all(&self.pool).await?;
        let mut warnings = Vec::new();
        let result: Vec<RowData> = rows
            .iter()
            .map(|row| mysql_row_to_row_data(row, &mut warnings))
            .collect();
        for w in warnings {
            tracing::debug!("{}", w);
        }
        Ok(result)
    }

    async fn execute(&self, sql: &str) -> Result<ExecuteResult> {
        let result: sqlx::mysql::MySqlQueryResult = sqlx::query(sql)
            .execute(&self.pool)
            .await?;
        let last_insert_id = result.last_insert_id();
        Ok(ExecuteResult {
            rows_affected: result.rows_affected(),
            last_insert_id: (last_insert_id > 0).then_some(last_insert_id),
        })
    }

    async fn execute_in_transaction(&self, sql: &str) -> Result<ExecuteResult> {
        let mut tx = self.pool.begin().await?;
        let result: sqlx::mysql::MySqlQueryResult = sqlx::query(sql)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        let last_insert_id = result.last_insert_id();
        Ok(ExecuteResult {
            rows_affected: result.rows_affected(),
            last_insert_id: (last_insert_id > 0).then_some(last_insert_id),
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
// MySQL type-aware row serialization
// ---------------------------------------------------------------------------

/// Convert a MySQL row to `RowData`, performing type-aware serialization.
fn mysql_row_to_row_data(
    row: &sqlx::mysql::MySqlRow,
    warnings: &mut Vec<String>,
) -> RowData {
    let mut columns = Vec::with_capacity(row.columns().len());
    for (i, col) in row.columns().iter().enumerate() {
        let value = mysql_column_to_json(row, i, col, warnings);
        columns.push((col.name().to_string(), value));
    }
    RowData { columns }
}

/// Convert a single MySQL column value to JSON.
fn mysql_column_to_json(
    row: &sqlx::mysql::MySqlRow,
    idx: usize,
    col: &sqlx::mysql::MySqlColumn,
    warnings: &mut Vec<String>,
) -> Value {
    let type_name = col.type_info().name();
    match type_name {
        "TINYINT(1)" | "BOOLEAN" | "BOOL" => {
            if let Ok(v) = row.try_get::<Option<bool>, _>(idx) {
                return v.map(Value::Bool).unwrap_or(Value::Null);
            }
        }
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
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
        }
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
            if let Ok(v) = row.try_get::<Option<u64>, _>(idx) {
                return v.map(|n| serde_json::json!(n)).unwrap_or(Value::Null);
            }
        }
        "BIGINT UNSIGNED" => {
            if let Ok(v) = row.try_get::<Option<u64>, _>(idx) {
                return v
                    .map(|n| {
                        if n > 9_007_199_254_740_992u64 {
                            Value::String(n.to_string())
                        } else {
                            serde_json::json!(n)
                        }
                    })
                    .unwrap_or(Value::Null);
            }
        }
        "FLOAT" | "DOUBLE" => match row.try_get::<Option<f64>, _>(idx) {
            Ok(Some(v)) => {
                return serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .unwrap_or_else(|| {
                        warnings.push(format!(
                            "Column '{}' contains NaN/Infinity value converted to NULL",
                            col.name()
                        ));
                        Value::Null
                    });
            }
            Ok(None) => return Value::Null,
            Err(_) => {}
        },
        "DECIMAL" | "NUMERIC" | "NEWDECIMAL" => {
            match row.try_get_unchecked::<Option<String>, _>(idx) {
                Ok(Some(s)) => return Value::String(s),
                Ok(None) => return Value::Null,
                Err(e) => {
                    tracing::warn!(
                        "DECIMAL column at index {} failed to decode as string: {}",
                        idx,
                        e
                    );
                    return Value::Null;
                }
            }
        }
        "BIT" => {
            if let Ok(v) = row.try_get::<Option<u64>, _>(idx) {
                return v.map(|n| serde_json::json!(n)).unwrap_or(Value::Null);
            }
            if let Ok(v) = row.try_get::<Option<bool>, _>(idx) {
                return v.map(Value::Bool).unwrap_or(Value::Null);
            }
        }
        "YEAR" => {
            if let Ok(v) = row.try_get::<Option<u16>, _>(idx) {
                return v.map(|y| Value::Number(y.into())).unwrap_or(Value::Null);
            }
        }
        "JSON" => {
            if let Ok(Some(s)) = row.try_get::<Option<String>, _>(idx) {
                match serde_json::from_str::<Value>(&s) {
                    Ok(v) => return v,
                    Err(e) => {
                        tracing::warn!(
                            "JSON column could not be parsed (returning as string): {}",
                            e
                        );
                        return Value::String(s);
                    }
                }
            }
            return Value::Null;
        }
        "ENUM" | "SET" => {
            if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }
        "DATETIME" | "TIMESTAMP" => {
            if let Ok(v) = row.try_get_unchecked::<Option<chrono::NaiveDateTime>, _>(idx) {
                return v
                    .map(|dt| Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
                    .unwrap_or(Value::Null);
            }
        }
        "DATE" => {
            if let Ok(v) = row.try_get_unchecked::<Option<chrono::NaiveDate>, _>(idx) {
                return v
                    .map(|d| Value::String(d.format("%Y-%m-%d").to_string()))
                    .unwrap_or(Value::Null);
            }
        }
        "TIME" => {
            if let Ok(v) = row.try_get_unchecked::<Option<chrono::NaiveTime>, _>(idx) {
                return v
                    .map(|t| Value::String(t.format("%H:%M:%S").to_string()))
                    .unwrap_or(Value::Null);
            }
        }
        _ => {}
    }
    // Try string for everything else (VARCHAR, TEXT, CHAR, etc.)
    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return v.map(Value::String).unwrap_or(Value::Null);
    }
    // Try bytes as last resort
    if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
        return match v {
            None => Value::Null,
            Some(b) => String::from_utf8(b).map(Value::String).unwrap_or_else(|e| {
                let bytes = e.into_bytes();
                let total = bytes.len();
                let display = &bytes[..total.min(MAX_BINARY_DISPLAY_BYTES)];
                let mut hex = String::with_capacity(display.len() * 2 + 2);
                hex.push_str("0x");
                for b in display {
                    use std::fmt::Write as _;
                    let _ = write!(hex, "{:02x}", b);
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
            }),
        };
    }
    warnings.push(format!(
        "Column '{}' (type '{}') could not be decoded as text or binary, returning NULL",
        col.name(),
        type_name
    ));
    Value::Null
}

// ---------------------------------------------------------------------------
// MySQL information_schema helpers
// ---------------------------------------------------------------------------

/// Try to extract a String from an information_schema column that may be VARBINARY.
pub(crate) fn is_col_str(row: &sqlx::mysql::MySqlRow, col: &str) -> String {
    use sqlx::Row;
    row.try_get::<String, _>(col)
        .or_else(|_| {
            row.try_get::<Vec<u8>, _>(col)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        })
        .unwrap_or_else(|e| {
            tracing::debug!("Failed to extract column '{}' from row: {}", col, e);
            String::new()
        })
}

pub(crate) fn is_col_str_opt(row: &sqlx::mysql::MySqlRow, col: &str) -> Option<String> {
    use sqlx::Row;
    let s = row
        .try_get::<Option<String>, _>(col)
        .ok()
        .flatten()
        .or_else(|| {
            row.try_get::<Option<Vec<u8>>, _>(col)
                .ok()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        })?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Escape a MySQL identifier for use in backtick-quoted contexts.
pub(crate) fn escape_mysql_identifier(name: &str) -> String {
    name.replace('`', "``")
}

// ---------------------------------------------------------------------------
// Pool creation helpers (extracted from db.rs)
// ---------------------------------------------------------------------------

/// Map the three SSL flags to a `MySqlSslMode`.
pub(crate) fn determine_ssl_mode(ssl: bool, accept_invalid: bool, has_ca: bool) -> sqlx::mysql::MySqlSslMode {
    match (ssl, accept_invalid, has_ca) {
        (false, _, _) => sqlx::mysql::MySqlSslMode::Disabled,
        (true, true, _) => sqlx::mysql::MySqlSslMode::Required,
        (true, false, true) => sqlx::mysql::MySqlSslMode::VerifyCa,
        (true, false, false) => sqlx::mysql::MySqlSslMode::VerifyIdentity,
    }
}

/// Apply consistent pool sizing, timeouts, and lifetime settings.
async fn create_pool(
    opts: sqlx::mysql::MySqlConnectOptions,
    max_connections: u32,
    acquire_timeout_ms: u64,
) -> Result<sqlx::MySqlPool> {
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_millis(acquire_timeout_ms))
        .idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
        .max_lifetime(Duration::from_secs(POOL_MAX_LIFETIME_SECS))
        .connect_with(opts)
        .await?;
    Ok(pool)
}

/// Build MySqlConnectOptions from the application Config.
pub(crate) fn build_connect_options(config: &Config) -> Result<sqlx::mysql::MySqlConnectOptions> {
    let conn = &config.connection;

    if let Some(cs) = &conn.connection_string {
        if cs.starts_with("mysql://") || cs.starts_with("mysql+ssl://") {
            let opts = sqlx::mysql::MySqlConnectOptions::from_str(cs)?;
            return Ok(opts);
        } else {
            let split_pos = if let Some(i) = cs.find("://") {
                i + 3
            } else {
                let mut pos = cs.len().min(32);
                while pos > 0 && !cs.is_char_boundary(pos) {
                    pos -= 1;
                }
                pos
            };
            anyhow::bail!(
                "connection_string must start with 'mysql://' or 'mysql+ssl://', got: '{}'",
                &cs[..split_pos]
            );
        }
    }

    if let Some(socket) = &conn.socket {
        let mut opts = sqlx::mysql::MySqlConnectOptions::new()
            .socket(socket)
            .username(&conn.user)
            .password(&conn.password);
        if let Some(db) = &conn.database {
            opts = opts.database(db);
        }
        return Ok(opts);
    }

    let ssl_mode = determine_ssl_mode(
        config.security.ssl,
        config.security.ssl_accept_invalid_certs,
        config.security.ssl_ca.is_some(),
    );
    let mut opts = sqlx::mysql::MySqlConnectOptions::new()
        .host(&conn.host)
        .port(conn.port.unwrap_or(3306))
        .username(&conn.user)
        .password(&conn.password)
        .ssl_mode(ssl_mode);
    if let Some(db) = &conn.database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = &config.security.ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
    Ok(opts)
}

/// Build a MySqlPool from the application config.
pub(crate) async fn build_pool(config: &Config) -> Result<sqlx::MySqlPool> {
    let connect_options =
        build_connect_options(config)?.statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    create_pool(connect_options, config.pool.size, config.pool.connect_timeout_ms).await
}

/// Build a pool through an SSH tunnel.
pub(crate) async fn build_pool_tunneled(
    config: &Config,
    tunnel: &TunnelHandle,
) -> Result<sqlx::MySqlPool> {
    let ssl_mode = determine_ssl_mode(
        config.security.ssl,
        config.security.ssl_accept_invalid_certs,
        config.security.ssl_ca.is_some(),
    );
    let mut opts = sqlx::mysql::MySqlConnectOptions::new()
        .host("127.0.0.1")
        .port(tunnel.local_port)
        .username(&config.connection.user)
        .password(&config.connection.password)
        .ssl_mode(ssl_mode)
        .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    if let Some(ref db) = config.connection.database {
        opts = opts.database(db);
    }
    if let Some(ref ca_path) = config.security.ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
    create_pool(opts, config.pool.size, config.pool.connect_timeout_ms).await
}

/// Build a small session pool from raw connection fields.
pub(crate) async fn build_session_pool_internal(params: &SessionConnectParams) -> Result<sqlx::MySqlPool> {
    let mut opts = sqlx::mysql::MySqlConnectOptions::new()
        .host(&params.host)
        .port(params.port)
        .username(&params.user)
        .password(&params.password)
        .ssl_mode(determine_ssl_mode(
            params.ssl,
            params.ssl_accept_invalid_certs,
            params.ssl_ca.is_some(),
        ));
    if let Some(db) = &params.database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = &params.ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
    create_pool(opts, 5, params.connect_timeout_ms).await
}

/// Build a small session pool through an SSH tunnel.
pub(crate) async fn build_session_pool_with_tunnel_internal(
    params: &SessionConnectParams,
    ssh: &crate::config::SshConfig,
) -> Result<(sqlx::MySqlPool, TunnelHandle)> {
    let tunnel = crate::tunnel::spawn_ssh_tunnel(ssh, &params.host, params.port).await?;
    let mut opts = sqlx::mysql::MySqlConnectOptions::new()
        .host("127.0.0.1")
        .port(tunnel.local_port)
        .username(&params.user)
        .password(&params.password)
        .ssl_mode(determine_ssl_mode(
            params.ssl,
            params.ssl_accept_invalid_certs,
            params.ssl_ca.is_some(),
        ));
    if let Some(db) = &params.database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = &params.ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
    let pool = create_pool(opts, 5, params.connect_timeout_ms).await?;
    Ok((pool, tunnel))
}

// ---------------------------------------------------------------------------
// MySqlBackend — implements Backend trait
// ---------------------------------------------------------------------------

/// MySQL backend implementation.
pub struct MySqlBackend;

impl MySqlBackend {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MySqlBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Backend for MySqlBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::MySql
    }

    fn backend_name(&self) -> &str {
        "MySQL"
    }

    fn default_port(&self) -> u16 {
        3306
    }

    fn sql_dialect(&self) -> &str {
        "MySQL"
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", escape_mysql_identifier(name))
    }

    fn supports_ssl(&self) -> bool {
        true
    }

    fn supports_ssh_tunnel(&self) -> bool {
        true
    }

    fn supports_network(&self) -> bool {
        true
    }

    async fn create_pool(&self, config: &Config, _security: &SecurityConfig) -> Result<PoolHandle> {
        let pool = build_pool(config).await?;
        Ok(PoolHandle::new(Box::new(MySqlPoolWrapper::new(pool))))
    }

    async fn create_session_pool(&self, params: &SessionConnectParams) -> Result<PoolHandle> {
        let pool = build_session_pool_internal(params).await?;
        Ok(PoolHandle::new(Box::new(MySqlPoolWrapper::new(pool))))
    }

    async fn create_tunnel_pool(
        &self,
        _tunnel: &TunnelHandle,
        _params: &SessionConnectParams,
    ) -> Result<(PoolHandle, TunnelHandle)> {
        // Tunnel pool creation is handled directly by the session store,
        // which needs the TunnelHandle. This method is not used for MySQL;
        // instead, the session store calls build_session_pool_with_tunnel_internal directly.
        anyhow::bail!("MySQL tunnel pool creation must go through the session store")
    }

    async fn fetch_tables(
        &self,
        pool: &PoolHandle,
        database: Option<&str>,
    ) -> Result<Vec<TableInfo>> {
        let sql = if let Some(db) = database {
            format!(
                r#"SELECT
                    TABLE_NAME as name,
                    TABLE_SCHEMA as `schema`,
                    TABLE_ROWS as row_count,
                    DATA_LENGTH as data_size_bytes,
                    CREATE_TIME as create_time,
                    UPDATE_TIME as update_time
                FROM information_schema.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND table_schema = '{}'
                ORDER BY TABLE_SCHEMA, TABLE_NAME"#,
                db.replace('\'', "''")
            )
        } else {
            r#"SELECT
                TABLE_NAME as name,
                TABLE_SCHEMA as `schema`,
                TABLE_ROWS as row_count,
                DATA_LENGTH as data_size_bytes,
                CREATE_TIME as create_time,
                UPDATE_TIME as update_time
            FROM information_schema.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
            ORDER BY TABLE_SCHEMA, TABLE_NAME"#
                .to_string()
        };
        let rows = pool.fetch_all(&sql).await?;

        let tables: Vec<TableInfo> = rows
            .iter()
            .map(|row| {
                // Find column by name in RowData
                let get_str = |row: &RowData, col: &str| -> String {
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
                };
                let get_opt_str = |row: &RowData, col: &str| -> Option<String> {
                    row.columns
                        .iter()
                        .find(|(name, _)| name == col)
                        .and_then(|(_, v)| v.as_str().map(String::from))
                        .filter(|s| !s.is_empty())
                };
                let get_opt_i64 = |row: &RowData, col: &str| -> Option<i64> {
                    row.columns
                        .iter()
                        .find(|(name, _)| name == col)
                        .and_then(|(_, v)| v.as_i64())
                };

                TableInfo {
                    name: get_str(row, "name"),
                    schema: get_str(row, "schema"),
                    row_count: get_opt_i64(row, "row_count"),
                    data_size_bytes: get_opt_i64(row, "data_size_bytes"),
                    create_time: get_opt_str(row, "create_time"),
                    update_time: get_opt_str(row, "update_time"),
                }
            })
            .collect();

        Ok(tables)
    }

    async fn fetch_columns(
        &self,
        pool: &PoolHandle,
        table_name: &str,
        database: Option<&str>,
    ) -> Result<Vec<ColumnInfo>> {
        let sql = if let Some(db) = database {
            format!(
                r#"SELECT
                    COLUMN_NAME as name,
                    DATA_TYPE as data_type,
                    COLUMN_TYPE as column_type,
                    IS_NULLABLE as is_nullable,
                    COLUMN_DEFAULT as column_default,
                    COLUMN_KEY as column_key,
                    EXTRA as extra
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'
                ORDER BY ORDINAL_POSITION"#,
                db.replace('\'', "''"),
                table_name.replace('\'', "''")
            )
        } else {
            format!(
                r#"SELECT
                    COLUMN_NAME as name,
                    DATA_TYPE as data_type,
                    COLUMN_TYPE as column_type,
                    IS_NULLABLE as is_nullable,
                    COLUMN_DEFAULT as column_default,
                    COLUMN_KEY as column_key,
                    EXTRA as extra
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'
                ORDER BY ORDINAL_POSITION"#,
                table_name.replace('\'', "''")
            )
        };
        let rows = pool.fetch_all(&sql).await?;

        let columns: Vec<ColumnInfo> = rows
            .iter()
            .map(|row| {
                let get_str = |row: &RowData, col: &str| -> String {
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
                };
                let get_opt_str = |row: &RowData, col: &str| -> Option<String> {
                    row.columns
                        .iter()
                        .find(|(name, _)| name == col)
                        .and_then(|(_, v)| v.as_str().map(String::from))
                        .filter(|s| !s.is_empty())
                };

                let nullable_str = get_str(row, "is_nullable");
                ColumnInfo {
                    name: get_str(row, "name"),
                    data_type: get_str(row, "data_type"),
                    column_type: get_str(row, "column_type"),
                    is_nullable: nullable_str == "YES",
                    column_default: get_opt_str(row, "column_default"),
                    column_key: get_opt_str(row, "column_key"),
                    extra: get_opt_str(row, "extra"),
                }
            })
            .collect();

        Ok(columns)
    }

    async fn fetch_indexed_columns(
        &self,
        pool: &PoolHandle,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<String>> {
        let qualified = match database {
            Some(db) => format!(
                "`{}`.`{}`",
                escape_mysql_identifier(db),
                escape_mysql_identifier(table)
            ),
            None => format!("`{}`", escape_mysql_identifier(table)),
        };
        let sql = format!("SHOW INDEX FROM {}", qualified);
        let rows = pool.fetch_all(&sql).await?;

        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cols: Vec<String> = Vec::new();
        for row in &rows {
            if let Some(col_name) = row
                .columns
                .iter()
                .find(|(name, _)| name == "Column_name")
                .and_then(|(_, v)| v.as_str())
            {
                if !col_name.is_empty() && seen.insert(col_name.to_lowercase()) {
                    cols.push(col_name.to_string());
                }
            }
        }
        Ok(cols)
    }

    async fn fetch_composite_indexes(
        &self,
        pool: &PoolHandle,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<IndexDef>> {
        let sql = if let Some(db) = database {
            format!(
                "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}' \
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX",
                db.replace('\'', "''"),
                table.replace('\'', "''")
            )
        } else {
            format!(
                "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}' \
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX",
                table.replace('\'', "''")
            )
        };
        let rows = pool.fetch_all(&sql).await?;

        let mut index_map: std::collections::BTreeMap<String, IndexDef> =
            std::collections::BTreeMap::new();
        for row in &rows {
            let get_str = |row: &RowData, col: &str| -> String {
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
            };

            let name = get_str(row, "INDEX_NAME");
            let non_unique: i64 = row
                .columns
                .iter()
                .find(|(n, _)| n == "NON_UNIQUE")
                .and_then(|(_, v)| v.as_i64())
                .unwrap_or(1);
            let col = get_str(row, "COLUMN_NAME");
            let entry = index_map.entry(name.clone()).or_insert_with(|| IndexDef {
                name,
                unique: non_unique == 0,
                columns: Vec::new(),
            });
            if !col.is_empty() {
                entry.columns.push(col);
            }
        }

        Ok(index_map.into_values().collect())
    }

    async fn fetch_schema_details(
        &self,
        pool: &PoolHandle,
        table_name: &str,
        database: Option<&str>,
        include_indexes: bool,
        include_fks: bool,
        include_size: bool,
    ) -> Result<serde_json::Value> {
        // Fetch columns
        let columns = self.fetch_columns(pool, table_name, database).await?;

        // Indexes
        let indexes: serde_json::Value = if include_indexes {
            let schema_filter = match database {
                Some(db) => format!("= '{}'", db.replace('\'', "''")),
                None => "= DATABASE()".to_string(),
            };
            let sql = format!(
                "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, INDEX_TYPE, NULLABLE \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA {} AND TABLE_NAME = '{}' \
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX",
                schema_filter,
                table_name.replace('\'', "''")
            );
            let rows = pool.fetch_all(&sql).await?;

            let mut idx_map: std::collections::BTreeMap<String, serde_json::Value> =
                std::collections::BTreeMap::new();
            for row in &rows {
                let get_str = |row: &RowData, col: &str| -> String {
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
                };
                let name: String = get_str(row, "INDEX_NAME");
                let non_unique: i64 = row
                    .columns
                    .iter()
                    .find(|(n, _)| n == "NON_UNIQUE")
                    .and_then(|(_, v)| v.as_i64())
                    .unwrap_or(1);
                let col: String = get_str(row, "COLUMN_NAME");
                let idx_type: String = get_str(row, "INDEX_TYPE");
                let nullable: String = get_str(row, "NULLABLE");
                let entry = idx_map.entry(name.clone()).or_insert_with(|| {
                    serde_json::json!({
                        "name": name, "unique": non_unique == 0, "type": idx_type, "columns": [],
                    })
                });
                if let Some(cols) = entry.get_mut("columns").and_then(|v| v.as_array_mut()) {
                    cols.push(serde_json::json!({ "column": col, "nullable": nullable == "YES" }));
                }
            }
            serde_json::Value::Array(idx_map.into_values().collect())
        } else {
            serde_json::Value::Null
        };

        // Foreign keys
        let foreign_keys: serde_json::Value = if include_fks {
            let schema_filter = match database {
                Some(db) => format!("= '{}'", db.replace('\'', "''")),
                None => "= DATABASE()".to_string(),
            };
            let sql = format!(
                "SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, \
                     kcu.REFERENCED_COLUMN_NAME, rc.UPDATE_RULE, rc.DELETE_RULE \
                 FROM information_schema.KEY_COLUMN_USAGE kcu \
                 JOIN information_schema.REFERENTIAL_CONSTRAINTS rc \
                   ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
                  AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA \
                 WHERE kcu.TABLE_SCHEMA {} AND kcu.TABLE_NAME = '{}' \
                   AND kcu.REFERENCED_TABLE_NAME IS NOT NULL",
                schema_filter,
                table_name.replace('\'', "''")
            );
            let rows = pool.fetch_all(&sql).await?;
            serde_json::Value::Array(
                rows.iter()
                    .map(|row| {
                        let get_str = |row: &RowData, col: &str| -> String {
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
                        };
                        serde_json::json!({
                            "constraint":        get_str(row, "CONSTRAINT_NAME"),
                            "column":            get_str(row, "COLUMN_NAME"),
                            "references_table":  get_str(row, "REFERENCED_TABLE_NAME"),
                            "references_column": get_str(row, "REFERENCED_COLUMN_NAME"),
                            "on_update":         get_str(row, "UPDATE_RULE"),
                            "on_delete":         get_str(row, "DELETE_RULE"),
                        })
                    })
                    .collect(),
            )
        } else {
            serde_json::Value::Null
        };

        // Table size
        let size: serde_json::Value = if include_size {
            let schema_filter = match database {
                Some(db) => format!("= '{}'", db.replace('\'', "''")),
                None => "= DATABASE()".to_string(),
            };
            let sql = format!(
                "SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH \
                 FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA {} AND TABLE_NAME = '{}'",
                schema_filter,
                table_name.replace('\'', "''")
            );
            let rows = pool.fetch_all(&sql).await?;
            match rows.into_iter().next() {
                Some(row) => {
                    let get_opt_u64 = |row: &RowData, col: &str| -> Option<u64> {
                        row.columns
                            .iter()
                            .find(|(name, _)| name == col)
                            .and_then(|(_, v)| v.as_u64())
                    };
                    serde_json::json!({
                        "estimated_rows": get_opt_u64(&row, "TABLE_ROWS"),
                        "data_bytes":     get_opt_u64(&row, "DATA_LENGTH"),
                        "index_bytes":    get_opt_u64(&row, "INDEX_LENGTH"),
                    })
                }
                None => serde_json::Value::Null,
            }
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
        if !indexes.is_null() {
            result["indexes"] = indexes;
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
        let sql =
            "SELECT VERSION() AS mysql_version, \
                    CURRENT_USER() AS `current_user`, \
                    DATABASE() AS current_database, \
                    @@sql_mode AS sql_mode, \
                    @@character_set_connection AS character_set, \
                    @@collation_connection AS collation, \
                    @@time_zone AS time_zone, \
                    @@read_only AS read_only";
        let rows = pool.fetch_all(sql).await?;
        if rows.is_empty() {
            anyhow::bail!("No response from server");
        }
        let row = &rows[0];

        let get_str = |row: &RowData, col: &str| -> String {
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
        };
        let get_opt_str = |row: &RowData, col: &str| -> Option<String> {
            row.columns
                .iter()
                .find(|(name, _)| name == col)
                .and_then(|(_, v)| v.as_str().map(String::from))
                .filter(|s| !s.is_empty())
        };
        let get_i64 = |row: &RowData, col: &str| -> i64 {
            row.columns
                .iter()
                .find(|(name, _)| name == col)
                .and_then(|(_, v)| v.as_i64())
                .unwrap_or(0)
        };

        let version = get_str(row, "mysql_version");
        let user = get_str(row, "current_user");
        let db = get_opt_str(row, "current_database");
        let sql_mode = get_str(row, "sql_mode");
        let character_set = get_str(row, "character_set");
        let collation = get_str(row, "collation");
        let time_zone = get_str(row, "time_zone");
        let read_only = get_i64(row, "read_only") != 0;

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
            "mysql_version": version,
            "current_user": user,
            "current_database": db,
            "sql_mode": sql_mode,
            "character_set": character_set,
            "collation": collation,
            "time_zone": time_zone,
            "read_only": read_only,
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
        let explain_sql = format!("EXPLAIN FORMAT=JSON {}", sql);
        let explain_fut = async { pool.fetch_all(&explain_sql).await };

        let rows = with_timeout(query_timeout_ms, "EXPLAIN", explain_fut).await?;

        if rows.is_empty() {
            anyhow::bail!("EXPLAIN returned no rows");
        }

        let row = &rows[0];
        let json_str = row
            .columns
            .first()
            .and_then(|(_, v)| v.as_str().map(String::from));

        let json_str = match json_str {
            Some(s) => s,
            None => anyhow::bail!("EXPLAIN returned non-string result"),
        };

        let v: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| {
            let location = format!("at line {}, column {}", e.line(), e.column());
            let top_level_keys: Vec<String> = serde_json::from_str::<serde_json::Value>(&json_str)
                .ok()
                .and_then(|v| v.as_object().map(|obj| obj.keys().cloned().collect()))
                .unwrap_or_default();
            let structure_info = if top_level_keys.is_empty() {
                "JSON is not a valid object or could not be partially parsed".to_string()
            } else {
                format!("top-level keys: {}", top_level_keys.join(", "))
            };
            anyhow::anyhow!(
                "Failed to parse EXPLAIN JSON: {} {}. JSON structure: {}",
                e,
                location,
                structure_info
            )
        })?;

        parse_mysql_explain(&v)
    }

    async fn fetch_list_tables(
        &self,
        pool: &PoolHandle,
        database: &str,
    ) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT CAST(TABLE_NAME AS CHAR) AS TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{}' ORDER BY TABLE_NAME",
            database.replace('\'', "''")
        );
        let rows = pool.fetch_all(&sql).await?;
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
// Backward-compatible re-exports for db.rs and schema/fetch.rs
// ---------------------------------------------------------------------------

/// Read DB_QUERY_TIMEOUT from environment, returning the value in milliseconds.
/// Returns 0 (no timeout) if not set or if parsing fails.
pub(crate) fn query_timeout_from_env() -> u64 {
    std::env::var("DB_QUERY_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn ssl_mode_is(mode: sqlx::mysql::MySqlSslMode, expected: &str) -> bool {
        format!("{:?}", mode) == expected
    }

    #[test]
    fn test_determine_ssl_mode_disabled() {
        assert!(ssl_mode_is(determine_ssl_mode(false, false, false), "Disabled"));
        assert!(ssl_mode_is(determine_ssl_mode(false, true, false), "Disabled"));
        assert!(ssl_mode_is(determine_ssl_mode(false, false, true), "Disabled"));
        assert!(ssl_mode_is(determine_ssl_mode(false, true, true), "Disabled"));
    }

    #[test]
    fn test_determine_ssl_mode_required() {
        assert!(ssl_mode_is(determine_ssl_mode(true, true, false), "Required"));
        assert!(ssl_mode_is(determine_ssl_mode(true, true, true), "Required"));
    }

    #[test]
    fn test_determine_ssl_mode_verify_ca() {
        assert!(ssl_mode_is(determine_ssl_mode(true, false, true), "VerifyCa"));
    }

    #[test]
    fn test_determine_ssl_mode_verify_identity() {
        assert!(ssl_mode_is(determine_ssl_mode(true, false, false), "VerifyIdentity"));
    }

    #[test]
    fn test_mysql_backend_kind() {
        let backend = MySqlBackend::new();
        assert_eq!(backend.kind(), BackendKind::MySql);
        assert_eq!(backend.backend_name(), "MySQL");
        assert_eq!(backend.default_port(), 3306);
        assert!(backend.supports_ssl());
        assert!(backend.supports_ssh_tunnel());
        assert!(backend.supports_network());
    }
}
