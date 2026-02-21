use std::collections::HashMap;
use crate::config::{Config, SchemaPermissions};

// ---------------------------------------------------------------------------
// Helpers — use eprintln! because this runs before the logging system starts.
// ---------------------------------------------------------------------------

fn parse_env_num<T: std::str::FromStr>(key: &str) -> Option<T> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => match v.parse::<T>() {
            Ok(n) => Some(n),
            Err(_) => {
                eprintln!(
                    "Warning: {} is set to {:?} but could not be parsed as a number; using default",
                    key, v
                );
                None
            }
        },
        _ => None,
    }
}

fn parse_bool_env(key: &str) -> Option<bool> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => match v.to_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => {
                eprintln!(
                    "Warning: {} is set to {:?} but is not a recognized boolean \
                     (true/false/1/0/yes/no); using default",
                    key, v
                );
                None
            }
        },
        _ => None,
    }
}

/// Parse all environment variables and return a partial Config to merge over TOML base.
/// Only sets fields where the env var is actually present.
pub fn load_env_config() -> EnvConfig {
    EnvConfig {
        host: std::env::var("MYSQL_HOST").ok(),
        port: parse_env_num::<u16>("MYSQL_PORT"),
        socket: std::env::var("MYSQL_SOCKET_PATH").ok(),
        user: std::env::var("MYSQL_USER").ok(),
        password: std::env::var("MYSQL_PASS").ok(),
        database: std::env::var("MYSQL_DB").ok(),
        connection_string: std::env::var("MYSQL_CONNECTION_STRING").ok(),
        pool_size: parse_env_num::<u32>("MYSQL_POOL_SIZE"),
        query_timeout_ms: parse_env_num::<u64>("MYSQL_QUERY_TIMEOUT"),
        connect_timeout_ms: parse_env_num::<u64>("MYSQL_CONNECT_TIMEOUT"),
        cache_ttl_secs: parse_env_num::<u64>("MYSQL_CACHE_TTL"),
        allow_insert: parse_bool_env("MYSQL_ALLOW_INSERT"),
        allow_update: parse_bool_env("MYSQL_ALLOW_UPDATE"),
        allow_delete: parse_bool_env("MYSQL_ALLOW_DELETE"),
        allow_ddl: parse_bool_env("MYSQL_ALLOW_DDL"),
        readonly_transaction: parse_bool_env("MYSQL_READONLY_TRANSACTION"),
        ssl: parse_bool_env("MYSQL_SSL"),
        ssl_accept_invalid_certs: parse_bool_env("MYSQL_SSL_ACCEPT_INVALID_CERTS"),
        ssl_ca: std::env::var("MYSQL_SSL_CA").ok().filter(|s| !s.is_empty()),
        multi_db_write_mode: parse_bool_env("MYSQL_MULTI_DB_WRITE_MODE"),
        allow_runtime_connections: parse_bool_env("MYSQL_ALLOW_RUNTIME_CONNECTIONS"),
        logging: parse_bool_env("MYSQL_ENABLE_LOGGING"),
        log_level: std::env::var("MYSQL_LOG_LEVEL").ok(),
        metrics_enabled: parse_bool_env("MYSQL_METRICS_ENABLED"),
        timezone: std::env::var("MYSQL_TIMEZONE").ok(),
        date_strings: parse_bool_env("MYSQL_DATE_STRINGS"),
        schema_permissions: parse_schema_permissions(),
        performance_hints: std::env::var("MYSQL_PERFORMANCE_HINTS").ok(),
        slow_query_threshold_ms: parse_env_num::<u64>("MYSQL_SLOW_QUERY_THRESHOLD_MS"),
        warmup_connections: parse_env_num::<u32>("MYSQL_POOL_WARMUP"),
        max_rows: parse_env_num::<u32>("MYSQL_MAX_ROWS"),
        max_sessions: parse_env_num::<u32>("MYSQL_MAX_SESSIONS"),
    }
}

/// Parse MYSQL_SCHEMA_<NAME>_PERMISSIONS env vars.
/// Format: MYSQL_SCHEMA_mydb_PERMISSIONS=insert,update (comma-separated allowed ops)
fn parse_schema_permissions() -> HashMap<String, SchemaPermissions> {
    let mut map = HashMap::new();
    for (key, val) in std::env::vars() {
        if let Some(schema_name) = key.strip_prefix("MYSQL_SCHEMA_").and_then(|s| s.strip_suffix("_PERMISSIONS")) {
            let schema_name = schema_name.to_lowercase();
            let ops: Vec<&str> = val.split(',').map(str::trim).collect();
            let perms = SchemaPermissions {
                allow_insert: Some(ops.contains(&"insert")),
                allow_update: Some(ops.contains(&"update")),
                allow_delete: Some(ops.contains(&"delete")),
                allow_ddl: Some(ops.contains(&"ddl")),
            };
            map.insert(schema_name, perms);
        }
    }
    map
}

/// All env var overrides (None = not set, don't override).
///
/// # Simplification note (mysql-mcp-p0y)
///
/// This struct duplicates every field of `Config` as `Option<T>` to enable the 3-layer
/// merge pattern (defaults → TOML → env vars). The duplication is the main boilerplate
/// cost: adding a new config field requires touching `Config` (mod.rs), `EnvConfig` here,
/// `load_env_config()` here, and `apply_to()` here.
///
/// The simplest safe alternatives would be:
///
/// A) A proc-macro that auto-generates the `Option<T>` parallel struct and `apply_to()` body
///    from `#[env_override(...)]` annotations on `Config` fields.
///
/// B) The `figment` crate, which handles layered configs declaratively with no intermediate
///    struct needed.
///
/// C) Replacing `EnvConfig` with a `Config::apply_env_overrides(&mut self)` method that reads
///    env vars directly. This would break the existing `EnvConfig`-based unit tests in
///    `merge.rs` (which construct `EnvConfig { field: Some(val), ..Default::default() }` to
///    test field-by-field override behavior).
///
/// None of these were implemented at this time because they each either require new
/// dependencies or would change the tested public interface of the config module.
#[derive(Debug, Default)]
pub struct EnvConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub socket: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub connection_string: Option<String>,
    pub pool_size: Option<u32>,
    pub query_timeout_ms: Option<u64>,
    pub connect_timeout_ms: Option<u64>,
    pub cache_ttl_secs: Option<u64>,
    pub allow_insert: Option<bool>,
    pub allow_update: Option<bool>,
    pub allow_delete: Option<bool>,
    pub allow_ddl: Option<bool>,
    pub readonly_transaction: Option<bool>,
    pub ssl: Option<bool>,
    pub ssl_accept_invalid_certs: Option<bool>,
    pub ssl_ca: Option<String>,
    pub multi_db_write_mode: Option<bool>,
    pub allow_runtime_connections: Option<bool>,
    pub logging: Option<bool>,
    pub log_level: Option<String>,
    pub metrics_enabled: Option<bool>,
    pub timezone: Option<String>,
    pub date_strings: Option<bool>,
    pub schema_permissions: HashMap<String, SchemaPermissions>,
    pub performance_hints: Option<String>,
    pub slow_query_threshold_ms: Option<u64>,
    pub warmup_connections: Option<u32>,
    pub max_rows: Option<u32>,
    pub max_sessions: Option<u32>,
}

impl EnvConfig {
    /// Apply env var overrides onto a base Config, returning the merged result.
    pub fn apply_to(&self, mut base: Config) -> Config {
        if let Some(v) = &self.host { base.connection.host = v.clone(); }
        if let Some(v) = self.port { base.connection.port = v; }
        if let Some(v) = &self.socket { base.connection.socket = Some(v.clone()); }
        if let Some(v) = &self.user { base.connection.user = v.clone(); }
        if let Some(v) = &self.password { base.connection.password = v.clone(); }
        if let Some(v) = &self.database { base.connection.database = Some(v.clone()); }
        if let Some(v) = &self.connection_string { base.connection.connection_string = Some(v.clone()); }
        if let Some(v) = self.pool_size { base.pool.size = v; }
        if let Some(v) = self.query_timeout_ms { base.pool.query_timeout_ms = v; }
        if let Some(v) = self.connect_timeout_ms { base.pool.connect_timeout_ms = v; }
        if let Some(v) = self.cache_ttl_secs { base.pool.cache_ttl_secs = v; }
        if let Some(v) = self.allow_insert { base.security.allow_insert = v; }
        if let Some(v) = self.allow_update { base.security.allow_update = v; }
        if let Some(v) = self.allow_delete { base.security.allow_delete = v; }
        if let Some(v) = self.allow_ddl { base.security.allow_ddl = v; }
        if let Some(v) = self.readonly_transaction { base.pool.readonly_transaction = v; }
        if let Some(v) = self.ssl { base.security.ssl = v; }
        if let Some(v) = self.ssl_accept_invalid_certs { base.security.ssl_accept_invalid_certs = v; }
        if let Some(v) = &self.ssl_ca { base.security.ssl_ca = Some(v.clone()); }
        if let Some(v) = self.multi_db_write_mode { base.security.multi_db_write_mode = v; }
        if let Some(v) = self.allow_runtime_connections { base.security.allow_runtime_connections = v; }
        if !self.schema_permissions.is_empty() {
            base.security.schema_permissions.extend(self.schema_permissions.clone());
        }
        if let Some(v) = self.logging { base.monitoring.logging = v; }
        if let Some(v) = &self.log_level { base.monitoring.log_level = v.clone(); }
        if let Some(v) = self.metrics_enabled { base.monitoring.metrics_enabled = v; }
        if let Some(v) = &self.timezone { base.timezone = Some(v.clone()); }
        if let Some(v) = self.date_strings { base.date_strings = v; }
        if let Some(v) = &self.performance_hints { base.pool.performance_hints = v.clone(); }
        if let Some(v) = self.slow_query_threshold_ms { base.pool.slow_query_threshold_ms = v; }
        if let Some(v) = self.warmup_connections { base.pool.warmup_connections = v; }
        if let Some(v) = self.max_rows { base.pool.max_rows = v; }
        if let Some(v) = self.max_sessions { base.security.max_sessions = v; }
        base
    }
}
