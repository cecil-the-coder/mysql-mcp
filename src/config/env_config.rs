use std::collections::HashMap;
use crate::config::{Config, SchemaPermissions};

/// Parse all environment variables and return a partial Config to merge over TOML base.
/// Only sets fields where the env var is actually present.
pub fn load_env_config() -> EnvConfig {
    EnvConfig {
        host: std::env::var("MYSQL_HOST").ok(),
        port: std::env::var("MYSQL_PORT").ok().and_then(|v| v.parse().ok()),
        socket: std::env::var("MYSQL_SOCKET_PATH").ok(),
        user: std::env::var("MYSQL_USER").ok(),
        password: std::env::var("MYSQL_PASS").ok(),
        database: std::env::var("MYSQL_DB").ok(),
        connection_string: std::env::var("MYSQL_CONNECTION_STRING").ok(),
        pool_size: std::env::var("MYSQL_POOL_SIZE").ok().and_then(|v| v.parse().ok()),
        query_timeout_ms: std::env::var("MYSQL_QUERY_TIMEOUT").ok().and_then(|v| v.parse().ok()),
        connect_timeout_ms: std::env::var("MYSQL_CONNECT_TIMEOUT").ok().and_then(|v| v.parse().ok()),
        queue_limit: std::env::var("MYSQL_QUEUE_LIMIT").ok().and_then(|v| v.parse().ok()),
        cache_ttl_secs: std::env::var("MYSQL_CACHE_TTL").ok().and_then(|v| v.parse().ok()),
        allow_insert: parse_bool_env("ALLOW_INSERT_OPERATION"),
        allow_update: parse_bool_env("ALLOW_UPDATE_OPERATION"),
        allow_delete: parse_bool_env("ALLOW_DELETE_OPERATION"),
        allow_ddl: parse_bool_env("ALLOW_DDL_OPERATION"),
        disable_read_only_transactions: parse_bool_env("MYSQL_DISABLE_READ_ONLY_TRANSACTIONS"),
        ssl: parse_bool_env("MYSQL_SSL"),
        multi_db_write_mode: parse_bool_env("MULTI_DB_WRITE_MODE"),
        remote_enabled: parse_bool_env("IS_REMOTE_MCP"),
        remote_secret_key: std::env::var("REMOTE_SECRET_KEY").ok(),
        remote_port: std::env::var("PORT").ok().and_then(|v| v.parse().ok()),
        logging: parse_bool_env("MYSQL_ENABLE_LOGGING"),
        log_level: std::env::var("MYSQL_LOG_LEVEL").ok(),
        metrics_enabled: parse_bool_env("MYSQL_METRICS_ENABLED"),
        timezone: std::env::var("MYSQL_TIMEZONE").ok(),
        date_strings: parse_bool_env("MYSQL_DATE_STRINGS"),
        schema_permissions: parse_schema_permissions(),
    }
}

fn parse_bool_env(key: &str) -> Option<bool> {
    std::env::var(key).ok().map(|v| {
        matches!(v.to_lowercase().as_str(), "true" | "1" | "yes")
    })
}

/// Parse SCHEMA_<NAME>_PERMISSIONS env vars.
/// Format: SCHEMA_mydb_PERMISSIONS=insert,update (comma-separated allowed ops)
fn parse_schema_permissions() -> HashMap<String, SchemaPermissions> {
    let mut map = HashMap::new();
    for (key, val) in std::env::vars() {
        if let Some(schema_name) = key.strip_prefix("SCHEMA_").and_then(|s| s.strip_suffix("_PERMISSIONS")) {
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

/// All env var overrides (None = not set, don't override)
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
    pub queue_limit: Option<u32>,
    pub cache_ttl_secs: Option<u64>,
    pub allow_insert: Option<bool>,
    pub allow_update: Option<bool>,
    pub allow_delete: Option<bool>,
    pub allow_ddl: Option<bool>,
    pub disable_read_only_transactions: Option<bool>,
    pub ssl: Option<bool>,
    pub multi_db_write_mode: Option<bool>,
    pub remote_enabled: Option<bool>,
    pub remote_secret_key: Option<String>,
    pub remote_port: Option<u16>,
    pub logging: Option<bool>,
    pub log_level: Option<String>,
    pub metrics_enabled: Option<bool>,
    pub timezone: Option<String>,
    pub date_strings: Option<bool>,
    pub schema_permissions: HashMap<String, SchemaPermissions>,
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
        if let Some(v) = self.queue_limit { base.pool.queue_limit = v; }
        if let Some(v) = self.cache_ttl_secs { base.pool.cache_ttl_secs = v; }
        if let Some(v) = self.allow_insert { base.security.allow_insert = v; }
        if let Some(v) = self.allow_update { base.security.allow_update = v; }
        if let Some(v) = self.allow_delete { base.security.allow_delete = v; }
        if let Some(v) = self.allow_ddl { base.security.allow_ddl = v; }
        if let Some(v) = self.disable_read_only_transactions { base.security.disable_read_only_transactions = v; }
        if let Some(v) = self.ssl { base.security.ssl = v; }
        if let Some(v) = self.multi_db_write_mode { base.security.multi_db_write_mode = v; }
        if !self.schema_permissions.is_empty() {
            base.security.schema_permissions.extend(self.schema_permissions.clone());
        }
        if let Some(v) = self.remote_enabled { base.remote.enabled = v; }
        if let Some(v) = &self.remote_secret_key { base.remote.secret_key = Some(v.clone()); }
        if let Some(v) = self.remote_port { base.remote.port = v; }
        if let Some(v) = self.logging { base.monitoring.logging = v; }
        if let Some(v) = &self.log_level { base.monitoring.log_level = v.clone(); }
        if let Some(v) = self.metrics_enabled { base.monitoring.metrics_enabled = v; }
        if let Some(v) = &self.timezone { base.timezone = Some(v.clone()); }
        if let Some(v) = self.date_strings { base.date_strings = v; }
        base
    }
}
