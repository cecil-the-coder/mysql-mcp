use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod toml_config;
pub mod env_config;
pub mod merge;
pub mod cli_parser;
#[cfg(test)]
mod tests;

/// Top-level configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub connection: ConnectionConfig,
    pub pool: PoolConfig,
    pub security: SecurityConfig,
    pub remote: RemoteConfig,
    pub monitoring: MonitoringConfig,
    pub timezone: Option<String>,
    pub date_strings: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub socket: Option<String>,
    pub user: String,
    pub password: String,
    pub database: Option<String>,
    /// Full connection string (overrides individual fields when set)
    pub connection_string: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PoolConfig {
    pub size: u32,
    pub query_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub cache_ttl_secs: u64,
    pub readonly_transaction: bool,
    pub performance_hints: String,
    pub slow_query_threshold_ms: u64,
    pub warmup_connections: u32,
    pub max_rows: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SecurityConfig {
    pub allow_insert: bool,
    pub allow_update: bool,
    pub allow_delete: bool,
    pub allow_ddl: bool,
    pub ssl: bool,
    pub ssl_accept_invalid_certs: bool,
    /// Path to a PEM CA bundle for SSL verification (optional).
    /// When set and ssl=true, this CA is used instead of the system trust store.
    pub ssl_ca: Option<String>,
    /// Per-schema permission overrides: schema_name -> SchemaPermissions
    pub schema_permissions: HashMap<String, SchemaPermissions>,
    pub multi_db_write_mode: bool,
    /// Allow mysql_connect to accept raw credentials at runtime.
    /// When false (default), only preset-based connections are allowed.
    pub allow_runtime_connections: bool,
    /// Maximum number of concurrent named sessions (not counting the default session).
    /// Prevents unbounded session creation when allow_runtime_connections is true.
    pub max_sessions: u32,
}

/// Per-schema permission overrides
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SchemaPermissions {
    pub allow_insert: Option<bool>,
    pub allow_update: Option<bool>,
    pub allow_delete: Option<bool>,
    pub allow_ddl: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct RemoteConfig {
    pub enabled: bool,
    pub secret_key: Option<String>,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct MonitoringConfig {
    pub logging: bool,
    pub log_level: String,
    pub metrics_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            pool: PoolConfig::default(),
            security: SecurityConfig::default(),
            remote: RemoteConfig::default(),
            monitoring: MonitoringConfig::default(),
            timezone: None,
            date_strings: false,
        }
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            socket: None,
            user: "root".to_string(),
            password: String::new(),
            database: None,
            connection_string: None,
        }
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            size: 20,
            query_timeout_ms: 30_000,
            connect_timeout_ms: 10_000,
            cache_ttl_secs: 60,
            readonly_transaction: false,
            performance_hints: "none".to_string(),
            slow_query_threshold_ms: 500,
            warmup_connections: 1,
            max_rows: 1000,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            allow_insert: false,
            allow_update: false,
            allow_delete: false,
            allow_ddl: false,
            ssl: false,
            ssl_accept_invalid_certs: false,
            ssl_ca: None,
            schema_permissions: HashMap::new(),
            multi_db_write_mode: false,
            allow_runtime_connections: false,
            max_sessions: 50,
        }
    }
}

impl Default for RemoteConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            secret_key: None,
            port: 3000,
        }
    }
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            logging: true,
            log_level: "info".to_string(),
            metrics_enabled: false,
        }
    }
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        // Pool size must be 1..=1000
        if self.pool.size == 0 {
            anyhow::bail!("Config error: MYSQL_POOL_SIZE cannot be 0 (got 0)");
        }
        if self.pool.size > 1000 {
            anyhow::bail!(
                "Config error: MYSQL_POOL_SIZE unreasonably large: {} (max 1000)",
                self.pool.size
            );
        }

        // Timeouts must be at least 100ms
        if self.pool.query_timeout_ms < 100 {
            anyhow::bail!(
                "Config error: MYSQL_QUERY_TIMEOUT must be at least 100ms (got {}ms)",
                self.pool.query_timeout_ms
            );
        }
        if self.pool.connect_timeout_ms < 100 {
            anyhow::bail!(
                "Config error: MYSQL_CONNECT_TIMEOUT must be at least 100ms (got {}ms)",
                self.pool.connect_timeout_ms
            );
        }

        // max_rows must be at least 1
        if self.pool.max_rows == 0 {
            anyhow::bail!("pool.max_rows must be >= 1 (set to a large number like 10000 for effectively unlimited rows)");
        }

        // warmup_connections cannot exceed the pool size
        if self.pool.warmup_connections > self.pool.size {
            anyhow::bail!(
                "pool.warmup_connections ({}) cannot exceed pool.size ({})",
                self.pool.warmup_connections,
                self.pool.size
            );
        }

        if self.security.max_sessions == 0 {
            anyhow::bail!("security.max_sessions must be >= 1");
        }

        // Remote mode requires a non-empty secret key
        if self.remote.enabled {
            match &self.remote.secret_key {
                None => {
                    anyhow::bail!(
                        "Config error: remote mode is enabled but remote.secret_key / MYSQL_REMOTE_SECRET is not set"
                    );
                }
                Some(s) if s.is_empty() => {
                    anyhow::bail!(
                        "Config error: remote mode is enabled but remote.secret_key / MYSQL_REMOTE_SECRET is not set"
                    );
                }
                _ => {}
            }
        }

        Ok(())
    }
}
