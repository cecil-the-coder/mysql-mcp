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
    pub queue_limit: u32,
    pub cache_ttl_secs: u64,
    pub readonly_transaction: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SecurityConfig {
    pub allow_insert: bool,
    pub allow_update: bool,
    pub allow_delete: bool,
    pub allow_ddl: bool,
    pub disable_read_only_transactions: bool,
    pub ssl: bool,
    pub ssl_accept_invalid_certs: bool,
    /// Per-schema permission overrides: schema_name -> SchemaPermissions
    pub schema_permissions: HashMap<String, SchemaPermissions>,
    pub multi_db_write_mode: bool,
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
            size: 10,
            query_timeout_ms: 30_000,
            connect_timeout_ms: 10_000,
            queue_limit: 100,
            cache_ttl_secs: 60,
            readonly_transaction: true,
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
            disable_read_only_transactions: false,
            ssl: false,
            ssl_accept_invalid_certs: false,
            schema_permissions: HashMap::new(),
            multi_db_write_mode: false,
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
