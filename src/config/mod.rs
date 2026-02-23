use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod env_config;
pub mod merge;
#[cfg(test)]
mod tests;

/// Top-level configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub connection: ConnectionConfig,
    pub pool: PoolConfig,
    pub security: SecurityConfig,
    pub ssh: Option<SshConfig>,
}

#[derive(Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SshConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub private_key: Option<String>,
    /// One of: "strict", "accept-new", "insecure"
    /// Maps to SSH StrictHostKeyChecking: yes, accept-new, no
    pub known_hosts_check: String,
    pub known_hosts_file: Option<String>,
}

impl Default for SshConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 22,
            user: String::new(),
            private_key: None,
            known_hosts_check: "strict".to_string(),
            known_hosts_file: None,
        }
    }
}

/// Per-schema permission overrides
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SchemaPermissions {
    pub allow_insert: Option<bool>,
    pub allow_update: Option<bool>,
    pub allow_delete: Option<bool>,
    pub allow_ddl: Option<bool>,
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

impl std::fmt::Debug for ConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("socket", &self.socket)
            .field("user", &self.user)
            .field("password", &"[redacted]")
            .field("database", &self.database.as_deref().map(|_| "[redacted]"))
            .field(
                "connection_string",
                &self.connection_string.as_ref().map(|_| "[redacted]"),
            )
            .finish()
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

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        // Host is only required when not using a Unix socket
        if self.connection.socket.is_none() && self.connection.host.is_empty() {
            anyhow::bail!("Config error: connection host must not be empty (unless using socket)");
        }

        // Warn if both connection_string and socket are set — connection_string wins
        if self.connection.connection_string.is_some() && self.connection.socket.is_some() {
            eprintln!(
                "Warning: both connection_string and socket are configured; \
                 connection_string takes precedence and socket will be ignored"
            );
        }

        // query_timeout_ms=0 disables query timeouts entirely (infinite wait).
        // This is intentionally allowed for operators who need unbounded query time,
        // but warn so it doesn't go unnoticed.
        if self.pool.query_timeout_ms == 0 {
            eprintln!(
                "Warning: pool.query_timeout_ms is 0 — queries will never time out. \
                 Set to a positive value (e.g. 30000) to limit runaway queries."
            );
        }

        // Port must be in valid range
        if self.connection.port == 0 {
            anyhow::bail!("connection.port must be between 1 and 65535 (got: 0)");
        }

        // connect_timeout_ms=0 means immediate timeout — connections always fail
        if self.pool.connect_timeout_ms == 0 {
            anyhow::bail!("pool.connect_timeout_ms must be > 0 (got: 0 — connections would always time out immediately)");
        }

        // pool.size must be at least 1
        if self.pool.size == 0 {
            anyhow::bail!("pool.size must be >= 1");
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

        // performance_hints must be one of the recognised modes
        if !matches!(
            self.pool.performance_hints.as_str(),
            "none" | "auto" | "always"
        ) {
            anyhow::bail!(
                "Config error: MYSQL_PERFORMANCE_HINTS must be one of: none, auto, always (got: '{}')",
                self.pool.performance_hints
            );
        }

        // SSL CA file must exist if specified
        if let Some(ref ca) = self.security.ssl_ca {
            if !std::path::Path::new(ca).exists() {
                anyhow::bail!("Config error: MYSQL_SSL_CA path does not exist: {}", ca);
            }
        }

        // SSH validation (only when SSH is configured)
        if let Some(ref ssh) = self.ssh {
            if ssh.host.is_empty() {
                anyhow::bail!("ssh.host must not be empty when SSH tunnel is configured");
            }
            if ssh.user.is_empty() {
                anyhow::bail!("ssh.user must not be empty when SSH tunnel is configured");
            }
            if !matches!(
                ssh.known_hosts_check.as_str(),
                "strict" | "accept-new" | "insecure"
            ) {
                anyhow::bail!(
                    "ssh.known_hosts_check must be one of: strict, accept-new, insecure (got: '{}')",
                    ssh.known_hosts_check
                );
            }
            if let Some(ref key_path) = ssh.private_key {
                if !std::path::Path::new(key_path).exists() {
                    anyhow::bail!("ssh.private_key path does not exist: {}", key_path);
                }
            }
            if let Some(ref khf) = ssh.known_hosts_file {
                if !std::path::Path::new(khf).exists() {
                    anyhow::bail!("ssh.known_hosts_file path does not exist: {}", khf);
                }
            }
        }

        Ok(())
    }
}
