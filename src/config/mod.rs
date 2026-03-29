//! Configuration management for the MySQL MCP server.
//!
//! This module provides configuration structures and loading logic for database
//! connections, connection pooling, security settings, and SSH tunnel options.
//! Configuration can be provided via TOML files and/or environment variables.
//!
//! # Key Types
//!
//! - [`Config`] - Top-level configuration container
//! - [`ConnectionConfig`] - MySQL connection parameters (host, port, credentials)
//! - [`PoolConfig`] - Connection pool sizing and timeouts
//! - [`SecurityConfig`] - Write permissions and SSL settings
//! - [`SshConfig`] - SSH tunnel configuration for bastion host access
//! - [`SchemaPermissions`] - Per-schema permission overrides
//!
//! # Example
//!
//! ```ignore
//! use config::load_config;
//! let config = load_config()?;
//! config.validate()?;
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::warn;

pub mod env_config;
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
    pub performance_hints: String,
    pub slow_query_threshold_ms: u64,
    pub max_rows: u32,
    /// Number of retry attempts for transient network errors (default: 2).
    /// Retries use exponential backoff (100ms, 200ms) between attempts.
    pub retry_attempts: u32,
    /// Maximum memory in MB for result sets (default: 256).
    /// When exceeded, results are truncated with a warning in parse_warnings.
    pub max_result_memory_mb: u32,
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
    /// Allow mysql_connect to accept raw credentials at runtime.
    /// When false (default), only preset-based connections are allowed.
    pub allow_runtime_connections: bool,
    /// Maximum number of concurrent named sessions (not counting the default session).
    /// Prevents unbounded session creation when allow_runtime_connections is true.
    pub max_sessions: u32,
    /// Maximum total database connections across all sessions (default pool + named session pools).
    /// Named sessions use 5 connections each. Prevents resource exhaustion.
    pub max_total_connections: u32,
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
            performance_hints: "none".to_string(),
            slow_query_threshold_ms: 500,
            max_rows: 1000,
            retry_attempts: 2,
            max_result_memory_mb: 256,
        }
    }
}

impl SecurityConfig {
    /// Returns a list of security warnings that should be shown to tool users.
    /// These warnings are surfaced in tool responses so LLM users are aware of
    /// configuration choices that reduce security.
    pub fn security_warnings(&self) -> Vec<String> {
        let mut warnings = vec![];
        if self.ssl_accept_invalid_certs {
            warnings.push(
                "SSL certificate validation is disabled — connection vulnerable to MITM attacks"
                    .to_string(),
            );
        }
        warnings
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
            allow_runtime_connections: false,
            max_sessions: 50,
            max_total_connections: 100,
        }
    }
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        let conn = &self.connection;
        let pool = &self.pool;
        let sec = &self.security;

        // -- Connection checks --
        if conn.socket.is_none() && conn.host.is_empty() {
            anyhow::bail!("connection.host must not be empty (unless using socket)");
        }
        // Note: when both connection_string and socket are set, connection_string takes precedence.

        // -- SSL checks --
        if sec.ssl_ca.is_some() && !sec.ssl {
            warn!("MYSQL_SSL_CA is set but MYSQL_SSL is false; CA cert will be ignored");
        }
        if sec.ssl_accept_invalid_certs {
            warn!("ssl_accept_invalid_certs is enabled — TLS validation disabled");
        }

        // -- Pool bound checks --
        if pool.connect_timeout_ms == 0 {
            anyhow::bail!("pool.connect_timeout_ms must be > 0");
        }
        if pool.size == 0 || pool.size > 1000 {
            anyhow::bail!("pool.size must be between 1 and 1000 (got: {})", pool.size);
        }
        if pool.max_rows == 0 {
            anyhow::bail!("pool.max_rows must be >= 1");
        }
        if pool.max_rows > 1_000_000 {
            anyhow::bail!(
                "pool.max_rows must be <= 1,000,000 (got: {})",
                pool.max_rows
            );
        }
        if pool.max_result_memory_mb == 0 || pool.max_result_memory_mb > 16384 {
            anyhow::bail!(
                "pool.max_result_memory_mb must be between 1 and 16384 (got: {})",
                pool.max_result_memory_mb
            );
        }
        if pool.retry_attempts > 10 {
            anyhow::bail!(
                "pool.retry_attempts must be between 0 and 10 (got: {})",
                pool.retry_attempts
            );
        }
        if pool.query_timeout_ms == 0 {
            warn!(
                "pool.query_timeout_ms is 0 — query timeouts are disabled; a runaway query can block the server indefinitely"
            );
        }
        if pool.slow_query_threshold_ms > 3_600_000 {
            warn!(
                "pool.slow_query_threshold_ms is very high ({}ms > 3,600,000ms / 1 hour); slow query logging may not trigger for most queries",
                pool.slow_query_threshold_ms
            );
        }
        const MAX_CACHE_TTL_SECS: u64 = 31_536_000; // 1 year
        if pool.cache_ttl_secs > MAX_CACHE_TTL_SECS {
            anyhow::bail!(
                "pool.cache_ttl_secs must be <= {} (1 year) (got: {})",
                MAX_CACHE_TTL_SECS,
                pool.cache_ttl_secs
            );
        }
        if !matches!(pool.performance_hints.as_str(), "none" | "auto" | "always") {
            anyhow::bail!(
                "MYSQL_PERFORMANCE_HINTS must be one of: none, auto, always (got: '{}')",
                pool.performance_hints
            );
        }

        // -- Security bound checks --
        if sec.max_sessions == 0 {
            anyhow::bail!("security.max_sessions must be >= 1");
        }
        if sec.max_total_connections < pool.size {
            anyhow::bail!(
                "security.max_total_connections ({}) must be >= pool.size ({})",
                sec.max_total_connections,
                pool.size
            );
        }
        // Warn if pool.size leaves no room for named sessions (each uses 5 connections)
        const NAMED_SESSION_POOL_SIZE: u32 = 5;
        if pool.size + NAMED_SESSION_POOL_SIZE > sec.max_total_connections {
            warn!(
                "pool.size ({}) leaves no room for named sessions: each named session requires {} connections. \
                 Consider increasing max_total_connections (currently {}) or reducing pool.size to at least {}",
                pool.size,
                NAMED_SESSION_POOL_SIZE,
                sec.max_total_connections,
                sec.max_total_connections.saturating_sub(NAMED_SESSION_POOL_SIZE)
            );
        }

        // -- File existence checks --
        if let Some(ref ca) = sec.ssl_ca {
            if let Err(e) = std::fs::File::open(ca) {
                anyhow::bail!("MYSQL_SSL_CA path is not readable: {} ({})", ca, e);
            }
        }

        // -- SSH validation --
        if let Some(ref ssh) = self.ssh {
            if ssh.host.is_empty() {
                anyhow::bail!("ssh.host must not be empty when SSH tunnel is configured");
            }
            if ssh.port == 0 {
                anyhow::bail!(
                    "ssh.port must be between 1 and 65535 (got: 0)"
                );
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
                check_private_key_permissions(key_path)?;
            }
            if let Some(ref khf) = ssh.known_hosts_file {
                let khf_path = std::path::Path::new(khf);
                if ssh.known_hosts_check == "strict" {
                    if !khf_path.exists() {
                        anyhow::bail!(
                            "ssh.known_hosts_file does not exist: {} (required for strict mode)",
                            khf
                        );
                    }
                } else if let Some(parent) = khf_path.parent() {
                    if !parent.exists() {
                        anyhow::bail!(
                            "ssh.known_hosts_file parent directory does not exist: {}",
                            parent.display()
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

/// Check that an SSH private key file has restrictive permissions (mode 0o600 or 0o400).
/// On Unix systems, this verifies the file is not world-readable or writable by group/others.
pub(crate) fn check_private_key_permissions(path: &str) -> anyhow::Result<()> {
    let metadata = std::fs::metadata(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        // Allow 0o600 (owner read/write) or 0o400 (owner read-only)
        // and 0o300/0o500 (owner write/owner read) in case user is root
        if mode & 0o77 != 0 {
            anyhow::bail!(
                "ssh.private_key {} has overly permissive permissions: {:o}. \
                 SSH private keys must not be readable by group or others. \
                 Run: chmod 600 {}",
                path,
                mode,
                path
            );
        }
    }

    #[cfg(not(unix))]
    {
        // On non-Unix systems, just check that the file is readable
        // and issue a warning since permissions cannot be validated
        tracing::warn!(
            "ssh.private_key {} permissions cannot be validated on this platform",
            path
        );
    }

    Ok(())
}

/// Load config from a TOML file path. Returns default config if file doesn't exist.
pub(crate) fn load_toml_config(path: &std::path::Path) -> anyhow::Result<Config> {
    if !path.exists() {
        return Ok(Config::default());
    }
    let content = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&content)?)
}

/// Load the final merged config: dotenv -> TOML base -> env var overrides.
pub fn load_config() -> anyhow::Result<Config> {
    if std::path::Path::new(".env").exists() {
        if let Err(e) = dotenv::dotenv() {
            warn!("failed to parse .env file: {}", e);
        }
    }
    let path = std::env::var("MCP_CONFIG_FILE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("mysql-mcp.toml"));
    let base = load_toml_config(&path)?;
    Ok(env_config::load_env_config().apply_to(base))
}
