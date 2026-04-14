//! Configuration management for the MySQL MCP server.
//!
//! # Configuration Architecture
//!
//! The configuration system is designed to be flexible and secure, supporting both
//! TOML configuration files and environment variables. The configuration is loaded
//! in a specific order with environment variables taking precedence over file settings.
//!
//! ## Configuration Sources and Precedence
//!
//! Configuration is loaded through the following pipeline, where each subsequent
//! source overrides values from the previous:
//!
//! 1. **Default values** - Built-in defaults for all configuration structures
//! 2. **TOML file** - Loaded from `MCP_CONFIG_FILE` environment variable (defaults to `mysql-mcp.toml`)
//!    - If the file doesn't exist, defaults are used without error
//! 3. **Environment variables** - All environment variables that correspond to
//!    configuration fields are applied as overrides
//!
//! This precedence order ensures that:
//! - Default values provide safe, working defaults out of the box
//! - TOML files allow for complex, multi-value configurations (like SSH keys, schema permissions)
//! - Environment variables enable runtime customization and secret injection (e.g., passwords)
//!
//! ## TOML File Format
//!
//! The primary configuration is specified in a TOML file. Key sections include:
//!
//! - `[connection]` - Database connection parameters (host, port, user, password, socket)
//! - `[pool]` - Connection pool settings (size, timeouts, retry behavior)
//! - `[security]` - Security settings (permissions, SSL, runtime connection controls)
//! - `[ssh]` - Optional SSH tunnel configuration for bastion hosts
//!
//! Environment variables can override any TOML value by using the same field name
//! with an `MCP_` prefix (e.g., `MCP_POOL_SIZE` overrides `pool.size`).
//!
//! ## Environment Variable Integration
//!
//! Environment variables provide runtime configuration flexibility and are essential
//! for injecting sensitive information (passwords, keys) without storing them in files.
//! All configuration fields can be set via environment variables using the `MCP_` prefix.
//! This supports:
//!
//! - **Simple values**: `MCP_POOL_SIZE=50`
//! - **Boolean flags**: `MCP_SECURITY_SSL=true`
//! - **Connection strings**: `MCP_CONNECTION_CONNECTION_STRING=...`
//!
//! When both a TOML file and environment variables are present, environment variables
//! take precedence, allowing deployment-specific overrides without modifying configuration files.
//!
//! ## Key Design Decisions
//!
//! ### Security by Default
//!
//! The configuration follows a security-first approach:
//!
//! - All write permissions (`allow_insert`, `allow_update`, etc.) are **disabled by default**
//! - Runtime connections are **disabled by default** (`allow_runtime_connections: false`)
//! - SSL is **disabled by default** (must be explicitly enabled)
//! - Session creation is strictly limited (`max_sessions: 50`)
//!
//! ### Connection String Precedence
//!
//! When `connection_string` is set in the connection configuration, it takes precedence
//! over individual field values (host, port, user, etc.). This allows complex connection
//! strings with parameters that might be difficult to express individually.
//!
//! ### Validation and Safety
//!
//! The `Config::validate()` method performs comprehensive validation including:
//!
//! - Range checks on numeric values (pool sizes, timeouts, retry counts)
//! - Security restriction validation (e.g., preventing insecure SSH + runtime connections)
//! - File existence checks for SSL certificates and SSH keys
//! - Permission validation on Unix systems for sensitive files
//!
//! Warnings are issued for potentially problematic but non-fatal configurations,
//! while errors result in `anyhow::Result` returns to prevent unsafe operation.
//!
//! ### SSH Tunnel Security
//!
//! SSH tunneling requires careful configuration:
//!
//! - In strict mode (default), known hosts must be verified
//! - Private keys must have restrictive permissions (0o600 or 0o400)
//! - **Critical**: Runtime connections cannot be used with insecure SSH host key checking
//!   as this would allow MITM attacks on database credentials
//!
//! # Example
//!
//! ```ignore
//! use config::load_config;
//! let config = load_config()?;
//! config.validate()?;
//! # Ok::<(), anyhow::Error>(())
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::backend::BackendKind;

pub mod env_config;
#[cfg(test)]
mod tests;

/// Top-level configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub backend: BackendKind,
    pub connection: ConnectionConfig,
    pub pool: PoolConfig,
    pub security: SecurityConfig,
    pub ssh: Option<SshConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            backend: BackendKind::MySql,
            connection: ConnectionConfig::default(),
            pool: PoolConfig::default(),
            security: SecurityConfig::default(),
            ssh: None,
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: Option<u16>,
    pub socket: Option<String>,
    pub user: String,
    pub password: String,
    pub database: Option<String>,
    /// Full connection string (overrides individual fields when set)
    pub connection_string: Option<String>,
    /// SQLite file path (for SQLite backend only)
    pub path: Option<String>,
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
    /// Allow connect to accept raw credentials at runtime.
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
            port: None,
            socket: None,
            user: String::new(),
            password: String::new(),
            database: None,
            connection_string: None,
            path: None,
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
            .field("path", &self.path)
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
        let is_sqlite = self.backend == BackendKind::Sqlite;

        if !is_sqlite {
            // -- Connection checks (network backends only) --
            if conn.socket.is_none() && conn.host.is_empty() {
                anyhow::bail!("connection.host must not be empty (unless using socket)");
            }
            // Note: when both connection_string and socket are set, connection_string takes precedence.

            // -- Socket validation (MySQL only) --
            if self.backend == BackendKind::MySql {
                if let Some(ref socket_path) = conn.socket {
                    if !std::path::Path::new(socket_path).exists() {
                        anyhow::bail!("connection.socket path does not exist: {}", socket_path);
                    }
                }
            }
        } else {
            // -- SQLite path validation --
            if let Some(ref db_path) = conn.path {
                // Validate parent directory exists
                if let Some(parent) = std::path::Path::new(db_path).parent() {
                    if !parent.as_os_str().is_empty() && !parent.exists() {
                        anyhow::bail!(
                            "connection.path parent directory does not exist: {}",
                            parent.display()
                        );
                    }
                }
            } else if conn.connection_string.is_none() {
                anyhow::bail!(
                    "connection.path or connection.connection_string must be set for SQLite backend"
                );
            }
        }

        // -- Connection string scheme validation --
        if let Some(ref cs) = conn.connection_string {
            let scheme = cs.split("://").next().unwrap_or("");
            let expected_scheme = match self.backend {
                BackendKind::MySql => "mysql",
                BackendKind::Postgres => "postgres",
                BackendKind::Sqlite => "sqlite",
            };
            if scheme != expected_scheme {
                anyhow::bail!(
                    "connection.connection_string scheme is '{}' but backend is {:?} (expected '{}://')",
                    scheme,
                    self.backend,
                    expected_scheme
                );
            }
        }

        // -- SSL checks (skip for backends that don't support SSL) --
        if !is_sqlite || sec.ssl || sec.ssl_ca.is_some() || sec.ssl_accept_invalid_certs {
            if sec.ssl_ca.is_some() && !sec.ssl {
                eprintln!("Warning: DB_SSL_CA is set but DB_SSL is false; CA cert will be ignored");
            }
            if sec.ssl_accept_invalid_certs {
                eprintln!("Warning: ssl_accept_invalid_certs is enabled — TLS validation disabled");
            }
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
        if pool.max_result_memory_mb == 0 {
            anyhow::bail!("pool.max_result_memory_mb must be >= 1");
        }
        if pool.retry_attempts > 10 {
            anyhow::bail!(
                "pool.retry_attempts must be between 0 and 10 (got: {})",
                pool.retry_attempts
            );
        }
        if !matches!(pool.performance_hints.as_str(), "none" | "auto" | "always") {
            anyhow::bail!(
                "DB_PERFORMANCE_HINTS must be one of: none, auto, always (got: '{}')",
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

        // -- File existence checks (skip SSL for SQLite) --
        if !is_sqlite {
            if let Some(ref ca) = sec.ssl_ca {
                if !std::path::Path::new(ca).exists() {
                    anyhow::bail!("DB_SSL_CA path does not exist: {}", ca);
                }
            }
        }

        // -- SSH validation (skip for SQLite) --
        if !is_sqlite {
            if let Some(ref ssh) = self.ssh {
                if ssh.host.is_empty() {
                    anyhow::bail!("ssh.host must not be empty when SSH tunnel is configured");
                }
                if ssh.port == 0 {
                    anyhow::bail!("ssh.port must be > 0 (got: 0)");
                }
                if ssh.user.is_empty() {
                    anyhow::bail!("ssh.user must not be empty when SSH tunnel is configured");
                }
                // Validate enum values first to ensure downstream checks work correctly
                if !matches!(
                    ssh.known_hosts_check.as_str(),
                    "strict" | "accept-new" | "insecure"
                ) {
                    anyhow::bail!(
                        "ssh.known_hosts_check must be one of: strict, accept-new, insecure (got: '{}')",
                        ssh.known_hosts_check
                    );
                }
                // Block dangerous combination: runtime connections + insecure SSH host key checking.
                // Without host key verification, an attacker can MITM the SSH tunnel and
                // intercept database credentials supplied at runtime.
                if sec.allow_runtime_connections && ssh.known_hosts_check == "insecure" {
                    anyhow::bail!(
                        "security.allow_runtime_connections cannot be enabled when \
                         ssh.known_hosts_check is \"insecure\". This combination allows \
                         arbitrary SSH tunnels without host key verification, enabling \
                         man-in-the-middle attacks that could intercept database credentials. \
                         Use \"strict\" or \"accept-new\" host key checking, or disable \
                         allow_runtime_connections."
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
                        check_known_hosts_permissions(khf)?;
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
        }

        Ok(())
    }
}

/// Check that an SSH known_hosts file has safe permissions for strict mode.
/// On Unix systems, this verifies the file is not writable by group or others.
/// Unlike private keys, the known_hosts file may be world-readable (e.g., 0o644 is acceptable).
pub(crate) fn check_known_hosts_permissions(path: &str) -> anyhow::Result<()> {
    let metadata = std::fs::metadata(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        // In strict mode, the known_hosts file must not be writable by group or others
        // to prevent tampering with host key mappings.
        if mode & 0o022 != 0 {
            anyhow::bail!(
                "ssh.known_hosts_file {} has overly permissive permissions: {:o}. \
                 In strict mode, the known_hosts file must not be writable by group or others. \
                 Run: chmod 644 {}",
                path,
                mode,
                path
            );
        }
    }

    #[cfg(not(unix))]
    {
        let _ = &metadata;
        tracing::warn!(
            "ssh.known_hosts_file {} permissions cannot be validated on this platform",
            path
        );
    }

    Ok(())
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