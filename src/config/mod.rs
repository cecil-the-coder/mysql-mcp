use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::warn;

pub mod env_config;
#[cfg(test)]
mod tests;

/// Top-level configuration for the MySQL MCP server.
///
/// This struct holds all configuration sections and is typically loaded from
/// a TOML file (e.g., `mysql-mcp.toml`) combined with environment variable overrides.
/// Configuration is loaded in this order: `.env` → TOML file → environment variables.
///
/// # Example TOML structure
/// ```toml
/// [connection]
/// host = "localhost"
/// port = 3306
/// user = "root"
///
/// [pool]
/// size = 20
///
/// [security]
/// allow_insert = false
///
/// [ssh]  # Optional SSH tunnel
/// host = "bastion.example.com"
/// user = "ec2-user"
/// ```
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// MySQL database connection settings (host, port, credentials, etc.).
    pub connection: ConnectionConfig,
    /// Connection pool tuning (size, timeouts, result limits).
    pub pool: PoolConfig,
    /// Security policy settings (allowed operations, SSL, per-schema permissions).
    pub security: SecurityConfig,
    /// SSH tunnel configuration for connecting through a jump host.
    /// When set, the MySQL connection is tunneled through SSH.
    pub ssh: Option<SshConfig>,
}

/// MySQL database connection settings.
///
/// Configures how the MCP server connects to the MySQL database.
/// You can specify individual fields (host, port, user, etc.) or use a
/// complete connection string that overrides the individual fields.
///
/// # Connection priority
/// 1. If `connection_string` is set, it takes precedence over all other fields
/// 2. If `socket` is set (and no connection_string), Unix socket is used
/// 3. Otherwise, TCP connection to `host:port` is established
///
/// # Security note
/// The `password` field is redacted in Debug output to prevent credential leakage in logs.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConnectionConfig {
    /// MySQL server hostname. Default: "localhost". Ignored if socket or connection_string is set.
    pub host: String,
    /// MySQL server port. Default: 3306.
    pub port: u16,
    /// Unix socket path for local connections. Takes precedence over host/port when set.
    pub socket: Option<String>,
    /// MySQL username for authentication. Default: "root".
    pub user: String,
    /// MySQL password for authentication. Default: empty string.
    pub password: String,
    /// Default database/schema to use. Optional; queries without explicit schema use this.
    pub database: Option<String>,
    /// Full MySQL connection string (e.g., "mysql://user:pass@host:3306/db").
    /// When set, this overrides all other connection fields.
    pub connection_string: Option<String>,
}

/// Connection pool and query execution tuning parameters.
///
/// Controls connection pooling behavior, query timeouts, result size limits,
/// and performance-related settings. These settings apply to the default
/// connection pool; named sessions created at runtime have their own pools
/// but inherit these settings as defaults.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool. Default: 20. Valid range: 1-1000.
    pub size: u32,
    /// Query execution timeout in milliseconds. Default: 30000 (30 seconds).
    pub query_timeout_ms: u64,
    /// Connection establishment timeout in milliseconds. Default: 10000 (10 seconds).
    pub connect_timeout_ms: u64,
    /// Schema metadata cache time-to-live in seconds. Default: 60.
    /// Controls how long table/column info is cached before refreshing.
    pub cache_ttl_secs: u64,
    /// Performance hints strategy: "none", "auto", or "always". Default: "none".
    /// When enabled, the server may use MySQL's optimizer hints automatically.
    pub performance_hints: String,
    /// Threshold in milliseconds for logging slow queries. Default: 500.
    /// Queries exceeding this duration are logged at WARN level.
    pub slow_query_threshold_ms: u64,
    /// Maximum number of rows returned per query. Default: 1000. Max: 1,000,000.
    /// Results exceeding this are truncated with a warning.
    pub max_rows: u32,
    /// Number of retry attempts for transient network errors. Default: 2.
    /// Retries use exponential backoff (100ms base, doubling) between attempts.
    /// Valid range: 0-10.
    pub retry_attempts: u32,
    /// Maximum memory in MB for result sets. Default: 256. Max: 16384.
    /// When exceeded, results are truncated with a warning in parse_warnings.
    pub max_result_memory_mb: u32,
}

/// Security policy configuration for controlling allowed database operations.
///
/// Defines which write operations (INSERT, UPDATE, DELETE, DDL) are permitted,
/// SSL/TLS settings, and per-schema permission overrides. By default, all
/// write operations are disabled for safety.
///
/// # Permission override logic
/// The `schema_permissions` map allows fine-grained control per schema.
/// When checking permissions for an operation on a specific schema:
/// 1. If the schema has an entry in `schema_permissions` and the relevant
///    field (e.g., `allow_insert`) is `Some(value)`, that value is used
/// 2. Otherwise, the global setting (e.g., `allow_insert`) is used
///
/// This allows patterns like "globally disable INSERT, but allow for the
/// `staging` schema" by setting `allow_insert = false` globally and
/// `schema_permissions.staging.allow_insert = Some(true)`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SecurityConfig {
    /// Allow INSERT statements globally. Default: false.
    /// Can be overridden per-schema via schema_permissions.
    pub allow_insert: bool,
    /// Allow UPDATE statements globally. Default: false.
    /// Can be overridden per-schema via schema_permissions.
    pub allow_update: bool,
    /// Allow DELETE statements globally. Default: false.
    /// Can be overridden per-schema via schema_permissions.
    pub allow_delete: bool,
    /// Allow DDL statements (CREATE, ALTER, DROP, etc.) globally. Default: false.
    /// Can be overridden per-schema via schema_permissions.
    pub allow_ddl: bool,
    /// Enable SSL/TLS for database connections. Default: false.
    pub ssl: bool,
    /// Accept invalid SSL certificates (e.g., self-signed). Default: false.
    /// WARNING: This disables certificate validation and exposes connections to MITM attacks.
    pub ssl_accept_invalid_certs: bool,
    /// Path to a PEM CA bundle file for SSL verification.
    /// When set with ssl=true, this CA is used instead of the system trust store.
    pub ssl_ca: Option<String>,
    /// Per-schema permission overrides. Maps schema name to permission settings.
    /// Fields set to `Some(value)` override the global security settings for that schema.
    /// See struct-level documentation for override logic details.
    pub schema_permissions: HashMap<String, SchemaPermissions>,
    /// Allow mysql_connect tool to accept raw credentials at runtime.
    /// When false (default), only connections using pre-configured presets are allowed.
    /// Enable with caution as this allows LLMs to specify arbitrary database credentials.
    pub allow_runtime_connections: bool,
    /// Maximum number of concurrent named sessions (excluding the default session).
    /// Default: 50. Prevents unbounded session creation when allow_runtime_connections is true.
    pub max_sessions: u32,
    /// Maximum total database connections across all sessions.
    /// Default: 100. Named sessions use 5 connections each (from this pool).
    /// Must be >= pool.size. Prevents database connection exhaustion.
    pub max_total_connections: u32,
}

/// SSH tunnel configuration for connecting through a jump host.
///
/// When configured, the MySQL connection is established through an SSH tunnel
/// to the specified host. This is useful for connecting to databases that are
/// only accessible via a bastion host or in private networks.
///
/// The tunnel is established before the MySQL connection, and all database
/// traffic flows through the encrypted SSH channel.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SshConfig {
    /// SSH bastion host hostname or IP address.
    pub host: String,
    /// SSH server port. Default: 22.
    pub port: u16,
    /// SSH username for authentication.
    pub user: String,
    /// Path to the SSH private key file for authentication.
    /// If not set, the default SSH agent or default keys (~/.ssh/id_rsa, etc.) are used.
    pub private_key: Option<String>,
    /// Host key verification strategy. One of: "strict", "accept-new", "insecure".
    /// - "strict": Only accept hosts already in known_hosts (fails for new hosts)
    /// - "accept-new": Accept and save new host keys, reject changed keys
    /// - "insecure": Accept any host key (WARNING: vulnerable to MITM attacks)
    pub known_hosts_check: String,
    /// Path to the known_hosts file for host key verification.
    /// If not set, uses the default OpenSSH known_hosts location (~/.ssh/known_hosts).
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

/// Per-schema permission overrides for SecurityConfig.
///
/// Allows enabling or disabling specific operations for individual database
/// schemas, overriding the global security settings. All fields are optional;
/// when a field is `None`, the corresponding global setting from `SecurityConfig`
/// is used for that schema.
///
/// # Example
/// To allow INSERT only on the `logs` schema while keeping global INSERT disabled:
/// ```toml
/// [security]
/// allow_insert = false
///
/// [security.schema_permissions.logs]
/// allow_insert = true
/// ```
///
/// To disable DDL on the `production` schema while allowing it elsewhere:
/// ```toml
/// [security]
/// allow_ddl = true
///
/// [security.schema_permissions.production]
/// allow_ddl = false
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SchemaPermissions {
    /// Override for allow_insert. `None` uses the global setting.
    pub allow_insert: Option<bool>,
    /// Override for allow_update. `None` uses the global setting.
    pub allow_update: Option<bool>,
    /// Override for allow_delete. `None` uses the global setting.
    pub allow_delete: Option<bool>,
    /// Override for allow_ddl. `None` uses the global setting.
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
            if !std::path::Path::new(ca).exists() {
                anyhow::bail!("MYSQL_SSL_CA path does not exist: {}", ca);
            }
        }

        // -- SSH validation --
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
