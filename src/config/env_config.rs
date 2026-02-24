use crate::config::{Config, SchemaPermissions};
use std::collections::HashMap;

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
        database: std::env::var("MYSQL_DB").ok().filter(|s| !s.is_empty()),
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
        schema_permissions: parse_schema_permissions(),
        performance_hints: std::env::var("MYSQL_PERFORMANCE_HINTS").ok(),
        slow_query_threshold_ms: parse_env_num::<u64>("MYSQL_SLOW_QUERY_THRESHOLD_MS"),
        warmup_connections: parse_env_num::<u32>("MYSQL_POOL_WARMUP"),
        max_rows: parse_env_num::<u32>("MYSQL_MAX_ROWS"),
        max_sessions: parse_env_num::<u32>("MYSQL_MAX_SESSIONS"),
        ssh_host: std::env::var("MYSQL_SSH_HOST")
            .ok()
            .filter(|s| !s.is_empty()),
        ssh_port: parse_env_num::<u16>("MYSQL_SSH_PORT"),
        ssh_user: std::env::var("MYSQL_SSH_USER")
            .ok()
            .filter(|s| !s.is_empty()),
        ssh_private_key: std::env::var("MYSQL_SSH_PRIVATE_KEY")
            .ok()
            .filter(|s| !s.is_empty()),
        ssh_known_hosts_check: std::env::var("MYSQL_SSH_KNOWN_HOSTS_CHECK")
            .ok()
            .filter(|s| !s.is_empty()),
        ssh_known_hosts_file: std::env::var("MYSQL_SSH_KNOWN_HOSTS_FILE")
            .ok()
            .filter(|s| !s.is_empty()),
    }
}

/// Parse MYSQL_SCHEMA_<NAME>_PERMISSIONS env vars.
/// Format: MYSQL_SCHEMA_mydb_PERMISSIONS=insert,update (comma-separated allowed ops)
fn parse_schema_permissions() -> HashMap<String, SchemaPermissions> {
    let mut map = HashMap::new();
    for (key, val) in std::env::vars() {
        if let Some(schema_name) = key
            .strip_prefix("MYSQL_SCHEMA_")
            .and_then(|s| s.strip_suffix("_PERMISSIONS"))
        {
            let schema_name = schema_name.to_lowercase();
            if schema_name.is_empty() {
                eprintln!("Warning: {key} has an empty schema name (double underscore?); expected MYSQL_SCHEMA_<name>_PERMISSIONS — skipping");
                continue;
            }
            if schema_name.len() > 64 {
                eprintln!("Warning: {key} schema name is too long (max 64 characters) — skipping");
                continue;
            }
            if !schema_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                eprintln!(
                    "Warning: {key} schema name contains invalid characters \
                     (only alphanumeric and underscores allowed) — skipping"
                );
                continue;
            }
            let ops: Vec<&str> = val
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            for op in &ops {
                if !op.is_empty() && !matches!(*op, "insert" | "update" | "delete" | "ddl") {
                    eprintln!(
                        "Warning: unrecognized permission '{}' in {}; valid values: insert, update, delete, ddl",
                        op, key
                    );
                }
            }
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
    pub schema_permissions: HashMap<String, SchemaPermissions>,
    pub performance_hints: Option<String>,
    pub slow_query_threshold_ms: Option<u64>,
    pub warmup_connections: Option<u32>,
    pub max_rows: Option<u32>,
    pub max_sessions: Option<u32>,
    pub ssh_host: Option<String>,
    pub ssh_port: Option<u16>,
    pub ssh_user: Option<String>,
    pub ssh_private_key: Option<String>,
    pub ssh_known_hosts_check: Option<String>,
    pub ssh_known_hosts_file: Option<String>,
}

impl EnvConfig {
    /// Apply env var overrides onto a base Config, returning the merged result.
    pub fn apply_to(self, mut base: Config) -> Config {
        if let Some(v) = self.host {
            base.connection.host = v;
        }
        if let Some(v) = self.port {
            base.connection.port = v;
        }
        if let Some(v) = self.socket {
            base.connection.socket = Some(v);
        }
        if let Some(v) = self.user {
            base.connection.user = v;
        }
        if let Some(v) = self.password {
            base.connection.password = v;
        }
        if let Some(v) = self.database {
            base.connection.database = Some(v);
        }
        if let Some(v) = self.connection_string {
            base.connection.connection_string = Some(v);
        }
        if let Some(v) = self.pool_size {
            base.pool.size = v;
        }
        if let Some(v) = self.query_timeout_ms {
            base.pool.query_timeout_ms = v;
        }
        if let Some(v) = self.connect_timeout_ms {
            base.pool.connect_timeout_ms = v;
        }
        if let Some(v) = self.cache_ttl_secs {
            base.pool.cache_ttl_secs = v;
        }
        if let Some(v) = self.allow_insert {
            base.security.allow_insert = v;
        }
        if let Some(v) = self.allow_update {
            base.security.allow_update = v;
        }
        if let Some(v) = self.allow_delete {
            base.security.allow_delete = v;
        }
        if let Some(v) = self.allow_ddl {
            base.security.allow_ddl = v;
        }
        if let Some(v) = self.readonly_transaction {
            base.pool.readonly_transaction = v;
        }
        if let Some(v) = self.ssl {
            base.security.ssl = v;
        }
        if let Some(v) = self.ssl_accept_invalid_certs {
            base.security.ssl_accept_invalid_certs = v;
        }
        if let Some(v) = self.ssl_ca {
            base.security.ssl_ca = Some(v);
        }
        if let Some(v) = self.multi_db_write_mode {
            base.security.multi_db_write_mode = v;
        }
        if let Some(v) = self.allow_runtime_connections {
            base.security.allow_runtime_connections = v;
        }
        if !self.schema_permissions.is_empty() {
            // Env vars are merged on top of the TOML base using extend(): env entries
            // for a given schema name overwrite any TOML entry with the same name,
            // while TOML entries for schemas not present in env vars are preserved.
            base.security
                .schema_permissions
                .extend(self.schema_permissions);
        }
        if let Some(v) = self.performance_hints {
            base.pool.performance_hints = v;
        }
        if let Some(v) = self.slow_query_threshold_ms {
            base.pool.slow_query_threshold_ms = v;
        }
        if let Some(v) = self.warmup_connections {
            base.pool.warmup_connections = v;
        }
        if let Some(v) = self.max_rows {
            base.pool.max_rows = v;
        }
        if let Some(v) = self.max_sessions {
            base.security.max_sessions = v;
        }
        // SSH tunnel config: if any MYSQL_SSH_* env var is set, build/update the SshConfig
        let any_ssh = self.ssh_host.is_some()
            || self.ssh_user.is_some()
            || self.ssh_port.is_some()
            || self.ssh_private_key.is_some()
            || self.ssh_known_hosts_check.is_some()
            || self.ssh_known_hosts_file.is_some();
        if any_ssh {
            let mut ssh = base.ssh.take().unwrap_or_default();
            if let Some(v) = self.ssh_host {
                ssh.host = v;
            }
            if let Some(v) = self.ssh_port {
                ssh.port = v;
            }
            if let Some(v) = self.ssh_user {
                ssh.user = v;
            }
            if let Some(v) = self.ssh_private_key {
                ssh.private_key = Some(v);
            }
            if let Some(v) = self.ssh_known_hosts_check {
                ssh.known_hosts_check = v;
            }
            if let Some(v) = self.ssh_known_hosts_file {
                ssh.known_hosts_file = Some(v);
            }
            base.ssh = Some(ssh);
        }
        base
    }
}
