//! Environment variable configuration for the MySQL MCP server.
//!
//! This module provides environment variable parsing for all `MYSQL_*` configuration
//! options, allowing configuration to be supplied via environment variables instead of
//! or in addition to TOML config files.
//!
//! The [`EnvConfig`] struct holds optional overrides parsed from environment variables.
//! These overrides are merged onto a TOML base config via the [`EnvConfig::apply_to`]
//! method, with environment variables taking precedence over TOML values.
//!
//! # Per-Schema Permissions
//!
//! This module also handles parsing of `MYSQL_SCHEMA_<NAME>_PERMISSIONS` environment
//! variables for per-schema permission overrides. For example:
//!
//! ```text
//! MYSQL_SCHEMA_mydb_PERMISSIONS=insert,update
//! ```
//!
//! This allows write operations (insert and update) specifically for the `mydb` schema,
//! overriding the global permission settings.

use crate::config::{Config, SchemaPermissions};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helpers — use eprintln! because this runs before the logging system starts.
// ---------------------------------------------------------------------------

/// Common MySQL reserved keywords that should not be used as unquoted schema names.
/// Source: https://dev.mysql.com/doc/refman/8.0/en/keywords.html
const MYSQL_RESERVED_KEYWORDS: &[&str] = &[
    "ACCESSIBLE",
    "ADD",
    "ALL",
    "ALTER",
    "ANALYZE",
    "AND",
    "AS",
    "ASC",
    "ASENSITIVE",
    "BEFORE",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BLOB",
    "BOTH",
    "BY",
    "CALL",
    "CASCADE",
    "CASE",
    "CHANGE",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "CONDITION",
    "CONSTRAINT",
    "CONTINUE",
    "CONVERT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CUME_DIST",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURSOR",
    "DATABASE",
    "DATABASES",
    "DAY_HOUR",
    "DAY_MICROSECOND",
    "DAY_MINUTE",
    "DAY_SECOND",
    "DEC",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DELAYED",
    "DELETE",
    "DENSE_RANK",
    "DESC",
    "DESCRIBE",
    "DETERMINISTIC",
    "DISTINCT",
    "DISTINCTROW",
    "DIV",
    "DOUBLE",
    "DROP",
    "DUAL",
    "EACH",
    "ELSE",
    "ELSEIF",
    "EMPTY",
    "ENCLOSED",
    "ESCAPED",
    "EXCEPT",
    "EXISTS",
    "EXIT",
    "EXPLAIN",
    "FALSE",
    "FETCH",
    "FIRST_VALUE",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "FOR",
    "FORCE",
    "FOREIGN",
    "FROM",
    "FULLTEXT",
    "FUNCTION",
    "GENERATED",
    "GET",
    "GRANT",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HAVING",
    "HIGH_PRIORITY",
    "HOUR_MICROSECOND",
    "HOUR_MINUTE",
    "HOUR_SECOND",
    "IF",
    "IGNORE",
    "IN",
    "INDEX",
    "INFILE",
    "INNER",
    "INOUT",
    "INSENSITIVE",
    "INSERT",
    "INT",
    "INT1",
    "INT2",
    "INT3",
    "INT4",
    "INT8",
    "INTEGER",
    "INTERVAL",
    "INTO",
    "IO_AFTER_GTIDS",
    "IO_BEFORE_GTIDS",
    "IS",
    "ITERATE",
    "JOIN",
    "JSON_TABLE",
    "KEY",
    "KEYS",
    "KILL",
    "LAG",
    "LAST_VALUE",
    "LATERAL",
    "LEAD",
    "LEADING",
    "LEAVE",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LINEAR",
    "LINES",
    "LOAD",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCK",
    "LONG",
    "LONGBLOB",
    "LONGTEXT",
    "LOOP",
    "LOW_PRIORITY",
    "MASTER_BIND",
    "MASTER_SSL_VERIFY_SERVER_CERT",
    "MATCH",
    "MAXVALUE",
    "MEDIUMBLOB",
    "MEDIUMINT",
    "MEDIUMTEXT",
    "MIDDLEINT",
    "MINUTE_MICROSECOND",
    "MINUTE_SECOND",
    "MOD",
    "MODIFIES",
    "NATURAL",
    "NOT",
    "NO_WRITE_TO_BINLOG",
    "NTH_VALUE",
    "NTILE",
    "NULL",
    "NUMERIC",
    "OF",
    "ON",
    "OPTIMIZE",
    "OPTIMIZER_COSTS",
    "OPTION",
    "OPTIONALLY",
    "OR",
    "ORDER",
    "OUT",
    "OUTER",
    "OUTFILE",
    "OVER",
    "PARTITION",
    "PERCENT_RANK",
    "PRECISION",
    "PRIMARY",
    "PROCEDURE",
    "PURGE",
    "RANGE",
    "RANK",
    "READ",
    "READS",
    "READ_WRITE",
    "REAL",
    "RECURSIVE",
    "REFERENCES",
    "REGEXP",
    "RELEASE",
    "RENAME",
    "REPEAT",
    "REPLACE",
    "REQUIRE",
    "RESIGNAL",
    "RESTRICT",
    "RETURN",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "ROW",
    "ROWS",
    "ROW_NUMBER",
    "SCHEMA",
    "SCHEMAS",
    "SECOND_MICROSECOND",
    "SELECT",
    "SENSITIVE",
    "SEPARATOR",
    "SET",
    "SHOW",
    "SIGNAL",
    "SMALLINT",
    "SPATIAL",
    "SPECIFIC",
    "SQL",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "SQL_BIG_RESULT",
    "SQL_CALC_FOUND_ROWS",
    "SQL_SMALL_RESULT",
    "SSL",
    "STARTING",
    "STORED",
    "STRAIGHT_JOIN",
    "SYSTEM",
    "TABLE",
    "TERMINATED",
    "THEN",
    "TINYBLOB",
    "TINYINT",
    "TINYTEXT",
    "TO",
    "TRAILING",
    "TRIGGER",
    "TRUE",
    "UNDO",
    "UNION",
    "UNIQUE",
    "UNLOCK",
    "UNSIGNED",
    "UPDATE",
    "USAGE",
    "USE",
    "USING",
    "UTC_DATE",
    "UTC_TIME",
    "UTC_TIMESTAMP",
    "VALUES",
    "VARBINARY",
    "VARCHAR",
    "VARCHARACTER",
    "VARYING",
    "VIRTUAL",
    "WHEN",
    "WHERE",
    "WHILE",
    "WINDOW",
    "WITH",
    "WRITE",
    "XOR",
    "YEAR_MONTH",
    "ZEROFILL",
];

/// Check if a name is a MySQL reserved keyword (case-insensitive).
fn is_mysql_reserved_keyword(name: &str) -> bool {
    MYSQL_RESERVED_KEYWORDS.contains(&name.to_uppercase().as_str())
}

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
        ssl: parse_bool_env("MYSQL_SSL"),
        ssl_accept_invalid_certs: parse_bool_env("MYSQL_SSL_ACCEPT_INVALID_CERTS"),
        ssl_ca: std::env::var("MYSQL_SSL_CA").ok().filter(|s| !s.is_empty()),
        allow_runtime_connections: parse_bool_env("MYSQL_ALLOW_RUNTIME_CONNECTIONS"),
        schema_permissions: parse_schema_permissions(),
        performance_hints: std::env::var("MYSQL_PERFORMANCE_HINTS").ok(),
        slow_query_threshold_ms: parse_env_num::<u64>("MYSQL_SLOW_QUERY_THRESHOLD_MS"),
        max_rows: parse_env_num::<u32>("MYSQL_MAX_ROWS"),
        max_sessions: parse_env_num::<u32>("MYSQL_MAX_SESSIONS"),
        max_total_connections: parse_env_num::<u32>("MYSQL_MAX_TOTAL_CONNECTIONS"),
        retry_attempts: parse_env_num::<u32>("MYSQL_RETRY_ATTEMPTS"),
        max_result_memory_mb: parse_env_num::<u32>("MYSQL_MAX_RESULT_MEMORY_MB"),
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
    const PREFIX: &str = "MYSQL_SCHEMA_";
    const SUFFIX: &str = "_PERMISSIONS";

    for (key, val) in std::env::vars() {
        // Early filter: skip keys that don't start with our prefix to avoid
        // unnecessary string operations on irrelevant environment variables.
        if !key.starts_with(PREFIX) {
            continue;
        }
        if let Some(schema_name) = key
            .strip_prefix(PREFIX)
            .and_then(|s| s.strip_suffix(SUFFIX))
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
            if is_mysql_reserved_keyword(&schema_name) {
                eprintln!(
                    "Warning: schema name '{schema_name}' in {key} is a MySQL reserved keyword; \
                     this may require quoting and could cause unexpected behavior"
                );
            }
            let ops: Vec<String> = val
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect();
            if ops.is_empty() {
                eprintln!(
                    "Warning: {key} has an empty permissions list; skipping \
                     (use e.g. MYSQL_SCHEMA_{}_PERMISSIONS=insert,update to allow writes)",
                    schema_name.to_uppercase()
                );
                continue;
            }
            for op in &ops {
                if !op.is_empty() && !matches!(op.as_str(), "insert" | "update" | "delete" | "ddl")
                {
                    eprintln!(
                        "Warning: unrecognized permission '{}' in {}; valid values: insert, update, delete, ddl",
                        op, key
                    );
                }
            }
            // Only set permissions that are explicitly listed; unlisted ops remain
            // None so they don't override values from TOML or defaults.
            let perms = SchemaPermissions {
                allow_insert: ops.iter().any(|s| s == "insert").then_some(true),
                allow_update: ops.iter().any(|s| s == "update").then_some(true),
                allow_delete: ops.iter().any(|s| s == "delete").then_some(true),
                allow_ddl: ops.iter().any(|s| s == "ddl").then_some(true),
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
    pub ssl: Option<bool>,
    pub ssl_accept_invalid_certs: Option<bool>,
    pub ssl_ca: Option<String>,
    pub allow_runtime_connections: Option<bool>,
    pub schema_permissions: HashMap<String, SchemaPermissions>,
    pub performance_hints: Option<String>,
    pub slow_query_threshold_ms: Option<u64>,
    pub max_rows: Option<u32>,
    pub max_sessions: Option<u32>,
    pub max_total_connections: Option<u32>,
    pub retry_attempts: Option<u32>,
    pub max_result_memory_mb: Option<u32>,
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
        if let Some(v) = self.ssl {
            base.security.ssl = v;
        }
        if let Some(v) = self.ssl_accept_invalid_certs {
            base.security.ssl_accept_invalid_certs = v;
        }
        if let Some(v) = self.ssl_ca {
            base.security.ssl_ca = Some(v);
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
        if let Some(v) = self.max_rows {
            base.pool.max_rows = v;
        }
        if let Some(v) = self.max_sessions {
            base.security.max_sessions = v;
        }
        if let Some(v) = self.max_total_connections {
            base.security.max_total_connections = v;
        }
        if let Some(v) = self.retry_attempts {
            base.pool.retry_attempts = v;
        }
        if let Some(v) = self.max_result_memory_mb {
            base.pool.max_result_memory_mb = v;
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
