//! Database connection pool management.
//!
//! This module provides MySQL connection pool creation and management using `sqlx`.
//! It supports direct TCP connections, Unix socket connections, and SSH-tunneled
//! connections through bastion hosts. SSL/TLS verification is configurable based
//! on security requirements.
//!
//! # Key Functions
//!
//! - [`build_pool`] - Creates the main connection pool from configuration
//! - [`build_pool_and_tunnel`] - Creates a pool through an SSH tunnel
//! - [`build_session_pool`] - Creates a small pool for named sessions
//! - [`build_connect_options`] - Builds `MySqlConnectOptions` from configuration
//!
//! # Connection Types
//!
//! - **TCP**: Standard network connections with optional SSL
//! - **Socket**: Unix domain socket connections (local only)
//! - **Connection String**: Full `mysql://` URL parsing
//! - **SSH Tunnel**: Connections proxied through a bastion host
//!
//! # SSL Modes
//!
//! SSL mode is determined by the `ssl`, `ssl_accept_invalid_certs`, and `ssl_ca` flags:
//! - `Disabled`: No SSL
//! - `Required`: SSL without certificate validation
//! - `VerifyCa`: SSL with CA validation (when `ssl_ca` is set)
//! - `VerifyIdentity`: Full SSL with hostname verification
//!
//! # Example
//!
//! ```ignore
//! let pool = build_pool(&config).await?;
//! let row = sqlx::query("SELECT 1").fetch_one(&pool).await?;
//! ```

use crate::config::Config;
use anyhow::Result;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlSslMode};
use std::str::FromStr;
use std::time::Duration;

/// Number of prepared statements cached per connection.
/// Only effective when using sqlx prepared statement macros.
pub const STATEMENT_CACHE_CAPACITY: usize = 100;

/// Idle connection lifetime before it is closed and removed from the pool.
const POOL_IDLE_TIMEOUT_SECS: u64 = 300;

/// Maximum lifetime of any pooled connection before it is recycled.
const POOL_MAX_LIFETIME_SECS: u64 = 1800;

/// Creates the main connection pool from configuration.
///
/// This function builds a connection pool suitable for general application use,
/// using the pool size and timeout settings from the configuration. The pool
/// supports TCP connections, Unix sockets, or connection strings depending on
/// what's configured.
///
/// # Arguments
///
/// * `config` - The application configuration containing connection details,
///   pool settings, and security options.
///
/// # Returns
///
/// A `Result` containing the configured `MySqlPool` on success, or an error
/// if the pool creation fails (e.g., invalid connection string, network issues).
///
/// # Examples
///
/// ```ignore
/// use mysql_mcp::config::Config;
/// use mysql_mcp::db::build_pool;
///
/// async fn example(config: &Config) -> anyhow::Result<sqlx::MySqlPool> {
///     let pool = build_pool(config).await?;
///     Ok(pool)
/// }
/// ```
pub async fn build_pool(config: &Config) -> Result<MySqlPool> {
    let connect_options =
        build_connect_options(config)?.statement_cache_capacity(STATEMENT_CACHE_CAPACITY);

    // sqlx manages its own internal connection queue; there is no queue_limit API.
    // Backpressure is provided by acquire_timeout: callers that cannot obtain a
    // connection within that window receive an error rather than queuing forever.
    create_pool(
        connect_options,
        config.pool.size,
        config.pool.connect_timeout_ms,
    )
    .await
}

/// Build a small pool (max 5 connections) for a named session from raw connection fields.
///
/// This is useful for creating temporary pools for specific database sessions or
/// interactive operations where a full-sized pool is not needed. The pool has
/// a hardcoded maximum of 5 connections to limit resource usage.
///
/// # Arguments
///
/// * `host` - The database server hostname or IP address.
/// * `port` - The database server port (typically 3306 for MySQL).
/// * `user` - The database username.
/// * `password` - The database password.
/// * `database` - Optional database name to connect to.
/// * `ssl` - Whether to enable SSL/TLS encryption.
/// * `ssl_accept_invalid_certs` - If true, accept invalid certificates (less secure).
/// * `ssl_ca` - Optional path to a CA certificate file for certificate validation.
/// * `connect_timeout_ms` - Connection timeout in milliseconds.
///
/// # Returns
///
/// A `Result` containing a small `MySqlPool` (max 5 connections) on success.
///
/// # Examples
///
/// ```ignore
/// use mysql_mcp::db::build_session_pool;
///
/// async fn example() -> anyhow::Result<sqlx::MySqlPool> {
///     let pool = build_session_pool(
///         "localhost",
///         3306,
///         "root",
///         "password",
///         Some("mydb"),
///         true,   // ssl enabled
///         false,  // strict cert validation
///         None,   // no custom CA
///         5000,   // 5 second timeout
///     ).await?;
///     Ok(pool)
/// }
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn build_session_pool(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: Option<&str>,
    ssl: bool,
    ssl_accept_invalid_certs: bool,
    ssl_ca: Option<&str>,
    connect_timeout_ms: u64,
) -> Result<MySqlPool> {
    let ssl_mode = determine_ssl_mode(ssl, ssl_accept_invalid_certs, ssl_ca.is_some());
    let opts = MySqlConnectOptions::new()
        .host(host)
        .port(port)
        .username(user)
        .password(password)
        .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    let opts = apply_ssl_and_db_options(opts, ssl_mode, ssl_ca, database);
    create_pool(opts, 5, connect_timeout_ms).await
}

/// Apply consistent pool sizing, timeouts, and lifetime settings.
async fn create_pool(
    opts: MySqlConnectOptions,
    max_connections: u32,
    acquire_timeout_ms: u64,
) -> Result<MySqlPool> {
    let pool = MySqlPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_millis(acquire_timeout_ms))
        .idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
        .max_lifetime(Duration::from_secs(POOL_MAX_LIFETIME_SECS))
        .connect_with(opts)
        .await?;
    Ok(pool)
}

/// Map the three SSL flags to a `MySqlSslMode`.
///
/// | ssl   | accept_invalid | has_ca | mode             |
/// |-------|---------------|--------|------------------|
/// | false | *             | *      | Disabled         |
/// | true  | true          | *      | Required         |
/// | true  | false         | true   | VerifyCa         |
/// | true  | false         | false  | VerifyIdentity   |
fn determine_ssl_mode(ssl: bool, accept_invalid: bool, has_ca: bool) -> MySqlSslMode {
    match (ssl, accept_invalid, has_ca) {
        (false, _, _) => MySqlSslMode::Disabled,
        (true, true, _) => MySqlSslMode::Required,
        (true, false, true) => MySqlSslMode::VerifyCa,
        (true, false, false) => MySqlSslMode::VerifyIdentity,
    }
}

/// Apply SSL mode, optional database, and optional CA certificate to MySqlConnectOptions.
///
/// This helper centralizes the common logic for configuring SSL and database options
/// across all connection pool builders to eliminate code duplication.
fn apply_ssl_and_db_options(
    opts: MySqlConnectOptions,
    ssl_mode: MySqlSslMode,
    ssl_ca: Option<&str>,
    database: Option<&str>,
) -> MySqlConnectOptions {
    let mut opts = opts.ssl_mode(ssl_mode);
    if let Some(db) = database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
    opts
}

/// Downgrade `VerifyIdentity` to `Required` for connections through an SSH tunnel.
///
/// Through a tunnel sqlx connects to `127.0.0.1`, so the server certificate's
/// CN/SAN (which matches the real DB hostname) can never match the loopback
/// address.  This keeps encryption intact while skipping the impossible hostname
/// check.
fn adjust_ssl_mode_for_tunnel(ssl_mode: MySqlSslMode) -> MySqlSslMode {
    if matches!(ssl_mode, MySqlSslMode::VerifyIdentity) {
        tracing::warn!(
            "SSL mode VerifyIdentity downgraded to Required: hostname verification \
             is not meaningful through an SSH tunnel (connecting to 127.0.0.1)"
        );
        MySqlSslMode::Required
    } else {
        ssl_mode
    }
}

/// Build a pool that connects through an already-established SSH tunnel.
/// Connects sqlx to `127.0.0.1:{tunnel.local_port}` rather than the real DB host/port.
async fn build_pool_tunneled(
    config: &Config,
    tunnel: &crate::tunnel::TunnelHandle,
) -> Result<MySqlPool> {
    let ssl_mode = adjust_ssl_mode_for_tunnel(determine_ssl_mode(
        config.security.ssl,
        config.security.ssl_accept_invalid_certs,
        config.security.ssl_ca.is_some(),
    ));
    let opts = MySqlConnectOptions::new()
        .host("127.0.0.1")
        .port(tunnel.local_port)
        .username(&config.connection.user)
        .password(&config.connection.password)
        .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    let opts = apply_ssl_and_db_options(
        opts,
        ssl_mode,
        config.security.ssl_ca.as_deref(),
        config.connection.database.as_deref(),
    );
    create_pool(opts, config.pool.size, config.pool.connect_timeout_ms).await
}

/// Build a connection pool through an SSH tunnel.
///
/// Spawns an SSH subprocess to create a tunnel, waits for it to be ready,
/// then creates a connection pool that connects through the tunnel to the
/// database. This allows secure access to databases behind bastion hosts.
///
/// # Arguments
///
/// * `config` - The application configuration containing connection details,
///   pool settings, and security options.
/// * `ssh` - SSH configuration for the bastion host (host, port, user, key, etc.).
///
/// # Returns
///
/// A `Result` containing a tuple of:
/// * A `MySqlPool` connected through the tunnel
/// * A `TunnelHandle` that must be kept alive to maintain the SSH tunnel
///
/// # Note
///
/// The caller must keep the returned tunnel handle alive for the duration
/// the pool is in use. When the tunnel handle is dropped, the SSH tunnel
/// will be closed and the pool connections will fail.
///
/// SSL `VerifyIdentity` mode is automatically downgraded to `Required` when
/// using an SSH tunnel, because hostname verification cannot work when
/// connecting to `127.0.0.1` (the local tunnel endpoint).
///
/// # Examples
///
/// ```ignore
/// use mysql_mcp::config::{Config, SshConfig};
/// use mysql_mcp::db::build_pool_and_tunnel;
///
/// async fn example(config: &Config, ssh: &SshConfig) -> anyhow::Result<(sqlx::MySqlPool, mysql_mcp::tunnel::TunnelHandle)> {
///     let (pool, tunnel) = build_pool_and_tunnel(config, ssh).await?;
///     Ok((pool, tunnel))
/// }
/// ```
pub async fn build_pool_and_tunnel(
    config: &Config,
    ssh: &crate::config::SshConfig,
) -> Result<(MySqlPool, crate::tunnel::TunnelHandle)> {
    let tunnel =
        crate::tunnel::spawn_ssh_tunnel(ssh, &config.connection.host, config.connection.port)
            .await?;
    let pool = build_pool_tunneled(config, &tunnel).await?;
    Ok((pool, tunnel))
}

/// Build a small session pool (max 5 connections) through an SSH tunnel.
///
/// Spawns an SSH tunnel to connect to the remote database via a bastion host,
/// then creates a small connection pool that connects through the tunnel.
/// The returned tunnel handle must be kept alive for the duration of pool use.
///
/// # Arguments
///
/// * `host` - The target database server hostname or IP address (accessed through the tunnel).
/// * `port` - The target database server port (typically 3306 for MySQL).
/// * `user` - The database username.
/// * `password` - The database password.
/// * `database` - Optional database name to connect to.
/// * `ssl` - Whether to enable SSL/TLS encryption (through the tunnel).
/// * `ssl_accept_invalid_certs` - If true, accept invalid certificates.
/// * `ssl_ca` - Optional path to a CA certificate file for certificate validation.
/// * `connect_timeout_ms` - Connection timeout in milliseconds.
/// * `ssh` - SSH configuration for the bastion host (host, port, user, key, etc.).
///
/// # Returns
///
/// A `Result` containing a tuple of:
/// * A small `MySqlPool` (max 5 connections) connected through the tunnel
/// * A `TunnelHandle` that must be kept alive to maintain the SSH tunnel
///
/// # Note
///
/// SSL `VerifyIdentity` mode is automatically downgraded to `Required` when
/// using an SSH tunnel, because hostname verification cannot work when
/// connecting to `127.0.0.1` (the local tunnel endpoint).
///
/// # Examples
///
/// ```ignore
/// use mysql_mcp::config::SshConfig;
/// use mysql_mcp::db::build_session_pool_with_tunnel;
///
/// async fn example() -> anyhow::Result<(sqlx::MySqlPool, mysql_mcp::tunnel::TunnelHandle)> {
///     let ssh = SshConfig {
///         host: "bastion.example.com".to_string(),
///         port: 22,
///         user: "ssh-user".to_string(),
///         private_key: "/path/to/key.pem".to_string(),
///         timeout_secs: 30,
///         strict_host_key_checking: true,
///     };
///
///     let (pool, tunnel) = build_session_pool_with_tunnel(
///         "internal-db.example.com",
///         3306,
///         "dbuser",
///         "dbpass",
///         Some("mydb"),
///         true,   // ssl enabled
///         false,  // strict cert validation
///         None,   // no custom CA
///         5000,   // 5 second timeout
///         &ssh,
///     ).await?;
///
///     // Use the pool...
///     // The tunnel handle must remain alive while using the pool
///     Ok((pool, tunnel))
/// }
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn build_session_pool_with_tunnel(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: Option<&str>,
    ssl: bool,
    ssl_accept_invalid_certs: bool,
    ssl_ca: Option<&str>,
    connect_timeout_ms: u64,
    ssh: &crate::config::SshConfig,
) -> Result<(MySqlPool, crate::tunnel::TunnelHandle)> {
    let tunnel = crate::tunnel::spawn_ssh_tunnel(ssh, host, port).await?;
    let ssl_mode = adjust_ssl_mode_for_tunnel(determine_ssl_mode(
        ssl,
        ssl_accept_invalid_certs,
        ssl_ca.is_some(),
    ));
    let opts = MySqlConnectOptions::new()
        .host("127.0.0.1")
        .port(tunnel.local_port)
        .username(user)
        .password(password)
        .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    let opts = apply_ssl_and_db_options(opts, ssl_mode, ssl_ca, database);
    let pool = create_pool(opts, 5, connect_timeout_ms).await?;
    Ok((pool, tunnel))
}

pub fn build_connect_options(config: &Config) -> Result<MySqlConnectOptions> {
    let conn = &config.connection;

    // If a full mysql:// URL is given, parse it directly.
    // Reject unrecognized schemes rather than silently falling through to TCP.
    if let Some(cs) = &conn.connection_string {
        if cs.starts_with("mysql://") || cs.starts_with("mysql+ssl://") {
            let opts = MySqlConnectOptions::from_str(cs)?
                .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
            return Ok(opts);
        } else {
            // Find where to truncate the preview. If "://" is present, include it;
            // otherwise truncate to 32 bytes, walking back to a valid UTF-8 char boundary.
            let split_pos = if let Some(i) = cs.find("://") {
                i + 3
            } else {
                let mut pos = cs.len().min(32);
                while pos > 0 && !cs.is_char_boundary(pos) {
                    pos -= 1;
                }
                pos
            };
            anyhow::bail!(
                "connection_string must start with 'mysql://' or 'mysql+ssl://', got: '{}'",
                &cs[..split_pos]
            );
        }
    }

    // Unix socket path
    if let Some(socket) = &conn.socket {
        let mut opts = MySqlConnectOptions::new()
            .socket(socket)
            .username(&conn.user)
            .password(&conn.password)
            .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        if let Some(db) = &conn.database {
            opts = opts.database(db);
        }
        return Ok(opts);
    }

    // TCP connection
    // When ssl_ca is set without ssl_accept_invalid_certs, use VerifyCa:
    //   - validates the cert chain against the specified CA
    //   - does NOT check hostname/IP (servers often use CN=hostname without a SAN)
    // VerifyIdentity would also check hostname, which fails for IP-addressed servers
    // with a CN-only cert.
    let ssl_mode = determine_ssl_mode(
        config.security.ssl,
        config.security.ssl_accept_invalid_certs,
        config.security.ssl_ca.is_some(),
    );
    let opts = MySqlConnectOptions::new()
        .host(&conn.host)
        .port(conn.port)
        .username(&conn.user)
        .password(&conn.password);
    let opts = apply_ssl_and_db_options(
        opts,
        ssl_mode,
        config.security.ssl_ca.as_deref(),
        conn.database.as_deref(),
    );
    Ok(opts)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to check SSL mode since MySqlSslMode doesn't implement PartialEq
    fn ssl_mode_is(mode: MySqlSslMode, expected: &str) -> bool {
        format!("{:?}", mode) == expected
    }

    #[test]
    fn test_determine_ssl_mode_disabled() {
        // When ssl=false, SSL should be disabled regardless of other flags
        assert!(
            ssl_mode_is(determine_ssl_mode(false, false, false), "Disabled"),
            "ssl=false, accept_invalid=false, has_ca=false should be Disabled"
        );
        assert!(
            ssl_mode_is(determine_ssl_mode(false, true, false), "Disabled"),
            "ssl=false, accept_invalid=true, has_ca=false should be Disabled"
        );
        assert!(
            ssl_mode_is(determine_ssl_mode(false, false, true), "Disabled"),
            "ssl=false, accept_invalid=false, has_ca=true should be Disabled"
        );
        assert!(
            ssl_mode_is(determine_ssl_mode(false, true, true), "Disabled"),
            "ssl=false, accept_invalid=true, has_ca=true should be Disabled"
        );
    }

    #[test]
    fn test_determine_ssl_mode_required() {
        // When ssl=true and accept_invalid=true, use Required (no cert validation)
        assert!(
            ssl_mode_is(determine_ssl_mode(true, true, false), "Required"),
            "ssl=true, accept_invalid=true, has_ca=false should be Required"
        );
        assert!(
            ssl_mode_is(determine_ssl_mode(true, true, true), "Required"),
            "ssl=true, accept_invalid=true, has_ca=true should be Required"
        );
    }

    #[test]
    fn test_determine_ssl_mode_verify_ca() {
        // When ssl=true, accept_invalid=false, and has_ca=true, use VerifyCa
        assert!(
            ssl_mode_is(determine_ssl_mode(true, false, true), "VerifyCa"),
            "ssl=true, accept_invalid=false, has_ca=true should be VerifyCa"
        );
    }

    #[test]
    fn test_determine_ssl_mode_verify_identity() {
        // When ssl=true, accept_invalid=false, and has_ca=false, use VerifyIdentity
        assert!(
            ssl_mode_is(determine_ssl_mode(true, false, false), "VerifyIdentity"),
            "ssl=true, accept_invalid=false, has_ca=false should be VerifyIdentity"
        );
    }

    #[test]
    fn test_adjust_ssl_mode_for_tunnel_downgrades_verify_identity() {
        // VerifyIdentity should be downgraded to Required for SSH tunnels
        assert!(
            ssl_mode_is(
                adjust_ssl_mode_for_tunnel(MySqlSslMode::VerifyIdentity),
                "Required"
            ),
            "VerifyIdentity should be downgraded to Required through tunnel"
        );
    }

    #[test]
    fn test_adjust_ssl_mode_for_tunnel_preserves_other_modes() {
        // All other modes should pass through unchanged
        for (mode, name) in [
            (MySqlSslMode::Disabled, "Disabled"),
            (MySqlSslMode::Required, "Required"),
            (MySqlSslMode::VerifyCa, "VerifyCa"),
        ] {
            assert!(
                ssl_mode_is(adjust_ssl_mode_for_tunnel(mode), name),
                "{name} should pass through unchanged"
            );
        }
    }
}
