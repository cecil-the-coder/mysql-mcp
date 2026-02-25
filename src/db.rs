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

/// Build a small pool (max 5) for a named session from raw connection fields.
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
    let mut opts = MySqlConnectOptions::new()
        .host(host)
        .port(port)
        .username(user)
        .password(password)
        .ssl_mode(determine_ssl_mode(
            ssl,
            ssl_accept_invalid_certs,
            ssl_ca.is_some(),
        ));
    if let Some(db) = database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
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

/// Build a pool that connects through an already-established SSH tunnel.
/// Connects sqlx to `127.0.0.1:{tunnel.local_port}` rather than the real DB host/port.
async fn build_pool_tunneled(
    config: &Config,
    tunnel: &crate::tunnel::TunnelHandle,
) -> Result<MySqlPool> {
    let ssl_mode = determine_ssl_mode(
        config.security.ssl,
        config.security.ssl_accept_invalid_certs,
        config.security.ssl_ca.is_some(),
    );
    let mut opts = MySqlConnectOptions::new()
        .host("127.0.0.1")
        .port(tunnel.local_port)
        .username(&config.connection.user)
        .password(&config.connection.password)
        .ssl_mode(ssl_mode)
        .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    if let Some(ref db) = config.connection.database {
        opts = opts.database(db);
    }
    if let Some(ref ca_path) = config.security.ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
    create_pool(opts, config.pool.size, config.pool.connect_timeout_ms).await
}

/// Build a connection pool through an SSH tunnel.
/// Spawns the SSH subprocess, waits for it to be ready, then connects sqlx through it.
/// Returns both the pool and the tunnel handle — the caller must keep the handle alive
/// for the duration the pool is in use.
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

/// Build a small session pool through an SSH tunnel.
/// Spawns the tunnel to reach `host:port` via the bastion in `ssh`, then connects sqlx
/// to `127.0.0.1:{tunnel.local_port}`.
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
    let mut opts = MySqlConnectOptions::new()
        .host("127.0.0.1")
        .port(tunnel.local_port)
        .username(user)
        .password(password)
        .ssl_mode(determine_ssl_mode(
            ssl,
            ssl_accept_invalid_certs,
            ssl_ca.is_some(),
        ));
    if let Some(db) = database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
    let pool = create_pool(opts, 5, connect_timeout_ms).await?;
    Ok((pool, tunnel))
}

pub fn build_connect_options(config: &Config) -> Result<MySqlConnectOptions> {
    let conn = &config.connection;

    // If a full mysql:// URL is given, parse it directly.
    // Reject unrecognized schemes rather than silently falling through to TCP.
    if let Some(cs) = &conn.connection_string {
        if cs.starts_with("mysql://") || cs.starts_with("mysql+ssl://") {
            let opts = MySqlConnectOptions::from_str(cs)?;
            return Ok(opts);
        } else {
            anyhow::bail!(
                "connection_string must start with 'mysql://' or 'mysql+ssl://', got: '{}'",
                cs.split_at(cs.find("://").map(|i| i + 3).unwrap_or(cs.len().min(32)))
                    .0
            );
        }
    }

    // Unix socket path
    if let Some(socket) = &conn.socket {
        let mut opts = MySqlConnectOptions::new()
            .socket(socket)
            .username(&conn.user)
            .password(&conn.password);
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
    let mut opts = MySqlConnectOptions::new()
        .host(&conn.host)
        .port(conn.port)
        .username(&conn.user)
        .password(&conn.password)
        .ssl_mode(ssl_mode);
    if let Some(db) = &conn.database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = &config.security.ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
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
}
