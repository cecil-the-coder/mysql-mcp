use std::time::Duration;
use anyhow::Result;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlConnectOptions, MySqlSslMode};
use std::str::FromStr;
use crate::config::Config;

/// Number of prepared statements cached per connection.
/// Only effective when using sqlx prepared statement macros.
pub const STATEMENT_CACHE_CAPACITY: usize = 100;

/// Idle connection lifetime before it is closed and removed from the pool.
const POOL_IDLE_TIMEOUT_SECS: u64 = 300;

/// Maximum lifetime of any pooled connection before it is recycled.
const POOL_MAX_LIFETIME_SECS: u64 = 1800;

pub async fn build_pool(config: &Config) -> Result<MySqlPool> {
    let connect_options = build_connect_options(config)?
        .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);

    // sqlx manages its own internal connection queue; there is no queue_limit API.
    // Backpressure is provided by acquire_timeout: callers that cannot obtain a
    // connection within that window receive an error rather than queuing forever.
    create_pool(connect_options, config.pool.size, config.pool.connect_timeout_ms).await
}

/// Build a small pool (max 5) for a named session from raw connection fields.
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
        .ssl_mode(determine_ssl_mode(ssl, ssl_accept_invalid_certs, ssl_ca.is_some()));
    if let Some(db) = database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
    create_pool(opts, 5, connect_timeout_ms).await
}

/// Apply consistent pool sizing, timeouts, and lifetime settings.
async fn create_pool(opts: MySqlConnectOptions, max_connections: u32, acquire_timeout_ms: u64) -> Result<MySqlPool> {
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
        (false, _, _)        => MySqlSslMode::Disabled,
        (true, true, _)      => MySqlSslMode::Required,
        (true, false, true)  => MySqlSslMode::VerifyCa,
        (true, false, false) => MySqlSslMode::VerifyIdentity,
    }
}

pub fn build_connect_options(config: &Config) -> Result<MySqlConnectOptions> {
    let conn = &config.connection;

    // If a full mysql:// URL is given, parse it directly
    if let Some(cs) = &conn.connection_string {
        if cs.starts_with("mysql://") || cs.starts_with("mysql+ssl://") {
            let opts = MySqlConnectOptions::from_str(cs)?;
            return Ok(opts);
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
