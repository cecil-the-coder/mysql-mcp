use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlConnectOptions, MySqlSslMode};
use std::str::FromStr;
use crate::config::Config;

pub struct DbPool {
    pool: MySqlPool,
    pub config: Arc<Config>,
}

impl DbPool {
    pub async fn new(config: Arc<Config>) -> Result<Self> {
        let pool = build_pool(&config).await?;
        Ok(Self { pool, config })
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
}

async fn build_pool(config: &Config) -> Result<MySqlPool> {
    let connect_options = build_connect_options_from_config(config)?
        .statement_cache_capacity(config.pool.statement_cache_capacity as usize);

    let pool = MySqlPoolOptions::new()
        .max_connections(config.pool.size)
        .acquire_timeout(Duration::from_millis(config.pool.connect_timeout_ms))
        .idle_timeout(Duration::from_secs(300))
        .max_lifetime(Duration::from_secs(1800))
        .connect_with(connect_options)
        .await?;

    Ok(pool)
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
    let ssl_mode = match (ssl, ssl_accept_invalid_certs) {
        (false, _) => MySqlSslMode::Disabled,
        (true, true) => MySqlSslMode::Required,
        (true, false) => MySqlSslMode::VerifyIdentity,
    };
    let mut opts = MySqlConnectOptions::new()
        .host(host)
        .port(port)
        .username(user)
        .password(password)
        .ssl_mode(ssl_mode);
    if let Some(db) = database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = ssl_ca {
        opts = opts.ssl_ca(ca_path);
    }
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_millis(connect_timeout_ms))
        .idle_timeout(Duration::from_secs(300))
        .max_lifetime(Duration::from_secs(1800))
        .connect_with(opts)
        .await?;
    Ok(pool)
}

fn build_connect_options_from_config(config: &Config) -> Result<MySqlConnectOptions> {
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
    let ssl_mode = match (config.security.ssl, config.security.ssl_accept_invalid_certs) {
        (false, _) => MySqlSslMode::Disabled,
        (true, true) => MySqlSslMode::Required,       // SSL on, skip cert verification
        (true, false) => MySqlSslMode::VerifyIdentity, // SSL on, verify cert + hostname
    };
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
