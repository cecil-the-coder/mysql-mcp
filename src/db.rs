//! Database pool creation facade.
//!
//! This module provides backward-compatible functions for creating MySQL pools.
//! The actual implementation lives in `backend::mysql`. This facade exists so
//! that existing code (tests, benchmarks, e2e tests) that import `crate::db::*` continue
//! to work without modification.

use crate::config::Config;
use anyhow::Result;
use sqlx::mysql::MySqlConnectOptions;

pub async fn build_pool(config: &Config) -> Result<sqlx::MySqlPool> {
    crate::backend::mysql::build_pool(config).await
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
) -> Result<sqlx::MySqlPool> {
    crate::backend::mysql::build_session_pool_internal(
        &crate::backend::SessionConnectParams {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            database: database.map(String::from),
            ssl,
            ssl_accept_invalid_certs,
            ssl_ca: ssl_ca.map(String::from),
            connect_timeout_ms,
        },
    )
    .await
}

/// Map the three SSL flags to a `MySqlSslMode`.
pub fn determine_ssl_mode(ssl: bool, accept_invalid: bool, has_ca: bool) -> sqlx::mysql::MySqlSslMode {
    crate::backend::mysql::determine_ssl_mode(ssl, accept_invalid, has_ca)
}

/// Build a pool that connects through an already-established SSH tunnel.
pub async fn build_pool_tunneled(
    config: &Config,
    tunnel: &crate::tunnel::TunnelHandle,
) -> Result<sqlx::MySqlPool> {
    crate::backend::mysql::build_pool_tunneled(config, tunnel).await
}

/// Build a connection pool through an SSH tunnel.
pub async fn build_pool_and_tunnel(
    config: &Config,
    ssh: &crate::config::SshConfig,
) -> Result<(sqlx::MySqlPool, crate::tunnel::TunnelHandle)> {
    let tunnel =
        crate::tunnel::spawn_ssh_tunnel(ssh, &config.connection.host, config.connection.port.unwrap_or(3306))
            .await?;
    let pool = build_pool_tunneled(config, &tunnel).await?;
    Ok((pool, tunnel))
}

/// Build a small session pool through an SSH tunnel.
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
) -> Result<(sqlx::MySqlPool, crate::tunnel::TunnelHandle)> {
    crate::backend::mysql::build_session_pool_with_tunnel_internal(
        &crate::backend::SessionConnectParams {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            database: database.map(String::from),
            ssl,
            ssl_accept_invalid_certs,
            ssl_ca: ssl_ca.map(String::from),
            connect_timeout_ms,
        },
        ssh,
    )
    .await
}

pub fn build_connect_options(config: &Config) -> Result<MySqlConnectOptions> {
    crate::backend::mysql::build_connect_options(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to check SSL mode since MySqlSslMode doesn't implement PartialEq
    fn ssl_mode_is(mode: sqlx::mysql::MySqlSslMode, expected: &str) -> bool {
        format!("{:?}", mode) == expected
    }

    #[test]
    fn test_determine_ssl_mode_disabled() {
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
        assert!(
            ssl_mode_is(determine_ssl_mode(true, false, true), "VerifyCa"),
            "ssl=true, accept_invalid=false, has_ca=true should be VerifyCa"
        );
    }

    #[test]
    fn test_determine_ssl_mode_verify_identity() {
        assert!(
            ssl_mode_is(determine_ssl_mode(true, false, false), "VerifyIdentity"),
            "ssl=true, accept_invalid=false, has_ca=false should be VerifyIdentity"
        );
    }
}
