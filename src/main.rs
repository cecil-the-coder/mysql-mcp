use anyhow::Result;
use std::sync::Arc;
use tracing::info;

mod config;
pub mod db;
#[cfg(test)]
mod e2e_session_tests;
#[cfg(test)]
mod e2e_ssh_tests;
#[cfg(test)]
mod e2e_test_utils;
#[cfg(test)]
mod e2e_tests;
#[cfg(test)]
mod perf_tests;
#[cfg(test)]
mod perf_write_tests;
pub mod permissions;
pub mod query;
pub mod schema;
pub mod server;
pub mod sql_parser;
#[cfg(test)]
pub mod test_helpers;
pub mod tunnel;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing — MUST write to stderr, not stdout.
    // The MCP server uses stdout as the JSON-RPC transport; any log line on stdout
    // would corrupt the protocol stream and appear as malformed input to the client.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    info!("mysql-mcp starting");

    // Load configuration
    let raw_config = config::load_config()?;
    raw_config.validate()?;
    let config = Arc::new(raw_config);
    info!("Configuration loaded");

    // Connect to the database (with optional SSH tunnel for the default session)
    let (db, _default_tunnel) = if let Some(ref ssh) = config.ssh {
        info!("SSH tunnel configured, establishing tunnel to {}", ssh.host);
        let (pool, tunnel) = db::build_pool_and_tunnel(&config, ssh).await?;
        info!("SSH tunnel established and database pool created");
        (Arc::new(pool), Some(tunnel))
    } else {
        let pool = db::build_pool(&config).await?;
        info!("Database pool created");
        (Arc::new(pool), None)
    };

    // Warm up one connection so the pool is ready for the first query.
    // Retry up to 3 attempts to improve reliability on slow startup scenarios
    // (e.g., SSH tunnels that may need time to establish).
    {
        let warmup_pool = (*db).clone();
        let acquire_timeout_ms = config.pool.connect_timeout_ms;
        tokio::spawn(async move {
            let max_attempts: u32 = 3;
            let retry_delay = std::time::Duration::from_millis(500);
            for attempt in 1..=max_attempts {
                match tokio::time::timeout(
                    std::time::Duration::from_millis(acquire_timeout_ms),
                    warmup_pool.acquire(),
                )
                .await
                {
                    Ok(Ok(conn)) => {
                        drop(conn);
                        tracing::debug!("Pool warmup complete (1 connection) on attempt {}", attempt);
                        return;
                    }
                    Ok(Err(e)) => {
                        if attempt < max_attempts {
                            tracing::warn!(
                                "Pool warmup attempt {}/{} failed: {}. Retrying in {:?}...",
                                attempt, max_attempts, e, retry_delay
                            );
                            tokio::time::sleep(retry_delay).await;
                        } else {
                            tracing::warn!(
                                "Pool warmup attempt {}/{} failed: {}. Giving up.",
                                attempt, max_attempts, e
                            );
                        }
                    }
                    Err(_) => {
                        if attempt < max_attempts {
                            tracing::warn!(
                                "Pool warmup attempt {}/{} timed out after {}ms. Retrying in {:?}...",
                                attempt, max_attempts, acquire_timeout_ms, retry_delay
                            );
                            tokio::time::sleep(retry_delay).await;
                        } else {
                            tracing::warn!(
                                "Pool warmup attempt {}/{} timed out after {}ms. Giving up.",
                                attempt, max_attempts, acquire_timeout_ms
                            );
                        }
                    }
                }
            }
        });
    }

    // Create and run the MCP server
    let mcp_server = server::McpServer::new(config, db, _default_tunnel);
    info!("MCP server starting on stdio");

    tokio::select! {
        result = mcp_server.run() => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl-C, shutting down");
        }
        _ = async {
            #[cfg(unix)]
            {
                match tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                ) {
                    Ok(mut sigterm) => { sigterm.recv().await; }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to install SIGTERM handler (restricted environment?): {}. \
                             Ctrl-C is still available.", e
                        );
                        std::future::pending::<()>().await;
                    }
                }
            }
            #[cfg(not(unix))]
            std::future::pending::<()>().await;
        } => {
            info!("Received SIGTERM, shutting down");
        }
    }

    Ok(())
}
