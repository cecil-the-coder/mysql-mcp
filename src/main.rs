use anyhow::Result;
use std::sync::Arc;
use tracing::info;

mod backend;
mod config;
#[cfg(feature = "mysql")]
pub mod db;
#[cfg(all(test, feature = "mysql"))]
mod e2e_session_tests;
#[cfg(all(test, feature = "mysql"))]
mod e2e_ssh_tests;
#[cfg(all(test, feature = "mysql"))]
mod e2e_test_utils;
#[cfg(all(test, feature = "mysql"))]
mod e2e_tests;
#[cfg(all(test, feature = "mysql"))]
mod perf_tests;
#[cfg(all(test, feature = "mysql"))]
mod perf_write_tests;
pub mod permissions;
pub mod query;
pub mod schema;
pub mod server;
pub mod sql_parser;
#[cfg(all(test, feature = "mysql"))]
pub mod test_helpers;
pub mod tunnel;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing — MUST write to stderr, not stdout.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    info!("sql-mcp starting");

    // Load configuration
    let raw_config = config::load_config()?;
    raw_config.validate()?;
    let config = Arc::new(raw_config);
    info!("Configuration loaded");

    // Create the appropriate backend based on config
    let backend = backend::create_backend(&config.backend);
    info!("Using {} backend", backend.backend_name());

    // Connect to the database (with optional SSH tunnel for the default session)
    let (db, _default_tunnel) = if let Some(ref ssh) = config.ssh {
        info!("SSH tunnel configured, establishing tunnel to {}", ssh.host);
        let tunnel =
            tunnel::spawn_ssh_tunnel(ssh, &config.connection.host, config.connection.port.unwrap_or(backend.default_port()))
                .await?;
        info!("SSH tunnel established, connecting through tunnel");
        let params = backend::SessionConnectParams {
            host: "127.0.0.1".to_string(),
            port: tunnel.local_port,
            user: config.connection.user.clone(),
            password: config.connection.password.clone(),
            database: config.connection.database.clone(),
            ssl: config.security.ssl,
            ssl_accept_invalid_certs: config.security.ssl_accept_invalid_certs,
            ssl_ca: config.security.ssl_ca.clone(),
            connect_timeout_ms: config.pool.connect_timeout_ms,
        };
        let pool = backend.create_session_pool(&params).await?;
        info!("Database pool created through SSH tunnel");
        (pool, Some(tunnel))
    } else {
        let pool = backend.create_pool(&config, &config.security).await?;
        info!("Database pool created");
        (pool, None)
    };

    // Warm up one connection so the pool is ready for the first query
    {
        let warmup_pool = db.clone();
        tokio::spawn(async move {
            // The warmup uses fetch_all with a simple query to verify connectivity
            match warmup_pool.fetch_all("SELECT 1").await {
                Ok(_) => {
                    tracing::debug!("Pool warmup complete (1 connection)");
                }
                Err(e) => tracing::warn!("Pool warmup connection failed: {}", e),
            }
        });
    }

    // Create and run the MCP server
    let mcp_server = server::McpServer::new(config, db, backend, _default_tunnel);
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
