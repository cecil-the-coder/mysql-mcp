use std::sync::Arc;
use anyhow::Result;
use tracing::info;

mod config;
pub mod sql_parser;
pub mod db;
pub mod permissions;
pub mod query;
pub mod schema;
pub mod server;
#[cfg(test)]
pub mod test_helpers;
#[cfg(test)]
mod e2e_tests;
#[cfg(test)]
mod perf_tests;

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
    let raw_config = config::merge::load_config()?;
    raw_config.validate()?;
    let config = Arc::new(raw_config);
    info!("Configuration loaded");

    // Connect to the database
    let db = Arc::new(db::DbPool::new(config.clone()).await?);
    info!("Database pool created");

    // Warm up the connection pool in the background
    if config.pool.warmup_connections > 0 {
        let warmup_pool = db.pool().clone();
        let n = config.pool.warmup_connections;
        tokio::spawn(async move {
            for i in 0..n {
                match warmup_pool.acquire().await {
                    Ok(conn) => drop(conn),
                    Err(e) => tracing::warn!("Pool warmup connection {} failed: {}", i + 1, e),
                }
            }
            tracing::debug!("Pool warmup complete ({} connections)", n);
        });
    }

    // Create and run the MCP server
    let mcp_server = server::McpServer::new(config, db);
    info!("MCP server starting on stdio");
    mcp_server.run().await?;

    Ok(())
}
