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
pub mod remote;
#[cfg(test)]
pub mod test_helpers;
#[cfg(test)]
mod e2e_tests;
#[cfg(test)]
mod perf_tests;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    info!("mysql-mcp starting");

    // Load configuration
    let config = Arc::new(config::merge::load_config()?);
    info!("Configuration loaded");

    // Connect to the database
    let db = Arc::new(db::DbPool::new(config.clone()).await?);
    info!("Database pool created");

    if config.remote.enabled {
        let server = Arc::new(server::McpServer::new(config.clone(), db));
        crate::remote::run_http_server(config, server).await?;
    } else {
        // Create and run the MCP server
        let mcp_server = server::McpServer::new(config, db);
        info!("MCP server starting on stdio");
        mcp_server.run().await?;
    }

    Ok(())
}
