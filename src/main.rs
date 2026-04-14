use anyhow::Result;
use std::sync::Arc;
use tracing::info;
use clap::Parser;

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

/// CLI arguments for the mysql-mcp server.
#[derive(Parser, Debug)]
#[command(name = "mysql-mcp")]
#[command(about = "MySQL MCP server - expose MySQL databases via the Model Context Protocol")]
#[command(version)]
struct CliArgs {
    /// Path to the TOML configuration file
    #[arg(short, long, value_name = "PATH")]
    config: Option<std::path::PathBuf>,

    /// Print the effective configuration and exit
    #[arg(long)]
    print_config: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments before initializing tracing to avoid
    // interfering with JSON-RPC transport on stdout
    let args = CliArgs::parse();

    // Handle --print-config early (before logging setup)
    if args.print_config {
        let config = load_and_merge_config(args.config.as_deref())?;
        // Print config to stdout as formatted TOML
        println!("{}", toml::to_string_pretty(&config)?);
        return Ok(());
    }

    // Initialize tracing — MUST write to stderr, not stdout.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    info!("sql-mcp starting");

    // Load configuration
    let raw_config = load_and_merge_config(args.config.as_deref())?;
    raw_config.validate()?;
    let config = Arc::new(raw_config);
    info!("Configuration loaded");

    // Create the appropriate backend based on config
    let backend = backend::create_backend(&config.backend);
    info!("Using {} backend", backend.backend_name());

    // Connect to the database (with optional SSH tunnel for the default session)
    let (db, _default_tunnel) = if let Some(ref ssh) = config.ssh {
        info!("SSH tunnel configured, establishing tunnel to {}", ssh.host);
        let tunnel = tunnel::spawn_ssh_tunnel(
            ssh,
            &config.connection.host,
            config.connection.port.unwrap_or(backend.default_port()),
        )
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

/// Load configuration from all sources: dotenv -> TOML file -> env var overrides.
/// The optional `cli_config_path` is used when --config is passed on the command line,
/// taking precedence over the MCP_CONFIG_FILE environment variable.
fn load_and_merge_config(cli_config_path: Option<&std::path::Path>) -> anyhow::Result<config::Config> {
    // Load .env file if present (must happen before any env var reads)
    if std::path::Path::new(".env").exists() {
        if let Err(e) = dotenv::dotenv() {
            eprintln!("Warning: failed to parse .env file: {}", e);
        }
    }

    // Determine which TOML config file to use (CLI arg > env var > default)
    let toml_path = cli_config_path
        .map(|p| p.to_path_buf())
        .or_else(|| {
            std::env::var("MCP_CONFIG_FILE")
                .ok()
                .map(std::path::PathBuf::from)
        })
        .unwrap_or_else(|| std::path::PathBuf::from("mysql-mcp.toml"));

    let base = if toml_path.exists() {
        let content = std::fs::read_to_string(&toml_path)?;
        toml::from_str(&content)?
    } else {
        config::Config::default()
    };

    // Apply environment variable overrides
    Ok(config::env_config::load_env_config().apply_to(base))
}
