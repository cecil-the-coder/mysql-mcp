use anyhow::Result;
use std::path::Path;
use crate::config::Config;

/// Load config from a TOML file path. Returns default config if file doesn't exist.
pub fn load_toml_config(path: &Path) -> Result<Config> {
    if !path.exists() {
        return Ok(Config::default());
    }
    let content = std::fs::read_to_string(path)?;
    let config: Config = toml::from_str(&content)?;
    Ok(config)
}

/// Load config from the default location (mysql-mcp.toml) or MCP_CONFIG_FILE env var.
pub fn load_default_config() -> Result<Config> {
    let path = std::env::var("MCP_CONFIG_FILE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("mysql-mcp.toml"));
    load_toml_config(&path)
}
