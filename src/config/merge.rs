use crate::config::env_config::load_env_config;
use crate::config::Config;
use anyhow::Result;
use std::path::Path;

/// Load config from a TOML file path. Returns default config if file doesn't exist.
pub(crate) fn load_toml_config(path: &Path) -> Result<Config> {
    if !path.exists() {
        return Ok(Config::default());
    }
    let content = std::fs::read_to_string(path)?;
    let config: Config = toml::from_str(&content)?;
    Ok(config)
}

/// Load the final merged config:
/// 1. Load dotenv if .env exists
/// 2. Load TOML base config
/// 3. Load env var overrides
/// 4. Apply overrides onto base
pub fn load_config() -> Result<Config> {
    // 1. Load .env if present (log parse errors rather than silencing them)
    if std::path::Path::new(".env").exists() {
        if let Err(e) = dotenv::dotenv() {
            eprintln!("Warning: failed to parse .env file: {}", e);
        }
    }

    // 2. TOML base (MCP_CONFIG_FILE env var or default mysql-mcp.toml)
    let path = std::env::var("MCP_CONFIG_FILE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("mysql-mcp.toml"));
    let base = load_toml_config(&path)?;

    // 3. Env overrides
    let env = load_env_config();

    // 4. Merge
    let config = env.apply_to(base);

    Ok(config)
}
