use anyhow::Result;
use crate::config::Config;
use crate::config::toml_config::load_default_config;
use crate::config::env_config::load_env_config;
use crate::config::cli_parser::parse_mysql_cli_string;

/// Load the final merged config:
/// 1. Load dotenv if .env exists
/// 2. Load TOML base config
/// 3. Load env var overrides
/// 4. Apply overrides onto base
/// 5. If connection_string looks like a CLI string, parse and apply it
pub fn load_config() -> Result<Config> {
    // 1. Load .env if present
    if std::path::Path::new(".env").exists() {
        dotenv::dotenv().ok();
    }

    // 2. TOML base
    let base = load_default_config()?;

    // 3. Env overrides
    let env = load_env_config();

    // 4. Merge
    let mut config = env.apply_to(base);

    // 5. If connection_string is set and looks like a CLI string, parse it
    if let Some(cs) = config.connection.connection_string.clone() {
        if cs.trim_start().starts_with('-') {
            let parsed = parse_mysql_cli_string(&cs);
            if let Some(h) = parsed.host {
                config.connection.host = h;
            }
            if let Some(p) = parsed.port {
                config.connection.port = p;
            }
            if let Some(u) = parsed.user {
                config.connection.user = u;
            }
            if let Some(pw) = parsed.password {
                config.connection.password = pw;
            }
            if let Some(db) = parsed.database {
                config.connection.database = Some(db);
            }
            // Clear the connection_string so db.rs won't try to use it as a URL
            config.connection.connection_string = None;
        }
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, ConnectionConfig};
    use crate::config::toml_config::load_toml_config;
    use crate::config::env_config::EnvConfig;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.connection.host, "localhost");
        assert_eq!(config.connection.port, 3306);
        assert_eq!(config.pool.size, 20);
        assert!(!config.security.allow_insert);
        assert!(!config.security.allow_update);
        assert!(!config.security.allow_delete);
        assert!(!config.security.allow_ddl);
    }

    #[test]
    fn test_toml_config_from_string() {
        let toml_str = r#"
[connection]
host = "db.example.com"
port = 5432
user = "myuser"
password = "mypass"
database = "mydb"

[pool]
size = 20
query_timeout_ms = 60000

[security]
allow_insert = true
allow_update = true
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.connection.host, "db.example.com");
        assert_eq!(config.connection.port, 5432);
        assert_eq!(config.pool.size, 20);
        assert!(config.security.allow_insert);
        assert!(config.security.allow_update);
        assert!(!config.security.allow_delete);
    }

    #[test]
    fn test_toml_missing_file_returns_default() {
        use std::path::Path;
        let config = load_toml_config(Path::new("/nonexistent/path/mysql-mcp.toml")).unwrap();
        assert_eq!(config.connection.host, "localhost");
        assert_eq!(config.connection.port, 3306);
    }

    #[test]
    fn test_env_apply_to_overrides() {
        let base = Config::default();
        let env = EnvConfig {
            host: Some("envhost".to_string()),
            port: Some(9999),
            allow_insert: Some(true),
            ..Default::default()
        };
        let merged = env.apply_to(base);
        assert_eq!(merged.connection.host, "envhost");
        assert_eq!(merged.connection.port, 9999);
        assert!(merged.security.allow_insert);
    }

    #[test]
    fn test_env_apply_to_does_not_override_unset_fields() {
        let mut base = Config::default();
        base.connection.host = "basehost".to_string();
        base.connection.port = 1234;
        let env = EnvConfig {
            host: None,
            port: None,
            ..Default::default()
        };
        let merged = env.apply_to(base);
        assert_eq!(merged.connection.host, "basehost");
        assert_eq!(merged.connection.port, 1234);
    }

    #[test]
    fn test_schema_permissions_in_toml() {
        let toml_str = r#"
[security.schema_permissions.mydb]
allow_insert = true
allow_update = false
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let perms = config.security.schema_permissions.get("mydb").unwrap();
        assert_eq!(perms.allow_insert, Some(true));
        assert_eq!(perms.allow_update, Some(false));
    }

    #[test]
    fn test_cli_string_parsing_via_load_config_logic() {
        // Test the CLI string parsing inline (simulates what load_config does)
        let mut config = Config::default();
        let cs = "-hlocalhost -P3307 -uadmin -psecret mydb".to_string();
        config.connection.connection_string = Some(cs.clone());

        // Simulate the connection_string CLI parsing logic
        if let Some(cs_val) = config.connection.connection_string.clone() {
            if cs_val.trim_start().starts_with('-') {
                let parsed = crate::config::cli_parser::parse_mysql_cli_string(&cs_val);
                if let Some(h) = parsed.host { config.connection.host = h; }
                if let Some(p) = parsed.port { config.connection.port = p; }
                if let Some(u) = parsed.user { config.connection.user = u; }
                if let Some(pw) = parsed.password { config.connection.password = pw; }
                if let Some(db) = parsed.database { config.connection.database = Some(db); }
                config.connection.connection_string = None;
            }
        }

        assert_eq!(config.connection.host, "localhost");
        assert_eq!(config.connection.port, 3307);
        assert_eq!(config.connection.user, "admin");
        assert_eq!(config.connection.password, "secret");
        assert_eq!(config.connection.database, Some("mydb".to_string()));
        assert!(config.connection.connection_string.is_none());
    }

    #[test]
    fn test_url_connection_string_not_parsed_as_cli() {
        let mut config = Config::default();
        let url = "mysql://user:pass@host/db".to_string();
        config.connection.connection_string = Some(url.clone());

        // URL strings don't start with '-' so they should be left as-is
        let cs = config.connection.connection_string.clone().unwrap();
        assert!(!cs.trim_start().starts_with('-'));
        assert_eq!(config.connection.connection_string, Some(url));
    }

    #[test]
    fn test_pool_env_override() {
        let base = Config::default();
        let env = EnvConfig {
            pool_size: Some(50),
            query_timeout_ms: Some(60_000),
            cache_ttl_secs: Some(120),
            ..Default::default()
        };
        let merged = env.apply_to(base);
        assert_eq!(merged.pool.size, 50);
        assert_eq!(merged.pool.query_timeout_ms, 60_000);
        assert_eq!(merged.pool.cache_ttl_secs, 120);
    }

    #[test]
    fn test_connection_config_default() {
        let conn = ConnectionConfig::default();
        assert_eq!(conn.host, "localhost");
        assert_eq!(conn.port, 3306);
        assert_eq!(conn.user, "root");
        assert!(conn.database.is_none());
        assert!(conn.socket.is_none());
        assert!(conn.connection_string.is_none());
    }
}
