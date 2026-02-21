/// Edge case and validation tests for config types.

#[cfg(test)]
mod tests {
    use crate::config::{Config, ConnectionConfig, SchemaPermissions};

    // Test: connection string URL format is preserved
    #[test]
    fn test_connection_string_url_preserved() {
        let base = Config {
            connection: ConnectionConfig {
                connection_string: Some("mysql://user:pass@host/db".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(base.connection.connection_string.as_deref().unwrap().starts_with("mysql://"));
    }

    // Test: ALLOW_DDL config is settable
    #[test]
    fn test_allow_ddl_config() {
        let mut config = Config::default();
        config.security.allow_ddl = true;
        // Config itself is valid - the warning would happen at runtime
        assert!(config.security.allow_ddl);
    }

    // Test: schema permission keys with unusual database names
    #[test]
    fn test_schema_permissions_unusual_names() {
        let toml_str = r#"
[security.schema_permissions."my-database_123"]
allow_insert = true
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.security.schema_permissions.contains_key("my-database_123"));
        let perms = config.security.schema_permissions.get("my-database_123").unwrap();
        assert_eq!(perms.allow_insert, Some(true));
    }

    // Test: empty MYSQL_DB means no database set (multi-DB mode)
    #[test]
    fn test_empty_database_is_none() {
        let base = Config::default();
        assert!(base.connection.database.is_none());
    }

    // Test: pool defaults are reasonable
    #[test]
    fn test_pool_defaults() {
        let config = Config::default();
        assert!(config.pool.size > 0);
        assert!(config.pool.query_timeout_ms > 0);
        assert!(config.pool.cache_ttl_secs > 0);
    }

    // Test: SchemaPermissions default has all None
    #[test]
    fn test_schema_permissions_default_all_none() {
        let perms = SchemaPermissions::default();
        assert!(perms.allow_insert.is_none());
        assert!(perms.allow_update.is_none());
        assert!(perms.allow_delete.is_none());
        assert!(perms.allow_ddl.is_none());
    }

    // Test: security defaults are all false
    #[test]
    fn test_security_defaults_all_false() {
        let config = Config::default();
        assert!(!config.security.allow_insert);
        assert!(!config.security.allow_update);
        assert!(!config.security.allow_delete);
        assert!(!config.security.allow_ddl);
        assert!(!config.security.ssl);
        assert!(!config.security.multi_db_write_mode);
        assert!(config.security.schema_permissions.is_empty());
    }

    // Test: multiple schema permissions in a single toml
    #[test]
    fn test_multiple_schema_permissions_in_toml() {
        let toml_str = r#"
[security.schema_permissions.db1]
allow_insert = true
allow_update = true

[security.schema_permissions.db2]
allow_delete = true
allow_ddl = false
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let db1 = config.security.schema_permissions.get("db1").unwrap();
        assert_eq!(db1.allow_insert, Some(true));
        assert_eq!(db1.allow_update, Some(true));
        assert!(db1.allow_delete.is_none());

        let db2 = config.security.schema_permissions.get("db2").unwrap();
        assert_eq!(db2.allow_delete, Some(true));
        assert_eq!(db2.allow_ddl, Some(false));
    }

    // Test: config with all security flags enabled via TOML
    #[test]
    fn test_full_security_enabled_toml() {
        let toml_str = r#"
[security]
allow_insert = true
allow_update = true
allow_delete = true
allow_ddl = true
ssl = true
multi_db_write_mode = true
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.security.allow_insert);
        assert!(config.security.allow_update);
        assert!(config.security.allow_delete);
        assert!(config.security.allow_ddl);
        assert!(config.security.ssl);
        assert!(config.security.multi_db_write_mode);
    }

    // Test: date_strings and timezone defaults
    #[test]
    fn test_date_strings_and_timezone_defaults() {
        let config = Config::default();
        assert!(!config.date_strings);
        assert!(config.timezone.is_none());
    }

    // Test: partial toml (missing sections fill with defaults)
    #[test]
    fn test_partial_toml_fills_defaults() {
        let toml_str = r#"
[connection]
host = "myhost"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.connection.host, "myhost");
        // Defaults for everything else
        assert_eq!(config.connection.port, 3306);
        assert_eq!(config.pool.size, 20);
        assert!(!config.security.allow_insert);
    }

    // Test: allow_runtime_connections defaults to false
    #[test]
    fn test_allow_runtime_connections_default_false() {
        let config = Config::default();
        assert!(!config.security.allow_runtime_connections,
            "allow_runtime_connections must default to false (security)");
    }

    // Test: max_sessions defaults to 50
    #[test]
    fn test_max_sessions_default() {
        let config = Config::default();
        assert_eq!(config.security.max_sessions, 50);
    }

    // Test: max_rows=0 fails validation and error mentions max_rows
    #[test]
    fn test_max_rows_zero_fails_validation() {
        let mut config = Config::default();
        config.pool.max_rows = 0;
        assert!(config.validate().is_err(), "max_rows=0 should fail validation");
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_rows"), "error should mention max_rows");
    }

    // Test: warmup_connections > pool size fails validation
    #[test]
    fn test_warmup_connections_exceeds_pool_size_fails() {
        let mut config = Config::default();
        config.pool.warmup_connections = config.pool.size + 1;
        assert!(config.validate().is_err());
    }

    // Test: max_sessions=0 fails validation
    #[test]
    fn test_max_sessions_zero_fails_validation() {
        let mut config = Config::default();
        config.security.max_sessions = 0;
        assert!(config.validate().is_err());
    }

    // Test: MYSQL_MAX_SESSIONS env var overrides max_sessions
    #[test]
    fn test_env_max_sessions_override() {
        use crate::config::env_config::load_env_config;

        std::env::set_var("MYSQL_MAX_SESSIONS", "5");
        let env = load_env_config();
        let config = env.apply_to(Config::default());
        std::env::remove_var("MYSQL_MAX_SESSIONS");

        assert_eq!(config.security.max_sessions, 5);
    }

    // Test: readonly_transaction defaults to false
    #[test]
    fn test_readonly_transaction_default() {
        let config = Config::default();
        assert!(!config.pool.readonly_transaction,
            "readonly_transaction should default to false (prefer 1-RTT path)");
    }

    // Test: pool size defaults to 20
    #[test]
    fn test_pool_size_default() {
        let config = Config::default();
        assert_eq!(config.pool.size, 20, "pool size should default to 20");
    }
}
