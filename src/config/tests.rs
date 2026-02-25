/// Edge case and validation tests for config types.
#[cfg(test)]
mod tests {
    use crate::config::env_config::EnvConfig;
    use crate::config::merge::load_toml_config;
    use crate::config::{Config, ConnectionConfig, SchemaPermissions, SshConfig};

    /// Serializes tests that mutate process-wide environment variables.
    /// Without this, parallel test threads can observe each other's `set_var`/`remove_var`
    /// calls, causing intermittent failures.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
        assert!(base
            .connection
            .connection_string
            .as_deref()
            .unwrap()
            .starts_with("mysql://"));
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
        assert!(config
            .security
            .schema_permissions
            .contains_key("my-database_123"));
        let perms = config
            .security
            .schema_permissions
            .get("my-database_123")
            .unwrap();
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
        assert_eq!(config.pool.retry_attempts, 2); // default is 2 retries
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
        assert!(
            !config.security.allow_runtime_connections,
            "allow_runtime_connections must default to false (security)"
        );
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
        assert!(
            config.validate().is_err(),
            "max_rows=0 should fail validation"
        );
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("max_rows"),
            "error should mention max_rows"
        );
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
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
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
        assert!(
            !config.pool.readonly_transaction,
            "readonly_transaction should default to false (prefer 1-RTT path)"
        );
    }

    // Test: pool size defaults to 20
    #[test]
    fn test_pool_size_default() {
        let config = Config::default();
        assert_eq!(config.pool.size, 20, "pool size should default to 20");
    }

    // Test: TOML string with connection, pool, and security fields parses correctly
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

    // Test: load_toml_config returns default config when file does not exist
    #[test]
    fn test_toml_missing_file_returns_default() {
        use std::path::Path;
        let config = load_toml_config(Path::new("/nonexistent/path/mysql-mcp.toml")).unwrap();
        assert_eq!(config.connection.host, "localhost");
        assert_eq!(config.connection.port, 3306);
    }

    // Test: EnvConfig overrides base config fields when set
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

    // Test: EnvConfig does not override base config fields when None
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

    // Test: single-database schema_permissions parses allow_insert and allow_update
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

    // Test: URL connection string is preserved as-is and does not look like a CLI flag
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

    // Test: EnvConfig overrides pool size, query timeout, cache TTL, and retry_attempts
    #[test]
    fn test_pool_env_override() {
        let base = Config::default();
        let env = EnvConfig {
            pool_size: Some(50),
            query_timeout_ms: Some(60_000),
            cache_ttl_secs: Some(120),
            retry_attempts: Some(5),
            ..Default::default()
        };
        let merged = env.apply_to(base);
        assert_eq!(merged.pool.size, 50);
        assert_eq!(merged.pool.query_timeout_ms, 60_000);
        assert_eq!(merged.pool.cache_ttl_secs, 120);
        assert_eq!(merged.pool.retry_attempts, 5);
    }

    // Test: ConnectionConfig default fields match expected values
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

    // SSH config tests
    #[test]
    fn test_ssh_config_default() {
        let config = Config::default();
        assert!(config.ssh.is_none(), "SSH config should be None by default");
    }

    #[test]
    fn test_ssh_config_from_toml() {
        let toml_str = r#"
[ssh]
host = "bastion.example.com"
user = "ubuntu"
private_key = "/tmp/key.pem"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let ssh = config.ssh.as_ref().expect("ssh should be Some");
        assert_eq!(ssh.host, "bastion.example.com");
        assert_eq!(ssh.user, "ubuntu");
        assert_eq!(ssh.port, 22); // default
        assert_eq!(ssh.known_hosts_check, "strict"); // default
    }

    #[test]
    fn test_ssh_config_validate_empty_host() {
        let mut config = Config::default();
        config.ssh = Some(SshConfig {
            host: String::new(),
            user: "user".to_string(),
            ..Default::default()
        });
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("ssh.host"));
    }

    #[test]
    fn test_ssh_config_validate_empty_user() {
        let mut config = Config::default();
        config.ssh = Some(SshConfig {
            host: "bastion".to_string(),
            user: String::new(),
            ..Default::default()
        });
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("ssh.user"));
    }

    #[test]
    fn test_ssh_config_validate_invalid_known_hosts_check() {
        let mut config = Config::default();
        config.ssh = Some(SshConfig {
            host: "bastion".to_string(),
            user: "ubuntu".to_string(),
            known_hosts_check: "banana".to_string(),
            ..Default::default()
        });
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("known_hosts_check"));
    }

    #[test]
    fn test_ssh_config_validate_valid_known_hosts_values() {
        for val in &["strict", "accept-new", "insecure"] {
            let mut config = Config::default();
            config.ssh = Some(SshConfig {
                host: "bastion".to_string(),
                user: "ubuntu".to_string(),
                known_hosts_check: val.to_string(),
                ..Default::default()
            });
            // Should not fail on known_hosts_check (private_key not set, path check skipped)
            let result = config.validate();
            // It passes validation for known_hosts_check itself
            assert!(
                !result
                    .as_ref()
                    .is_err_and(|e| e.to_string().contains("known_hosts_check")),
                "known_hosts_check='{}' should be valid",
                val
            );
        }
    }

    #[test]
    fn test_ssh_env_vars_override() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        use crate::config::env_config::load_env_config;
        std::env::set_var("MYSQL_SSH_HOST", "mybastion");
        std::env::set_var("MYSQL_SSH_USER", "ubuntu");
        std::env::set_var("MYSQL_SSH_PORT", "2222");
        std::env::set_var("MYSQL_SSH_KNOWN_HOSTS_CHECK", "accept-new");
        let env = load_env_config();
        let config = env.apply_to(Config::default());
        std::env::remove_var("MYSQL_SSH_HOST");
        std::env::remove_var("MYSQL_SSH_USER");
        std::env::remove_var("MYSQL_SSH_PORT");
        std::env::remove_var("MYSQL_SSH_KNOWN_HOSTS_CHECK");

        let ssh = config
            .ssh
            .expect("ssh config should be Some when env vars set");
        assert_eq!(ssh.host, "mybastion");
        assert_eq!(ssh.user, "ubuntu");
        assert_eq!(ssh.port, 2222);
        assert_eq!(ssh.known_hosts_check, "accept-new");
    }

    // Test: pool.size = 1000 is the valid upper boundary
    #[test]
    fn test_pool_size_1000_is_valid() {
        let mut config = Config::default();
        config.pool.size = 1000;
        assert!(
            config.validate().is_ok(),
            "pool.size=1000 should be the valid upper bound"
        );
    }

    // Test: pool.size = 1001 exceeds the cap and fails validation
    #[test]
    fn test_pool_size_1001_fails_validation() {
        let mut config = Config::default();
        config.pool.size = 1001;
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("pool.size"),
            "error should mention pool.size, got: {}",
            err
        );
    }

    // Test: cache_ttl_secs=0 passes validation (it warns but is intentionally allowed)
    #[test]
    fn test_cache_ttl_secs_zero_is_valid() {
        let mut config = Config::default();
        config.pool.cache_ttl_secs = 0;
        assert!(
            config.validate().is_ok(),
            "cache_ttl_secs=0 should be allowed (disables cache with a warning)"
        );
    }

    #[test]
    fn test_no_ssh_env_vars_means_none() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        use crate::config::env_config::load_env_config;
        // Ensure none of the SSH vars are set
        for key in &[
            "MYSQL_SSH_HOST",
            "MYSQL_SSH_USER",
            "MYSQL_SSH_PORT",
            "MYSQL_SSH_PRIVATE_KEY",
            "MYSQL_SSH_KNOWN_HOSTS_CHECK",
            "MYSQL_SSH_KNOWN_HOSTS_FILE",
        ] {
            std::env::remove_var(key);
        }
        let env = load_env_config();
        let config = env.apply_to(Config::default());
        assert!(
            config.ssh.is_none(),
            "ssh should remain None when no MYSQL_SSH_* vars set"
        );
    }
}
