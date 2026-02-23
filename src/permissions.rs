use crate::config::Config;
use crate::sql_parser::StatementType;
use anyhow::{bail, Result};

/// Check if a SQL statement is allowed based on config.
/// Returns Ok(()) if allowed, Err with descriptive message if denied.
pub fn check_permission(
    config: &Config,
    stmt_type: &StatementType,
    target_schema: Option<&str>,
) -> Result<()> {
    let sec = &config.security;

    // Resolve the effective schema for permission lookup:
    // - Use the explicit target_schema from the SQL (e.g., `mcp_test.users` -> `mcp_test`).
    // - Fall back to config.connection.database in single-DB mode (unqualified SQL like
    //   `INSERT INTO users ...` implicitly targets the connected database).
    // Keys in schema_permissions are stored lowercase (env var parser lowercases them),
    // so normalise to lowercase for the lookup.
    let effective_schema: Option<String> = target_schema.map(|s| s.to_lowercase()).or_else(|| {
        config
            .connection
            .database
            .as_ref()
            .map(|d| d.to_lowercase())
    });

    // Get schema-specific permissions if we have an effective schema
    let schema_perms = effective_schema
        .as_deref()
        .and_then(|s| sec.schema_permissions.get(s));

    match stmt_type {
        StatementType::Select | StatementType::Show | StatementType::Explain => {
            // Always allowed (read-only)
            Ok(())
        }
        StatementType::Insert => {
            let allowed = schema_perms
                .and_then(|p| p.allow_insert)
                .unwrap_or(sec.allow_insert);
            check_write_op(
                allowed,
                "INSERT",
                "MYSQL_ALLOW_INSERT",
                config,
                target_schema,
            )
        }
        StatementType::Update => {
            let allowed = schema_perms
                .and_then(|p| p.allow_update)
                .unwrap_or(sec.allow_update);
            check_write_op(
                allowed,
                "UPDATE",
                "MYSQL_ALLOW_UPDATE",
                config,
                target_schema,
            )
        }
        StatementType::Delete => {
            let allowed = schema_perms
                .and_then(|p| p.allow_delete)
                .unwrap_or(sec.allow_delete);
            check_write_op(
                allowed,
                "DELETE",
                "MYSQL_ALLOW_DELETE",
                config,
                target_schema,
            )
        }
        StatementType::Create
        | StatementType::Alter
        | StatementType::Drop
        | StatementType::Truncate => {
            let allowed = schema_perms
                .and_then(|p| p.allow_ddl)
                .unwrap_or(sec.allow_ddl);
            let label = format!("DDL ({})", stmt_type.name());
            check_write_op(allowed, &label, "MYSQL_ALLOW_DDL", config, target_schema)
        }
        StatementType::Use => {
            // USE is informational, allow it
            Ok(())
        }
        StatementType::Set => {
            // SET is informational
            Ok(())
        }
        StatementType::Other(name) => {
            let hint = if name.contains("Call") {
                "CALL (stored procedures) is not supported by this server.".to_string()
            } else if name.contains("Load") {
                "LOAD DATA is not supported. Use INSERT statements to load data.".to_string()
            } else if name.contains("LockTables") || name.contains("UnlockTables") {
                "LOCK/UNLOCK TABLES is not supported.".to_string()
            } else if name.contains("Prepare")
                || name.contains("Execute")
                || name.contains("Deallocate")
            {
                "The prepared-statement protocol (PREPARE/EXECUTE/DEALLOCATE) is not supported. Send the final SQL directly."
                    .to_string()
            } else if name.contains("Do") {
                "DO is not supported. Use SELECT instead (e.g. SELECT SLEEP(1)).".to_string()
            } else {
                format!("Unsupported statement type: {name}. Supported types: SELECT, SHOW, EXPLAIN, INSERT, UPDATE, DELETE, CREATE (TABLE/DATABASE), ALTER, DROP, TRUNCATE, USE. Note: CREATE INDEX, CREATE VIEW, and similar variants are not supported.")
            };
            bail!("{}", hint);
        }
    }
}

/// In multi-DB mode, check if writes are allowed.
fn check_multi_db_write(config: &Config, target_schema: Option<&str>) -> Result<()> {
    // If we're in single-DB mode (database is set), no additional check needed
    if config.connection.database.is_some() {
        return Ok(());
    }
    // Multi-DB mode: check MYSQL_MULTI_DB_WRITE_MODE
    if !config.security.multi_db_write_mode {
        bail!(
            "Write operations on schema '{}' are not allowed in multi-database mode. Set MYSQL_MULTI_DB_WRITE_MODE=true to enable writes.",
            target_schema.unwrap_or("<unknown>")
        );
    }
    Ok(())
}

/// Check a write operation permission and enforce multi-DB write rules.
fn check_write_op(
    allowed: bool,
    op: &str,
    env_var: &str,
    config: &Config,
    target_schema: Option<&str>,
) -> Result<()> {
    if !allowed {
        bail!(
            "{} operations are not allowed. Set {}=true to enable.",
            op,
            env_var
        );
    }
    check_multi_db_write(config, target_schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, SecurityConfig};

    fn config_with_security(sec: SecurityConfig) -> Config {
        Config {
            security: sec,
            ..Config::default()
        }
    }

    #[test]
    fn test_select_always_allowed() {
        let config = Config::default();
        assert!(check_permission(&config, &StatementType::Select, None).is_ok());
        assert!(check_permission(&config, &StatementType::Show, None).is_ok());
        assert!(check_permission(&config, &StatementType::Explain, None).is_ok());
    }

    #[test]
    fn test_insert_denied_by_default() {
        let config = Config::default();
        assert!(check_permission(&config, &StatementType::Insert, None).is_err());
    }

    #[test]
    fn test_insert_allowed_when_enabled() {
        let mut sec = SecurityConfig::default();
        sec.allow_insert = true;
        // single-DB mode: database is set
        let mut config = config_with_security(sec);
        config.connection.database = Some("mydb".to_string());
        assert!(check_permission(&config, &StatementType::Insert, None).is_ok());
    }

    #[test]
    fn test_ddl_denied_by_default() {
        let config = Config::default();
        assert!(check_permission(&config, &StatementType::Create, None).is_err());
        assert!(check_permission(&config, &StatementType::Alter, None).is_err());
        assert!(check_permission(&config, &StatementType::Drop, None).is_err());
        assert!(check_permission(&config, &StatementType::Truncate, None).is_err());
    }

    #[test]
    fn test_multi_db_write_blocked() {
        let mut sec = SecurityConfig::default();
        sec.allow_insert = true;
        // No database set = multi-DB mode, multi_db_write_mode = false
        let config = config_with_security(sec);
        let err = check_permission(&config, &StatementType::Insert, Some("mydb")).unwrap_err();
        assert!(err.to_string().contains("multi-database mode"));
    }

    #[test]
    fn test_multi_db_write_allowed_when_flag_set() {
        let mut sec = SecurityConfig::default();
        sec.allow_insert = true;
        sec.multi_db_write_mode = true;
        let config = config_with_security(sec);
        assert!(check_permission(&config, &StatementType::Insert, Some("mydb")).is_ok());
    }

    #[test]
    fn test_use_and_set_always_allowed() {
        let config = Config::default();
        assert!(check_permission(&config, &StatementType::Use, None).is_ok());
        assert!(check_permission(&config, &StatementType::Set, None).is_ok());
    }

    #[test]
    fn test_other_always_denied() {
        let config = Config::default();
        let err = check_permission(
            &config,
            &StatementType::Other("SomeUnknownStatement".to_string()),
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Unsupported statement type"));
    }

    #[test]
    fn test_other_call_gives_helpful_message() {
        let config = Config::default();
        let err =
            check_permission(&config, &StatementType::Other("Call".to_string()), None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("stored procedures") || msg.contains("CALL"),
            "expected stored-procedure hint, got: {msg}"
        );
    }

    #[test]
    fn test_other_lock_tables_gives_helpful_message() {
        let config = Config::default();
        let err = check_permission(
            &config,
            &StatementType::Other("LockTables".to_string()),
            None,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("LOCK") || msg.contains("UNLOCK"),
            "expected LOCK/UNLOCK hint, got: {msg}"
        );
    }

    #[test]
    fn test_other_load_gives_helpful_message() {
        let config = Config::default();
        let err = check_permission(&config, &StatementType::Other("LoadData".to_string()), None)
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("INSERT") || msg.contains("LOAD"),
            "expected INSERT/LOAD hint, got: {msg}"
        );
    }

    fn config_with_inserts() -> Config {
        let mut c = Config::default();
        c.connection.database = Some("testdb".to_string()); // single-DB mode
        c.security.allow_insert = true;
        c
    }

    #[test]
    fn test_insert_allowed_when_enabled_single_db() {
        let config = config_with_inserts();
        assert!(check_permission(&config, &StatementType::Insert, None).is_ok());
    }

    #[test]
    fn test_update_denied_by_default() {
        let config = Config::default();
        assert!(check_permission(&config, &StatementType::Update, None).is_err());
    }

    #[test]
    fn test_delete_denied_by_default() {
        let config = Config::default();
        assert!(check_permission(&config, &StatementType::Delete, None).is_err());
    }

    #[test]
    fn test_update_allowed_when_enabled() {
        let mut sec = SecurityConfig::default();
        sec.allow_update = true;
        let mut config = config_with_security(sec);
        config.connection.database = Some("testdb".to_string());
        assert!(check_permission(&config, &StatementType::Update, None).is_ok());
    }

    #[test]
    fn test_delete_allowed_when_enabled() {
        let mut sec = SecurityConfig::default();
        sec.allow_delete = true;
        let mut config = config_with_security(sec);
        config.connection.database = Some("testdb".to_string());
        assert!(check_permission(&config, &StatementType::Delete, None).is_ok());
    }

    #[test]
    fn test_ddl_allowed_when_enabled() {
        let mut sec = SecurityConfig::default();
        sec.allow_ddl = true;
        let mut config = config_with_security(sec);
        config.connection.database = Some("testdb".to_string());
        assert!(check_permission(&config, &StatementType::Create, None).is_ok());
        assert!(check_permission(&config, &StatementType::Drop, None).is_ok());
        assert!(check_permission(&config, &StatementType::Alter, None).is_ok());
        assert!(check_permission(&config, &StatementType::Truncate, None).is_ok());
    }

    #[test]
    fn test_schema_specific_override() {
        use crate::config::SchemaPermissions;
        let mut config = Config::default();
        config.connection.database = Some("testdb".to_string());
        config.security.allow_insert = false;
        config.security.schema_permissions.insert(
            "allowed_schema".to_string(),
            SchemaPermissions {
                allow_insert: Some(true),
                ..Default::default()
            },
        );
        // Denied for other schemas (falls back to global allow_insert = false)
        assert!(check_permission(&config, &StatementType::Insert, Some("denied_schema")).is_err());
        // Allowed for allowed_schema (schema override = true)
        assert!(check_permission(&config, &StatementType::Insert, Some("allowed_schema")).is_ok());
    }

    #[test]
    fn test_schema_override_deny() {
        use crate::config::SchemaPermissions;
        let mut config = Config::default();
        config.connection.database = Some("testdb".to_string());
        config.security.allow_insert = true;
        config.security.schema_permissions.insert(
            "restricted_schema".to_string(),
            SchemaPermissions {
                allow_insert: Some(false),
                ..Default::default()
            },
        );
        // Global allows, but schema-specific denies
        assert!(
            check_permission(&config, &StatementType::Insert, Some("restricted_schema")).is_err()
        );
        // Other schema falls back to global allow
        assert!(check_permission(&config, &StatementType::Insert, Some("other_schema")).is_ok());
    }

    #[test]
    fn test_multi_db_write_requires_flag() {
        let mut config = Config::default();
        // No database set = multi-DB mode
        config.security.allow_insert = true;
        config.security.multi_db_write_mode = false;
        // Write in multi-DB mode without flag should fail
        assert!(check_permission(&config, &StatementType::Insert, Some("anydb")).is_err());
    }

    #[test]
    fn test_multi_db_write_allowed_with_flag() {
        let mut config = Config::default();
        config.security.allow_insert = true;
        config.security.multi_db_write_mode = true;
        assert!(check_permission(&config, &StatementType::Insert, Some("anydb")).is_ok());
    }

    #[test]
    fn test_error_messages_contain_useful_info() {
        let config = Config::default();
        let err = check_permission(&config, &StatementType::Insert, None).unwrap_err();
        assert!(err.to_string().contains("INSERT"));

        let err = check_permission(&config, &StatementType::Update, None).unwrap_err();
        assert!(err.to_string().contains("UPDATE"));

        let err = check_permission(&config, &StatementType::Delete, None).unwrap_err();
        assert!(err.to_string().contains("DELETE"));
    }

    #[test]
    fn test_multi_db_error_message() {
        let mut config = Config::default();
        config.security.allow_insert = true;
        // No database = multi-DB mode, flag not set
        let err = check_permission(&config, &StatementType::Insert, Some("mydb")).unwrap_err();
        assert!(err.to_string().contains("multi-database mode"));
    }

    // --- New tests for the schema-fallback and case-insensitivity fixes ---

    #[test]
    fn test_schema_override_via_connected_db_unqualified_sql() {
        // When SQL is unqualified (target_schema = None) and we're in single-DB mode,
        // the connected database name should be used for schema permission lookup.
        use crate::config::SchemaPermissions;
        let mut config = Config::default();
        config.connection.database = Some("mcp_test".to_string());
        config.security.allow_insert = false; // global: deny
        config.security.schema_permissions.insert(
            "mcp_test".to_string(),
            SchemaPermissions {
                allow_insert: Some(true),
                ..Default::default()
            },
        );
        // target_schema = None (unqualified SQL), but connected DB = mcp_test -> override allows
        assert!(check_permission(&config, &StatementType::Insert, None).is_ok());
    }

    #[test]
    fn test_schema_override_deny_via_connected_db_unqualified_sql() {
        // Global allows insert, but schema override denies it; unqualified SQL should still be denied.
        use crate::config::SchemaPermissions;
        let mut config = Config::default();
        config.connection.database = Some("mcp_test".to_string());
        config.security.allow_insert = true; // global: allow
        config.security.schema_permissions.insert(
            "mcp_test".to_string(),
            SchemaPermissions {
                allow_insert: Some(false),
                ..Default::default()
            },
        );
        // target_schema = None (unqualified SQL), but connected DB = mcp_test -> override denies
        assert!(check_permission(&config, &StatementType::Insert, None).is_err());
    }

    #[test]
    fn test_schema_lookup_is_case_insensitive() {
        // Keys in schema_permissions are stored lowercase; SQL may extract mixed case.
        use crate::config::SchemaPermissions;
        let mut config = Config::default();
        config.connection.database = Some("testdb".to_string());
        config.security.allow_insert = false;
        // Key stored lowercase (as the env var parser does)
        config.security.schema_permissions.insert(
            "myschema".to_string(),
            SchemaPermissions {
                allow_insert: Some(true),
                ..Default::default()
            },
        );
        // SQL-extracted schema in mixed case should still match
        assert!(check_permission(&config, &StatementType::Insert, Some("MySchema")).is_ok());
        assert!(check_permission(&config, &StatementType::Insert, Some("MYSCHEMA")).is_ok());
    }
}
