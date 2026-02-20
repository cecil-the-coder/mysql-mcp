use anyhow::{bail, Result};
use crate::config::Config;
use crate::sql_parser::StatementType;

/// Check if a SQL statement is allowed based on config.
/// Returns Ok(()) if allowed, Err with descriptive message if denied.
pub fn check_permission(
    config: &Config,
    stmt_type: &StatementType,
    target_schema: Option<&str>,
) -> Result<()> {
    let sec = &config.security;

    // Get schema-specific permissions if we have a target schema
    let schema_perms = target_schema
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
            if !allowed {
                bail!("INSERT operations are not allowed. Set ALLOW_INSERT_OPERATION=true to enable.");
            }
            check_multi_db_write(config, target_schema)?;
            Ok(())
        }
        StatementType::Update => {
            let allowed = schema_perms
                .and_then(|p| p.allow_update)
                .unwrap_or(sec.allow_update);
            if !allowed {
                bail!("UPDATE operations are not allowed. Set ALLOW_UPDATE_OPERATION=true to enable.");
            }
            check_multi_db_write(config, target_schema)?;
            Ok(())
        }
        StatementType::Delete => {
            let allowed = schema_perms
                .and_then(|p| p.allow_delete)
                .unwrap_or(sec.allow_delete);
            if !allowed {
                bail!("DELETE operations are not allowed. Set ALLOW_DELETE_OPERATION=true to enable.");
            }
            check_multi_db_write(config, target_schema)?;
            Ok(())
        }
        StatementType::Create | StatementType::Alter | StatementType::Drop | StatementType::Truncate => {
            let allowed = schema_perms
                .and_then(|p| p.allow_ddl)
                .unwrap_or(sec.allow_ddl);
            if !allowed {
                bail!("DDL operations ({}) are not allowed. Set ALLOW_DDL_OPERATION=true to enable.", stmt_type.name());
            }
            check_multi_db_write(config, target_schema)?;
            Ok(())
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
            bail!("Unsupported statement type: {}. Only SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, TRUNCATE, SHOW, USE are supported.", name);
        }
    }
}

/// In multi-DB mode, check if writes are allowed.
fn check_multi_db_write(config: &Config, target_schema: Option<&str>) -> Result<()> {
    // If we're in single-DB mode (database is set), no additional check needed
    if config.connection.database.is_some() {
        return Ok(());
    }
    // Multi-DB mode: check MULTI_DB_WRITE_MODE
    if !config.security.multi_db_write_mode {
        bail!(
            "Write operations on schema '{}' are not allowed in multi-database mode. Set MULTI_DB_WRITE_MODE=true to enable writes.",
            target_schema.unwrap_or("<unknown>")
        );
    }
    Ok(())
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
        let err = check_permission(&config, &StatementType::Other("CALL".to_string()), None).unwrap_err();
        assert!(err.to_string().contains("Unsupported statement type"));
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
            SchemaPermissions { allow_insert: Some(true), ..Default::default() }
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
            SchemaPermissions { allow_insert: Some(false), ..Default::default() }
        );
        // Global allows, but schema-specific denies
        assert!(check_permission(&config, &StatementType::Insert, Some("restricted_schema")).is_err());
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
}
