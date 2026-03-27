//! SQL statement permission checking.
//!
//! This module enforces security policies by validating whether SQL statements
//! are allowed to execute based on the configured permissions. It supports
//! global permission flags (e.g., `allow_insert`) as well as per-schema
//! overrides for fine-grained access control.
//!
//! # Key Functions
//!
//! - [`check_permission`] - Validates a single statement against the security config
//! - [`check_all_permissions`] - Validates multi-table statements against all target schemas
//!
//! # Permission Model
//!
//! - Read operations (SELECT, SHOW, EXPLAIN, SET) are always allowed
//! - Write operations (INSERT, UPDATE, DELETE) require explicit enablement
//! - DDL operations (CREATE, ALTER, DROP, TRUNCATE) require explicit enablement
//! - USE statements are blocked (incompatible with connection pooling)
//!
//! # Example
//!
//! ```ignore
//! use permissions::check_permission;
//! let result = check_permission(&config, &StatementType::Insert, Some("mydb"));
//! ```

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

    // Read-only and informational statements are always allowed.
    // SET is allowed without restriction — operators should be aware that SET SESSION/GLOBAL
    // can affect security/behavior (e.g. sql_mode, global settings).
    match stmt_type {
        StatementType::Select
        | StatementType::Show
        | StatementType::Explain
        | StatementType::Set => return Ok(()),
        StatementType::Use => bail!(
            "USE is not supported with connection pooling — the database change would be lost on the next query. \
             Instead, specify the database in your SQL (e.g., SELECT * FROM mydb.table) or \
             use mysql_connect to create a session with a different default database."
        ),
        _ => {}
    }

    // Write operations: resolve (allowed, label, env_var) from statement type + schema perms.
    if let Some((allowed, label, env_var)) = match stmt_type {
        StatementType::Insert => Some((
            schema_perms
                .and_then(|p| p.allow_insert)
                .unwrap_or(sec.allow_insert),
            "INSERT".to_string(),
            "MYSQL_ALLOW_INSERT",
        )),
        StatementType::Update => Some((
            schema_perms
                .and_then(|p| p.allow_update)
                .unwrap_or(sec.allow_update),
            "UPDATE".to_string(),
            "MYSQL_ALLOW_UPDATE",
        )),
        StatementType::Delete => Some((
            schema_perms
                .and_then(|p| p.allow_delete)
                .unwrap_or(sec.allow_delete),
            "DELETE".to_string(),
            "MYSQL_ALLOW_DELETE",
        )),
        StatementType::Create
        | StatementType::Alter
        | StatementType::Drop
        | StatementType::Truncate => Some((
            schema_perms
                .and_then(|p| p.allow_ddl)
                .unwrap_or(sec.allow_ddl),
            format!("DDL ({})", stmt_type.name()),
            "MYSQL_ALLOW_DDL",
        )),
        _ => None,
    } {
        return check_write_op(allowed, &label, env_var, config, target_schema);
    }

    // Unsupported statement types — provide targeted hints where possible.
    // "Load" uses starts_with to cover both Statement::Load and Statement::LoadData.
    if let StatementType::Other(name) = stmt_type {
        let hint = match name.as_str() {
            "Call" => "CALL (stored procedures) is not supported by this server".to_string(),
            "LockTables" | "UnlockTables" => "LOCK/UNLOCK TABLES is not supported".to_string(),
            "Prepare" | "Execute" | "Deallocate" => {
                "The prepared-statement protocol (PREPARE/EXECUTE/DEALLOCATE) is not supported. Send the final SQL directly".to_string()
            }
            "Do" => "DO is not supported. Use SELECT instead (e.g. SELECT SLEEP(1))".to_string(),
            _ if name.starts_with("Load") => {
                "LOAD DATA is not supported. Use INSERT statements to load data".to_string()
            }
            _ => format!("Unsupported statement type: {name}. Supported types: SELECT, SHOW, EXPLAIN, INSERT, UPDATE, DELETE, CREATE (TABLE/DATABASE/INDEX), ALTER, DROP, TRUNCATE, USE, SET"),
        };
        bail!("{}", hint);
    }

    Ok(())
}

/// Check permissions for ALL target schemas in a parsed statement.
/// For multi-table DELETEs, this checks every referenced schema and fails
/// if ANY schema is denied. For single-schema statements, behaves identically
/// to `check_permission`.
pub fn check_all_permissions(
    config: &Config,
    parsed: &crate::sql_parser::ParsedStatement,
) -> Result<()> {
    if parsed.all_target_schemas.is_empty() {
        // No explicit schema — check with None (falls back to connected DB).
        return check_permission(config, &parsed.statement_type, None);
    }
    for schema in &parsed.all_target_schemas {
        check_permission(config, &parsed.statement_type, Some(schema.as_str()))?;
    }
    Ok(())
}

/// Check a write operation permission.
fn check_write_op(
    allowed: bool,
    op: &str,
    env_var: &str,
    _config: &Config,
    _target_schema: Option<&str>,
) -> Result<()> {
    if !allowed {
        bail!(
            "{} operations are not allowed. Set {}=true to enable",
            op,
            env_var
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
        let sec = SecurityConfig {
            allow_insert: true,
            ..Default::default()
        };
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
    fn test_set_allowed_and_use_denied() {
        let config = Config::default();
        assert!(check_permission(&config, &StatementType::Set, None).is_ok());
        assert!(check_permission(&config, &StatementType::Use, None).is_err());
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
        let sec = SecurityConfig {
            allow_update: true,
            ..Default::default()
        };
        let mut config = config_with_security(sec);
        config.connection.database = Some("testdb".to_string());
        assert!(check_permission(&config, &StatementType::Update, None).is_ok());
    }

    #[test]
    fn test_delete_allowed_when_enabled() {
        let sec = SecurityConfig {
            allow_delete: true,
            ..Default::default()
        };
        let mut config = config_with_security(sec);
        config.connection.database = Some("testdb".to_string());
        assert!(check_permission(&config, &StatementType::Delete, None).is_ok());
    }

    #[test]
    fn test_ddl_allowed_when_enabled() {
        let sec = SecurityConfig {
            allow_ddl: true,
            ..Default::default()
        };
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
    fn test_insert_allowed_without_database_set() {
        let mut config = Config::default();
        // No database set — writes should still work if allow flag is true
        config.security.allow_insert = true;
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

    // --- Tests for check_all_permissions (multi-schema) ---

    fn make_parsed(
        stmt_type: StatementType,
        target_schema: Option<String>,
        all_target_schemas: Vec<String>,
    ) -> crate::sql_parser::ParsedStatement {
        crate::sql_parser::ParsedStatement {
            statement_type: stmt_type,
            target_schema,
            all_target_schemas,
            target_table: None,
            has_limit: false,
            has_where: true,
            has_wildcard: false,
            where_columns: vec![],
            has_leading_wildcard_like: false,
            warnings: vec![],
        }
    }

    #[test]
    fn test_multi_schema_delete_all_allowed() {
        let mut config = Config::default();
        config.security.allow_delete = true;

        // Both schemas allowed globally
        let parsed = make_parsed(
            StatementType::Delete,
            Some("db1".to_string()),
            vec!["db1".to_string(), "db2".to_string()],
        );
        assert!(check_all_permissions(&config, &parsed).is_ok());
    }

    #[test]
    fn test_multi_schema_delete_one_denied() {
        use crate::config::SchemaPermissions;
        let mut config = Config::default();
        config.security.allow_delete = true;

        // Deny delete on db2 specifically
        config.security.schema_permissions.insert(
            "db2".to_string(),
            SchemaPermissions {
                allow_delete: Some(false),
                ..Default::default()
            },
        );
        let parsed = make_parsed(
            StatementType::Delete,
            Some("db1".to_string()),
            vec!["db1".to_string(), "db2".to_string()],
        );
        // Should fail because db2 denies delete
        assert!(check_all_permissions(&config, &parsed).is_err());
    }

    #[test]
    fn test_multi_schema_delete_first_denied_second_allowed() {
        use crate::config::SchemaPermissions;
        let mut config = Config::default();
        config.security.allow_delete = true;

        // Deny delete on db1 specifically
        config.security.schema_permissions.insert(
            "db1".to_string(),
            SchemaPermissions {
                allow_delete: Some(false),
                ..Default::default()
            },
        );
        let parsed = make_parsed(
            StatementType::Delete,
            Some("db1".to_string()),
            vec!["db1".to_string(), "db2".to_string()],
        );
        // Should fail because db1 denies delete (even though db2 allows it)
        assert!(check_all_permissions(&config, &parsed).is_err());
    }

    #[test]
    fn test_multi_schema_empty_schemas_falls_back() {
        // No explicit schemas — should fall back to connected DB check (like single-schema)
        let mut config = Config::default();
        config.connection.database = Some("testdb".to_string());
        config.security.allow_delete = true;
        let parsed = make_parsed(StatementType::Delete, None, vec![]);
        assert!(check_all_permissions(&config, &parsed).is_ok());
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
