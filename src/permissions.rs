use crate::config::Config;
use crate::sql_parser::StatementType;
use anyhow::{bail, Result};

/// Check if a SQL statement is allowed based on config.
///
/// Returns `Ok(())` if allowed, `Err` with a descriptive message if denied.
///
/// # Permission Resolution Order
///
/// 1. **Schema-specific override**: If `target_schema` matches a key in
///    `config.security.schema_permissions`, that schema's permission flags are used.
/// 2. **Global fallback**: If no schema-specific override exists, the global
///    `config.security.allow_*` flags are used.
///
/// Schema lookup is case-insensitive (keys are stored lowercase).
///
/// # Schema Resolution
///
/// The `target_schema` parameter determines which schema's permissions to check:
/// - If provided (e.g., from qualified table references like `mydb.users`), that schema is used.
/// - If `None`, falls back to `config.connection.database` for single-database mode.
/// - If neither is available, only global permissions apply.
///
/// # Statement Type Categories
///
/// ## Always Allowed (no configuration needed)
/// - `SELECT`, `SHOW`, `EXPLAIN`: Read-only operations
/// - `SET`: Session/local variable assignment (note: operators should be aware
///   that `SET SESSION/GLOBAL` can affect security behavior)
///
/// ## Always Denied
/// - `USE`: Not supported with connection pooling (database changes would be lost)
/// - Other unsupported types (e.g., `CALL`, `LOCK TABLES`, `LOAD DATA`)
///
/// ## Opt-In (require explicit permission)
/// - `INSERT`: Controlled by `MYSQL_ALLOW_INSERT` or schema-specific `allow_insert`
/// - `UPDATE`: Controlled by `MYSQL_ALLOW_UPDATE` or schema-specific `allow_update`
/// - `DELETE`: Controlled by `MYSQL_ALLOW_DELETE` or schema-specific `allow_delete`
/// - `CREATE`, `ALTER`, `DROP`, `TRUNCATE` (DDL): Controlled by `MYSQL_ALLOW_DDL`
///   or schema-specific `allow_ddl`
///
/// # Example
///
/// ```ignore
/// use crate::permissions::check_permission;
/// use crate::sql_parser::StatementType;
///
/// // Check if INSERT is allowed on a specific schema
/// check_permission(&config, &StatementType::Insert, Some("mydb"))?;
///
/// // Check with default schema (uses connection.database)
/// check_permission(&config, &StatementType::Insert, None)?;
/// ```
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
///
/// This function is used for multi-table statements (e.g., multi-table DELETEs)
/// where multiple schemas may be referenced. It iterates through all target schemas
/// and fails if **any** schema denies the operation.
///
/// # Behavior
///
/// - If `parsed.all_target_schemas` is empty, falls back to [`check_permission`]
///   with `target_schema = None` (uses connected database for permission lookup).
/// - Otherwise, calls [`check_permission`] for each schema in `all_target_schemas`.
/// - Returns `Err` on the **first** denied schema (short-circuit evaluation).
///
/// # Permission Resolution
///
/// Each schema is checked independently using the same resolution order as
/// [`check_permission`]: schema-specific overrides take precedence over global
/// settings for each schema.
///
/// # Example
///
/// ```ignore
/// use crate::permissions::check_all_permissions;
///
/// // For a multi-table DELETE across db1.table1 and db2.table2:
/// // - Both db1 and db2 must allow DELETE
/// let parsed = sql_parser::parse("DELETE FROM db1.t1, db2.t2 WHERE ...")?;
/// check_all_permissions(&config, &parsed)?;
/// ```
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
