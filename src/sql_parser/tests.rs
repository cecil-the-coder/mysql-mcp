use super::*;

#[test]
fn test_select() {
    let p = parse_sql("SELECT * FROM users").unwrap();
    assert_eq!(p.statement_type, StatementType::Select);
    assert!(p.statement_type.is_read_only());
}

#[test]
fn test_select_with_schema() {
    let p = parse_sql("SELECT * FROM mydb.users").unwrap();
    assert_eq!(p.statement_type, StatementType::Select);
    // schema extraction is from INSERT/UPDATE/DELETE/DDL, not SELECT
}

#[test]
fn test_insert() {
    let p = parse_sql("INSERT INTO mydb.users (id, name) VALUES (1, 'Alice')").unwrap();
    assert_eq!(p.statement_type, StatementType::Insert);
    assert_eq!(p.target_schema, Some("mydb".to_string()));
    assert!(p.statement_type.is_write());
}

#[test]
fn test_update() {
    let p = parse_sql("UPDATE mydb.users SET name = 'Bob' WHERE id = 1").unwrap();
    assert_eq!(p.statement_type, StatementType::Update);
    assert_eq!(p.target_schema, Some("mydb".to_string()));
}

#[test]
fn test_delete() {
    let p = parse_sql("DELETE FROM mydb.orders WHERE id = 5").unwrap();
    assert_eq!(p.statement_type, StatementType::Delete);
    assert_eq!(p.target_schema, Some("mydb".to_string()));
}

#[test]
fn test_create_table() {
    let p = parse_sql("CREATE TABLE mydb.t (id INT)").unwrap();
    assert_eq!(p.statement_type, StatementType::Create);
    assert!(p.statement_type.is_ddl());
    assert_eq!(p.target_schema, Some("mydb".to_string()));
}

#[test]
fn test_alter_table() {
    let p = parse_sql("ALTER TABLE mydb.users ADD COLUMN age INT").unwrap();
    assert_eq!(p.statement_type, StatementType::Alter);
    assert_eq!(p.target_schema, Some("mydb".to_string()));
}

#[test]
fn test_drop_table() {
    let p = parse_sql("DROP TABLE mydb.users").unwrap();
    assert_eq!(p.statement_type, StatementType::Drop);
    assert_eq!(p.target_schema, Some("mydb".to_string()));
}

#[test]
fn test_truncate() {
    let p = parse_sql("TRUNCATE TABLE mydb.logs").unwrap();
    assert_eq!(p.statement_type, StatementType::Truncate);
    assert_eq!(p.target_schema, Some("mydb".to_string()));
}

#[test]
fn test_use() {
    let p = parse_sql("USE mydb").unwrap();
    assert_eq!(p.statement_type, StatementType::Use);
}

#[test]
fn test_show_tables() {
    let p = parse_sql("SHOW TABLES").unwrap();
    assert_eq!(p.statement_type, StatementType::Show);
    assert!(p.statement_type.is_read_only());
}

#[test]
fn test_explain() {
    let p = parse_sql("EXPLAIN SELECT * FROM users").unwrap();
    assert_eq!(p.statement_type, StatementType::Explain);
    assert!(p.statement_type.is_read_only());
}

#[test]
fn test_set_variable() {
    let p = parse_sql("SET @x = 1").unwrap();
    assert_eq!(p.statement_type, StatementType::Set);
}

#[test]
fn test_empty_sql() {
    assert!(parse_sql("").is_err());
}

#[test]
fn test_names() {
    assert_eq!(StatementType::Select.name(), "SELECT");
    assert_eq!(StatementType::Insert.name(), "INSERT");
    assert_eq!(StatementType::Other("FOO".to_string()).name(), "FOO");
}

#[test]
fn test_insert_no_schema() {
    let parsed = parse_sql("INSERT INTO users (name) VALUES ('test')").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Insert);
    assert!(parsed.target_schema.is_none());
}

#[test]
fn test_update_no_schema() {
    let parsed = parse_sql("UPDATE users SET name='new' WHERE id=1").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Update);
    assert!(parsed.target_schema.is_none());
}

#[test]
fn test_delete_no_schema() {
    let parsed = parse_sql("DELETE FROM users WHERE id=1").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Delete);
    assert!(parsed.target_schema.is_none());
}

#[test]
fn test_create_table_no_schema() {
    let parsed = parse_sql("CREATE TABLE foo (id INT PRIMARY KEY)").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Create);
    assert!(parsed.target_schema.is_none());
}

#[test]
fn test_alter_table_no_schema() {
    let parsed = parse_sql("ALTER TABLE foo ADD COLUMN bar VARCHAR(255)").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Alter);
    assert!(parsed.target_schema.is_none());
}

#[test]
fn test_drop_table_no_schema() {
    let parsed = parse_sql("DROP TABLE foo").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Drop);
    assert!(parsed.target_schema.is_none());
}

#[test]
fn test_truncate_no_schema() {
    let parsed = parse_sql("TRUNCATE TABLE foo").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Truncate);
    assert!(parsed.target_schema.is_none());
}

#[test]
fn test_select_no_schema() {
    let parsed = parse_sql("SELECT * FROM users").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Select);
    assert!(parsed.target_schema.is_none());
}

#[test]
fn test_invalid_sql() {
    let result = parse_sql("NOT VALID SQL !!!");
    assert!(result.is_err());
}

#[test]
fn test_schema_detection_qualified() {
    let parsed = parse_sql("INSERT INTO mydb.users (name) VALUES ('test')").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Insert);
    assert_eq!(parsed.target_schema, Some("mydb".to_string()));
}

#[test]
fn test_is_read_only() {
    assert!(StatementType::Select.is_read_only());
    assert!(StatementType::Show.is_read_only());
    assert!(StatementType::Explain.is_read_only());
    assert!(!StatementType::Insert.is_read_only());
    assert!(!StatementType::Create.is_read_only());
    assert!(!StatementType::Update.is_read_only());
    assert!(!StatementType::Delete.is_read_only());
}

#[test]
fn test_is_write() {
    assert!(StatementType::Insert.is_write());
    assert!(StatementType::Update.is_write());
    assert!(StatementType::Delete.is_write());
    assert!(!StatementType::Select.is_write());
    assert!(!StatementType::Show.is_write());
    assert!(!StatementType::Create.is_write());
}

#[test]
fn test_is_ddl_comprehensive() {
    assert!(StatementType::Create.is_ddl());
    assert!(StatementType::Alter.is_ddl());
    assert!(StatementType::Drop.is_ddl());
    assert!(StatementType::Truncate.is_ddl());
    assert!(!StatementType::Select.is_ddl());
    assert!(!StatementType::Insert.is_ddl());
    assert!(!StatementType::Update.is_ddl());
    assert!(!StatementType::Delete.is_ddl());
    assert!(!StatementType::Show.is_ddl());
}

#[test]
fn test_use_database() {
    // USE is supported - should parse as Use or similar
    let result = parse_sql("USE mydb");
    if let Ok(parsed) = result {
        assert!(matches!(
            parsed.statement_type,
            StatementType::Use | StatementType::Other(_)
        ));
    }
    // If err, that's also acceptable - just don't panic
}

#[test]
fn test_show_databases() {
    let parsed = parse_sql("SHOW DATABASES").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Show);
    assert!(parsed.statement_type.is_read_only());
}

#[test]
fn test_explain_select() {
    let parsed = parse_sql("EXPLAIN SELECT * FROM users WHERE id = 1").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Explain);
    assert!(parsed.statement_type.is_read_only());
}

#[test]
fn test_set_statement() {
    let parsed = parse_sql("SET @x = 1").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Set);
}

#[test]
fn test_create_database() {
    let parsed = parse_sql("CREATE DATABASE mydb").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Create);
}

#[test]
fn test_drop_qualified_schema() {
    let parsed = parse_sql("DROP TABLE mydb.users").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Drop);
    assert_eq!(parsed.target_schema, Some("mydb".to_string()));
}

// Tests for parse_write_warnings

#[test]
fn test_write_warnings_update_no_where() {
    let parsed = parse_sql("UPDATE users SET name = 'x'").unwrap();
    let warnings = parse_write_warnings(&parsed);
    assert_eq!(warnings.len(), 1);
    assert!(
        warnings[0].contains("WHERE"),
        "Expected WHERE warning, got: {}",
        warnings[0]
    );
}

#[test]
fn test_write_warnings_update_with_where() {
    let parsed = parse_sql("UPDATE users SET name = 'x' WHERE id = 1").unwrap();
    let warnings = parse_write_warnings(&parsed);
    assert!(
        warnings.is_empty(),
        "Expected no warnings for UPDATE with WHERE"
    );
}

#[test]
fn test_write_warnings_delete_no_where() {
    let parsed = parse_sql("DELETE FROM users").unwrap();
    let warnings = parse_write_warnings(&parsed);
    assert_eq!(warnings.len(), 1);
    assert!(
        warnings[0].contains("WHERE"),
        "Expected WHERE warning, got: {}",
        warnings[0]
    );
}

#[test]
fn test_write_warnings_delete_with_where() {
    let parsed = parse_sql("DELETE FROM users WHERE id = 1").unwrap();
    let warnings = parse_write_warnings(&parsed);
    assert!(
        warnings.is_empty(),
        "Expected no warnings for DELETE with WHERE"
    );
}

#[test]
fn test_write_warnings_truncate() {
    let parsed = parse_sql("TRUNCATE TABLE users").unwrap();
    let warnings = parse_write_warnings(&parsed);
    assert_eq!(warnings.len(), 1);
    assert!(
        warnings[0].contains("TRUNCATE"),
        "Expected TRUNCATE warning, got: {}",
        warnings[0]
    );
}

#[test]
fn test_write_warnings_insert_no_warnings() {
    let parsed = parse_sql("INSERT INTO users (name) VALUES ('Alice')").unwrap();
    let warnings = parse_write_warnings(&parsed);
    assert!(warnings.is_empty(), "Expected no warnings for INSERT");
}

#[test]
fn test_write_warnings_select_no_warnings() {
    let parsed = parse_sql("SELECT * FROM users").unwrap();
    let warnings = parse_write_warnings(&parsed);
    assert!(warnings.is_empty(), "Expected no warnings for SELECT");
}

#[test]
fn test_multi_statement_rejected() {
    // A multi-statement input must be rejected, regardless of what the
    // first statement is — otherwise "SELECT 1; DROP TABLE t" would be
    // misclassified as a read-only SELECT while MySQL would execute both.
    let err = parse_sql("SELECT 1; DROP TABLE t").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Multi-statement"),
        "Expected multi-statement error, got: {msg}"
    );

    // Two harmless SELECTs are also forbidden.
    let err2 = parse_sql("SELECT 1; SELECT 2").unwrap_err();
    assert!(err2.to_string().contains("Multi-statement"));
}

// Tests for pre-computed warnings (populated by parse_sql, no re-parse)

#[test]
fn test_parse_warnings_leading_wildcard_like() {
    let parsed = parse_sql("SELECT id FROM users WHERE name LIKE '%foo'").unwrap();
    assert!(
        parsed.has_leading_wildcard_like,
        "Expected has_leading_wildcard_like=true"
    );
    assert!(
        parsed
            .warnings
            .iter()
            .any(|w| w.contains("Leading wildcard")),
        "Expected leading wildcard warning, got: {:?}",
        parsed.warnings
    );
}

#[test]
fn test_parse_warnings_no_leading_wildcard() {
    let parsed = parse_sql("SELECT id FROM users WHERE name LIKE 'foo%' LIMIT 10").unwrap();
    assert!(
        !parsed.has_leading_wildcard_like,
        "Expected has_leading_wildcard_like=false for trailing wildcard"
    );
    assert!(
        !parsed
            .warnings
            .iter()
            .any(|w| w.contains("Leading wildcard")),
        "Should not warn for trailing wildcard, got: {:?}",
        parsed.warnings
    );
}

#[test]
fn test_parse_warnings_no_reparse() {
    // Warnings are pre-computed by parse_sql() and stored in parsed.warnings —
    // verify that parse_sql populates the field correctly for a simple SELECT.
    let parsed = parse_sql("SELECT * FROM users").unwrap();
    // SELECT * with no LIMIT and no WHERE triggers multiple warnings.
    assert!(
        !parsed.warnings.is_empty(),
        "SELECT * FROM users with no LIMIT should produce warnings, got: {:?}",
        parsed.warnings
    );
}

#[test]
fn test_union_query_gets_compound_warning() {
    let parsed = parse_sql("SELECT 1 UNION SELECT 2").unwrap();
    assert_eq!(
        parsed.statement_type,
        StatementType::Select,
        "UNION should still be Select"
    );
    assert!(
        parsed.warnings.iter().any(|w| w.contains("UNION")),
        "UNION query should have a compound-query warning, got: {:?}",
        parsed.warnings
    );
}

#[test]
fn test_select_for_update_rejected() {
    // FOR UPDATE is a locking read — classified via query.locks AST field, not raw string.
    let parsed = parse_sql("SELECT id FROM users WHERE id = 1 FOR UPDATE").unwrap();
    assert!(
        matches!(parsed.statement_type, StatementType::Other(_)),
        "SELECT FOR UPDATE should be classified as Other, got: {:?}",
        parsed.statement_type
    );
}

#[test]
fn test_select_for_share_rejected() {
    let parsed = parse_sql("SELECT id FROM users WHERE id = 1 FOR SHARE").unwrap();
    assert!(
        matches!(parsed.statement_type, StatementType::Other(_)),
        "SELECT FOR SHARE should be classified as Other"
    );
}

#[test]
fn test_select_plain_not_falsely_rejected_for_update() {
    // A column alias containing "update" must NOT be rejected as a locking read.
    let parsed = parse_sql("SELECT count(*) AS total_for_update_review FROM t").unwrap();
    assert_eq!(
        parsed.statement_type,
        StatementType::Select,
        "Plain SELECT with 'update' in alias should not be classified as locking read"
    );
}

#[test]
fn test_create_index_classified_as_create() {
    // CREATE INDEX must go through the explicit arm, not the debug-string catchall.
    let parsed = parse_sql("CREATE INDEX idx_name ON users (email)").unwrap();
    assert_eq!(
        parsed.statement_type,
        StatementType::Create,
        "CREATE INDEX should be classified as Create"
    );
    assert!(parsed.statement_type.is_ddl());
}

#[test]
fn test_union_with_limit_has_limit_set() {
    // query.limit is a top-level field on Query, so it applies to UNION bodies too.
    // Verify has_limit=true so max_rows injection is not incorrectly applied.
    let parsed = parse_sql("SELECT 1 UNION SELECT 2 LIMIT 5").unwrap();
    assert_eq!(parsed.statement_type, StatementType::Select);
    assert!(
        parsed.has_limit,
        "UNION with LIMIT should have has_limit=true"
    );
}

#[test]
fn test_select_comment_not_false_positive_for_outfile() {
    // A SQL comment containing "INTO OUTFILE" must not cause a false rejection.
    // The normalized check uses AST Display which strips comments.
    let parsed = parse_sql("SELECT id FROM t -- INTO OUTFILE '/tmp/out'").unwrap();
    assert_eq!(
        parsed.statement_type,
        StatementType::Select,
        "Comment containing INTO OUTFILE should not be rejected"
    );
}
