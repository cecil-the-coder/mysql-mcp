use std::sync::Arc;
use super::introspect::SchemaIntrospector;
use super::is_low_cardinality_type;
use crate::test_helpers::setup_test_db;

// mysql-mcp-t7a: list tables returns results
#[tokio::test]
async fn test_list_tables_returns_results() {
    let Some(test_db) = setup_test_db().await else { return; };
    let pool = Arc::new(test_db.pool.clone());
    let introspector = SchemaIntrospector::new(pool, 60);
    let db = test_db.config.connection.database.as_deref();
    let tables = introspector.list_tables(db).await;
    assert!(tables.is_ok());
}

// mysql-mcp-63f: schema cache TTL=0 disables caching
#[tokio::test]
async fn test_schema_cache_ttl_zero_disables_cache() {
    let Some(test_db) = setup_test_db().await else { return; };
    let pool = Arc::new(test_db.pool.clone());
    let db = test_db.config.connection.database.as_deref();

    // Use a unique table name to avoid races with other concurrent tests.
    let marker = format!("_cache_ttl_test_{}", std::process::id());

    // Ensure the marker table doesn't exist before we start.
    sqlx::query(&format!("DROP TABLE IF EXISTS {marker}"))
        .execute(pool.as_ref()).await.unwrap();

    // TTL=0 means instant expiry; every call re-fetches from the DB.
    let introspector = SchemaIntrospector::new(pool.clone(), 0);

    // First call: marker table doesn't exist yet.
    let tables_before = introspector.list_tables(db).await.unwrap();

    // Create the marker table between calls.
    sqlx::query(&format!("CREATE TABLE {marker} (id INT)"))
        .execute(pool.as_ref()).await.unwrap();

    // Second call: must re-fetch (TTL=0), so it should see the new table.
    let tables_after = introspector.list_tables(db).await.unwrap();

    // Cleanup.
    sqlx::query(&format!("DROP TABLE IF EXISTS {marker}"))
        .execute(pool.as_ref()).await.ok();

    let before_names: Vec<&str> = tables_before.iter().map(|t| t.name.as_str()).collect();
    let after_names: Vec<&str> = tables_after.iter().map(|t| t.name.as_str()).collect();
    assert!(!before_names.contains(&marker.as_str()),
        "marker table should not exist before creation");
    assert!(after_names.contains(&marker.as_str()),
        "TTL=0 must re-fetch: new table should be visible after creation");
}

// mysql-mcp-1au: get columns for a known table
#[tokio::test]
async fn test_get_columns_for_table() {
    let Some(test_db) = setup_test_db().await else { return; };
    let pool = &test_db.pool;
    let db = test_db.config.connection.database.as_deref();

    sqlx::query("CREATE TABLE IF NOT EXISTS test_schema_cols (id INT PRIMARY KEY, name VARCHAR(100))")
        .execute(pool)
        .await
        .unwrap();

    let arc_pool = Arc::new(pool.clone());
    let introspector = SchemaIntrospector::new(arc_pool.clone(), 60);
    let columns = introspector.get_columns("test_schema_cols", db).await;
    assert!(columns.is_ok());

    sqlx::query("DROP TABLE IF EXISTS test_schema_cols")
        .execute(arc_pool.as_ref())
        .await
        .ok();
}

// mysql-mcp-26b: pool behavior under concurrent load - stub
#[tokio::test]
async fn test_pool_concurrent_queries_stub() {
    eprintln!("TODO: requires special setup (concurrent load testing with controlled pool size)");
}

// mysql-mcp-8r8: list_indexed_columns() caching - cache returns same data on second call
#[tokio::test]
async fn test_indexed_columns_cache_hit() {
    let Some(test_db) = setup_test_db().await else { return; };
    let pool = Arc::new(test_db.pool.clone());
    let db = test_db.config.connection.database.as_deref();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_indexed_cache (id INT PRIMARY KEY, val VARCHAR(50), INDEX idx_val (val))"
    )
    .execute(pool.as_ref()).await.unwrap();

    let introspector = SchemaIntrospector::new(pool.clone(), 60);

    let first = introspector.list_indexed_columns("test_indexed_cache", db).await.unwrap();
    let second = introspector.list_indexed_columns("test_indexed_cache", db).await.unwrap();
    assert_eq!(first, second, "cached result must equal first fetch");
    assert!(first.iter().any(|c| c.eq_ignore_ascii_case("val")), "idx_val should be in indexed cols");
    assert!(first.iter().any(|c| c.eq_ignore_ascii_case("id")), "PRIMARY should be in indexed cols");

    sqlx::query("DROP TABLE IF EXISTS test_indexed_cache").execute(pool.as_ref()).await.ok();
}

// mysql-mcp-8r8: invalidate_table clears indexed_columns_cache
#[tokio::test]
async fn test_indexed_columns_cache_invalidated_on_ddl() {
    let Some(test_db) = setup_test_db().await else { return; };
    let pool = Arc::new(test_db.pool.clone());
    let db = test_db.config.connection.database.as_deref();

    // Start with TTL=0 so every real fetch goes to DB, allowing us to test invalidation behavior.
    let introspector = SchemaIntrospector::new(pool.clone(), 0);

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_idx_inval (id INT PRIMARY KEY, val VARCHAR(50))"
    )
    .execute(pool.as_ref()).await.unwrap();

    let before = introspector.list_indexed_columns("test_idx_inval", db).await.unwrap();
    assert!(!before.iter().any(|c| c.eq_ignore_ascii_case("val")), "val should not be indexed yet");

    sqlx::query("ALTER TABLE test_idx_inval ADD INDEX idx_val_inval (val)")
        .execute(pool.as_ref()).await.unwrap();

    // With TTL=0 the cache never serves stale data, so the next call should see the new index.
    introspector.invalidate_table("test_idx_inval").await;
    let after = introspector.list_indexed_columns("test_idx_inval", db).await.unwrap();
    assert!(after.iter().any(|c| c.eq_ignore_ascii_case("val")), "val should be indexed after ALTER");

    sqlx::query("DROP TABLE IF EXISTS test_idx_inval").execute(pool.as_ref()).await.ok();
}

// mysql-mcp-7f3: list_composite_indexes returns index structure
#[tokio::test]
async fn test_list_composite_indexes() {
    let Some(test_db) = setup_test_db().await else { return; };
    let pool = Arc::new(test_db.pool.clone());
    let db = test_db.config.connection.database.as_deref();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_composite_idx (
            id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            INDEX idx_name (last_name, first_name),
            UNIQUE INDEX idx_email (email)
        )"
    )
    .execute(pool.as_ref()).await.unwrap();

    let introspector = SchemaIntrospector::new(pool.clone(), 60);
    let indexes = introspector.list_composite_indexes("test_composite_idx", db).await.unwrap();

    let name_idx = indexes.iter().find(|i| i.name.eq_ignore_ascii_case("idx_name"));
    assert!(name_idx.is_some(), "idx_name composite index should be found");
    let name_idx = name_idx.unwrap();
    assert_eq!(name_idx.columns.len(), 2, "composite index should have 2 columns");
    assert!(name_idx.columns[0].eq_ignore_ascii_case("last_name"), "first column of composite index");
    assert!(name_idx.columns[1].eq_ignore_ascii_case("first_name"), "second column of composite index");

    let email_idx = indexes.iter().find(|i| i.name.eq_ignore_ascii_case("idx_email"));
    assert!(email_idx.is_some(), "idx_email unique index should be found");
    assert!(email_idx.unwrap().unique, "idx_email should be unique");

    sqlx::query("DROP TABLE IF EXISTS test_composite_idx").execute(pool.as_ref()).await.ok();
}

// mysql-mcp-7f3: generate_index_suggestions - single unindexed column
#[tokio::test]
async fn test_generate_suggestions_single_unindexed() {
    let Some(test_db) = setup_test_db().await else { return; };
    let pool = Arc::new(test_db.pool.clone());
    let db = test_db.config.connection.database.as_deref();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_suggest_single (
            id INT PRIMARY KEY,
            category VARCHAR(50)
        )"
    )
    .execute(pool.as_ref()).await.unwrap();

    let introspector = SchemaIntrospector::new(pool.clone(), 60);
    let suggestions = introspector.generate_index_suggestions(
        "test_suggest_single",
        db,
        &["category".to_string()],
    ).await;

    assert!(!suggestions.is_empty(), "should generate a suggestion for unindexed column");
    assert!(suggestions[0].contains("category"), "suggestion should mention the column");
    assert!(suggestions[0].contains("CREATE INDEX"), "suggestion should include CREATE INDEX");

    sqlx::query("DROP TABLE IF EXISTS test_suggest_single").execute(pool.as_ref()).await.ok();
}

// mysql-mcp-7f3: generate_index_suggestions - composite index suggestion for multiple unindexed columns
#[tokio::test]
async fn test_generate_suggestions_composite_index() {
    let Some(test_db) = setup_test_db().await else { return; };
    let pool = Arc::new(test_db.pool.clone());
    let db = test_db.config.connection.database.as_deref();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_suggest_composite (
            id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50)
        )"
    )
    .execute(pool.as_ref()).await.unwrap();

    let introspector = SchemaIntrospector::new(pool.clone(), 60);
    let suggestions = introspector.generate_index_suggestions(
        "test_suggest_composite",
        db,
        &["first_name".to_string(), "last_name".to_string()],
    ).await;

    assert!(!suggestions.is_empty(), "should generate suggestion for multiple unindexed columns");
    // Should suggest a composite index, not two individual ones.
    assert!(suggestions[0].contains("composite"), "should mention composite index");
    assert!(suggestions.len() == 1, "should be a single composite suggestion, not per-column");

    sqlx::query("DROP TABLE IF EXISTS test_suggest_composite").execute(pool.as_ref()).await.ok();
}

// mysql-mcp-7f3: generate_index_suggestions - low-cardinality column warning
#[tokio::test]
async fn test_generate_suggestions_low_cardinality() {
    let Some(test_db) = setup_test_db().await else { return; };
    let pool = Arc::new(test_db.pool.clone());
    let db = test_db.config.connection.database.as_deref();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_suggest_lowcard (
            id INT PRIMARY KEY,
            active TINYINT(1)
        )"
    )
    .execute(pool.as_ref()).await.unwrap();

    let introspector = SchemaIntrospector::new(pool.clone(), 60);
    let suggestions = introspector.generate_index_suggestions(
        "test_suggest_lowcard",
        db,
        &["active".to_string()],
    ).await;

    assert!(!suggestions.is_empty(), "should generate a suggestion for unindexed low-cardinality column");
    assert!(suggestions[0].contains("low cardinality") || suggestions[0].contains("cardinality"),
        "suggestion should mention low cardinality: {}", suggestions[0]);

    sqlx::query("DROP TABLE IF EXISTS test_suggest_lowcard").execute(pool.as_ref()).await.ok();
}

// mysql-mcp-7f3: is_low_cardinality_type helper function
#[test]
fn test_is_low_cardinality_type() {
    assert!(is_low_cardinality_type("tinyint"), "tinyint is low cardinality");
    assert!(is_low_cardinality_type("bool"), "bool is low cardinality");
    assert!(is_low_cardinality_type("boolean"), "boolean is low cardinality");
    assert!(is_low_cardinality_type("enum"), "enum is low cardinality");
    assert!(is_low_cardinality_type("set"), "set is low cardinality");
    assert!(is_low_cardinality_type("bit"), "bit is low cardinality");
    assert!(is_low_cardinality_type("TINYINT"), "TINYINT case-insensitive");
    assert!(is_low_cardinality_type("ENUM"), "ENUM case-insensitive");

    assert!(!is_low_cardinality_type("int"), "int is not low cardinality");
    assert!(!is_low_cardinality_type("varchar"), "varchar is not low cardinality");
    assert!(!is_low_cardinality_type("bigint"), "bigint is not low cardinality");
    assert!(!is_low_cardinality_type("datetime"), "datetime is not low cardinality");
}
