use super::introspect::SchemaIntrospector;
use crate::test_helpers::setup_pg_test_db;

// pg-schema-t1: list tables returns results
#[tokio::test]
async fn test_pg_list_tables_returns_results() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };
    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        60,
    );
    let schema = Some(test_db.schema.as_str());
    let tables = introspector.list_tables(schema).await;
    assert!(tables.is_ok(), "list_tables should succeed: {:?}", tables.err());
}

// pg-schema-t2: get columns for a known table
#[tokio::test]
async fn test_pg_get_columns_for_table() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pg_test_schema_cols (id SERIAL PRIMARY KEY, name VARCHAR(100))",
    )
    .execute(&test_db.pool)
    .await
    .unwrap();

    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        60,
    );
    let columns = introspector
        .get_columns("pg_test_schema_cols", Some(&test_db.schema))
        .await;
    assert!(columns.is_ok(), "get_columns should succeed: {:?}", columns.err());
    let cols = columns.unwrap();
    assert!(
        cols.iter().any(|c| c.name == "id"),
        "should find 'id' column"
    );
    assert!(
        cols.iter().any(|c| c.name == "name"),
        "should find 'name' column"
    );

    sqlx::query("DROP TABLE IF EXISTS pg_test_schema_cols")
        .execute(&test_db.pool)
        .await
        .ok();
}

// pg-schema-t3: list_indexed_columns returns indexed columns
#[tokio::test]
async fn test_pg_indexed_columns_cache_hit() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pg_test_indexed_cache (id SERIAL PRIMARY KEY, val VARCHAR(50))",
    )
    .execute(&test_db.pool)
    .await
    .unwrap();
    sqlx::query("CREATE INDEX idx_pg_cache_val ON pg_test_indexed_cache (val)")
        .execute(&test_db.pool)
        .await
        .unwrap();

    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        60,
    );

    let first = introspector
        .list_indexed_columns("pg_test_indexed_cache", Some(&test_db.schema))
        .await
        .unwrap();
    let second = introspector
        .list_indexed_columns("pg_test_indexed_cache", Some(&test_db.schema))
        .await
        .unwrap();
    assert_eq!(first, second, "cached result must equal first fetch");
    assert!(
        first.iter().any(|c| c.eq_ignore_ascii_case("val")),
        "idx_pg_cache_val should be in indexed cols"
    );

    sqlx::query("DROP TABLE IF EXISTS pg_test_indexed_cache")
        .execute(&test_db.pool)
        .await
        .ok();
}

// pg-schema-t4: invalidate_table clears indexed_columns_cache
#[tokio::test]
async fn test_pg_indexed_columns_cache_invalidated_on_ddl() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };

    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        0,
    );

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pg_idx_inval (id SERIAL PRIMARY KEY, val VARCHAR(50))",
    )
    .execute(&test_db.pool)
    .await
    .unwrap();

    let before = introspector
        .list_indexed_columns("pg_idx_inval", Some(&test_db.schema))
        .await
        .unwrap();
    assert!(
        !before.iter().any(|c| c.eq_ignore_ascii_case("val")),
        "val should not be indexed yet"
    );

    sqlx::query("CREATE INDEX idx_pg_inval_val ON pg_idx_inval (val)")
        .execute(&test_db.pool)
        .await
        .unwrap();

    introspector
        .invalidate_table("pg_idx_inval", Some(&test_db.schema))
        .await;
    let after = introspector
        .list_indexed_columns("pg_idx_inval", Some(&test_db.schema))
        .await
        .unwrap();
    assert!(
        after.iter().any(|c| c.eq_ignore_ascii_case("val")),
        "val should be indexed after CREATE INDEX"
    );

    sqlx::query("DROP TABLE IF EXISTS pg_idx_inval")
        .execute(&test_db.pool)
        .await
        .ok();
}

// pg-schema-t5: list_composite_indexes returns index structure
#[tokio::test]
async fn test_pg_list_composite_indexes() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pg_test_composite_idx (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100) UNIQUE
        )",
    )
    .execute(&test_db.pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE INDEX idx_pg_name ON pg_test_composite_idx (last_name, first_name)",
    )
    .execute(&test_db.pool)
    .await
    .unwrap();

    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        60,
    );
    let indexes = introspector
        .list_composite_indexes("pg_test_composite_idx", Some(&test_db.schema))
        .await
        .unwrap();

    let name_idx = indexes
        .iter()
        .find(|i| i.name.eq_ignore_ascii_case("idx_pg_name"))
        .expect("idx_pg_name composite index should be found");
    assert_eq!(
        name_idx.columns.len(),
        2,
        "composite index should have 2 columns"
    );
    assert!(
        name_idx.columns[0].eq_ignore_ascii_case("last_name"),
        "first column of composite index"
    );
    assert!(
        name_idx.columns[1].eq_ignore_ascii_case("first_name"),
        "second column of composite index"
    );

    // The UNIQUE constraint on email creates a unique index
    let email_idx = indexes
        .iter()
        .find(|i| i.name.contains("email") || i.name.contains("pg_test_composite_idx_email"))
        .or_else(|| indexes.iter().find(|i| i.unique && i.columns.len() == 1 && i.columns[0].eq_ignore_ascii_case("email")));
    assert!(
        email_idx.is_some(),
        "unique index on email should be found"
    );

    sqlx::query("DROP TABLE IF EXISTS pg_test_composite_idx")
        .execute(&test_db.pool)
        .await
        .ok();
}

// pg-schema-t6: generate_index_suggestions - single unindexed column
#[tokio::test]
async fn test_pg_generate_suggestions_single_unindexed() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pg_test_suggest_single (
            id SERIAL PRIMARY KEY,
            category VARCHAR(50)
        )",
    )
    .execute(&test_db.pool)
    .await
    .unwrap();

    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        60,
    );
    let suggestions = introspector
        .generate_index_suggestions(
            "pg_test_suggest_single",
            Some(&test_db.schema),
            &["category".to_string()],
        )
        .await;

    assert!(
        !suggestions.is_empty(),
        "should generate a suggestion for unindexed column"
    );
    assert!(
        suggestions[0].contains("category"),
        "suggestion should mention the column"
    );
    assert!(
        suggestions[0].contains("CREATE INDEX"),
        "suggestion should include CREATE INDEX"
    );

    sqlx::query("DROP TABLE IF EXISTS pg_test_suggest_single")
        .execute(&test_db.pool)
        .await
        .ok();
}

// pg-schema-t7: generate_index_suggestions - composite index suggestion
#[tokio::test]
async fn test_pg_generate_suggestions_composite_index() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pg_test_suggest_composite (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50)
        )",
    )
    .execute(&test_db.pool)
    .await
    .unwrap();

    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        60,
    );
    let suggestions = introspector
        .generate_index_suggestions(
            "pg_test_suggest_composite",
            Some(&test_db.schema),
            &["first_name".to_string(), "last_name".to_string()],
        )
        .await;

    assert!(
        !suggestions.is_empty(),
        "should generate suggestion for multiple unindexed columns"
    );
    assert!(
        suggestions[0].contains("composite"),
        "should mention composite index"
    );
    assert!(
        suggestions.len() == 1,
        "should be a single composite suggestion, not per-column"
    );

    sqlx::query("DROP TABLE IF EXISTS pg_test_suggest_composite")
        .execute(&test_db.pool)
        .await
        .ok();
}

// pg-schema-t8: generate_index_suggestions - low cardinality column warning
#[tokio::test]
async fn test_pg_generate_suggestions_low_cardinality() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pg_test_suggest_lowcard (
            id SERIAL PRIMARY KEY,
            active BOOLEAN
        )",
    )
    .execute(&test_db.pool)
    .await
    .unwrap();

    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        60,
    );
    let suggestions = introspector
        .generate_index_suggestions(
            "pg_test_suggest_lowcard",
            Some(&test_db.schema),
            &["active".to_string()],
        )
        .await;

    assert!(
        !suggestions.is_empty(),
        "should generate a suggestion for unindexed low-cardinality column"
    );
    assert!(
        suggestions[0].contains("low cardinality") || suggestions[0].contains("cardinality"),
        "suggestion should mention low cardinality: {}",
        suggestions[0]
    );

    sqlx::query("DROP TABLE IF EXISTS pg_test_suggest_lowcard")
        .execute(&test_db.pool)
        .await
        .ok();
}

// pg-schema-t9: get_schema_info with indexes
#[tokio::test]
async fn test_pg_get_schema_info_with_indexes() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pg_test_schema_info (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE
        )",
    )
    .execute(&test_db.pool)
    .await
    .unwrap();

    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        60,
    );
    let info = introspector
        .get_schema_info(
            "pg_test_schema_info",
            Some(&test_db.schema),
            true,  // include_indexes
            false, // include_foreign_keys
            true,  // include_size
        )
        .await;

    assert!(info.is_ok(), "get_schema_info should succeed: {:?}", info.err());
    let info = info.unwrap();
    assert!(info.get("columns").is_some(), "should have columns");
    assert!(info.get("indexes").is_some(), "should have indexes");
    assert!(info.get("size").is_some(), "should have size");

    sqlx::query("DROP TABLE IF EXISTS pg_test_schema_info")
        .execute(&test_db.pool)
        .await
        .ok();
}

// pg-schema-t10: schema cache TTL=0 disables caching
#[tokio::test]
async fn test_pg_schema_cache_ttl_zero_disables_cache() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };

    let marker = format!("_pg_cache_ttl_test_{}", std::process::id());

    sqlx::query(&format!("DROP TABLE IF EXISTS {}", marker))
        .execute(&test_db.pool)
        .await
        .unwrap();

    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        0,
    );

    let tables_before = introspector
        .list_tables(Some(&test_db.schema))
        .await
        .unwrap();

    sqlx::query(&format!("CREATE TABLE {} (id SERIAL PRIMARY KEY)", marker))
        .execute(&test_db.pool)
        .await
        .unwrap();

    let tables_after = introspector
        .list_tables(Some(&test_db.schema))
        .await
        .unwrap();

    sqlx::query(&format!("DROP TABLE IF EXISTS {}", marker))
        .execute(&test_db.pool)
        .await
        .ok();

    assert!(
        !tables_before.iter().any(|t| t.name == marker),
        "new table should not appear in pre-DDL list"
    );
    assert!(
        tables_after.iter().any(|t| t.name == marker),
        "new table should appear after DDL"
    );
}

// pg-schema-ttl: non-zero TTL expires and triggers re-fetch
#[tokio::test]
async fn test_pg_schema_cache_ttl_expiry_triggers_refetch() {
    let Some(test_db) = setup_pg_test_db().await else {
        return;
    };

    let marker = format!("_pg_ttl_expiry_test_{}", std::process::id());

    sqlx::query(&format!("DROP TABLE IF EXISTS {}", marker))
        .execute(&test_db.pool)
        .await
        .unwrap();

    let introspector = SchemaIntrospector::new_with_backend(
        test_db.pool_handle.clone(),
        test_db.backend.clone(),
        1,
    );

    let before = introspector
        .list_tables(Some(&test_db.schema))
        .await
        .unwrap();
    assert!(
        !before.iter().any(|t| t.name == marker),
        "marker should not exist yet"
    );

    sqlx::query(&format!("CREATE TABLE {} (id SERIAL PRIMARY KEY)", marker))
        .execute(&test_db.pool)
        .await
        .unwrap();

    let cached = introspector
        .list_tables(Some(&test_db.schema))
        .await
        .unwrap();
    assert!(
        !cached.iter().any(|t| t.name == marker),
        "cache should still be warm immediately after DDL"
    );

    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    let after = introspector
        .list_tables(Some(&test_db.schema))
        .await
        .unwrap();

    sqlx::query(&format!("DROP TABLE IF EXISTS {}", marker))
        .execute(&test_db.pool)
        .await
        .ok();

    assert!(
        after.iter().any(|t| t.name == marker),
        "cache should have expired and re-fetched the new table"
    );
}
