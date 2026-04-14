use crate::backend::Backend;
use crate::schema::introspect::SchemaIntrospector;
use crate::test_helpers::setup_sqlite_test_db;
use std::sync::Arc;

/// Helper: create a SchemaIntrospector from the test DB.
async fn make_introspector(pool: &crate::backend::PoolHandle) -> SchemaIntrospector {
    let backend: Arc<dyn Backend> = Arc::new(crate::backend::sqlite::SqliteBackend::new());
    SchemaIntrospector::new_with_backend(pool.clone(), backend, 0)
}

#[tokio::test]
async fn test_sqlite_list_tables_returns_results() {
    let db = setup_sqlite_test_db().await;
    let introspector = make_introspector(&db.pool).await;
    let tables = introspector.list_tables(None).await.unwrap();

    let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(
        names.contains(&"users"),
        "tables should include 'users', got: {:?}",
        names
    );
    assert!(
        names.contains(&"products"),
        "tables should include 'products', got: {:?}",
        names
    );
    assert!(
        names.contains(&"orders"),
        "tables should include 'orders', got: {:?}",
        names
    );
}

#[tokio::test]
async fn test_sqlite_list_tables_excludes_internal() {
    let db = setup_sqlite_test_db().await;
    let introspector = make_introspector(&db.pool).await;
    let tables = introspector.list_tables(None).await.unwrap();

    // SQLite internal tables should not appear
    for t in &tables {
        assert!(
            !t.name.starts_with("sqlite_"),
            "internal table '{}' should not appear",
            t.name
        );
    }
}

#[tokio::test]
async fn test_sqlite_get_columns_for_table() {
    let db = setup_sqlite_test_db().await;
    let introspector = make_introspector(&db.pool).await;

    let columns = introspector.get_columns("users", None).await.unwrap();
    let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();

    assert!(col_names.contains(&"id"), "users should have 'id' column");
    assert!(
        col_names.contains(&"name"),
        "users should have 'name' column"
    );
    assert!(
        col_names.contains(&"email"),
        "users should have 'email' column"
    );

    // Check primary key detection
    let id_col = columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(
        id_col.column_key.as_deref(),
        Some("PRI"),
        "id should be PRI key"
    );
}

#[tokio::test]
async fn test_sqlite_column_nullable() {
    let db = setup_sqlite_test_db().await;
    let introspector = make_introspector(&db.pool).await;

    let columns = introspector.get_columns("users", None).await.unwrap();

    let name_col = columns.iter().find(|c| c.name == "name").unwrap();
    assert!(
        !name_col.is_nullable,
        "name should NOT be nullable (NOT NULL)"
    );

    let email_col = columns.iter().find(|c| c.name == "email").unwrap();
    assert!(email_col.is_nullable, "email should be nullable");

    let age_col = columns.iter().find(|c| c.name == "age").unwrap();
    assert!(age_col.is_nullable, "age should be nullable");
}

#[tokio::test]
async fn test_sqlite_fetch_composite_indexes() {
    let db = setup_sqlite_test_db().await;
    // Create an index on orders(user_id, status)
    db.execute("CREATE INDEX idx_orders_user_status ON orders (user_id, status)")
        .await
        .unwrap();

    let backend = crate::backend::sqlite::SqliteBackend::new();
    let indexes = backend
        .fetch_composite_indexes(&db.pool, "orders", None)
        .await
        .unwrap();

    let idx = indexes
        .iter()
        .find(|i| i.name == "idx_orders_user_status")
        .expect("idx_orders_user_status should be found");
    assert_eq!(idx.columns.len(), 2);
    assert_eq!(idx.columns[0], "user_id");
    assert_eq!(idx.columns[1], "status");
    assert!(!idx.unique, "this index is not unique");

    // Create a unique index
    db.execute("CREATE UNIQUE INDEX idx_orders_id_unique ON orders (id)")
        .await
        .unwrap();
    let indexes2 = backend
        .fetch_composite_indexes(&db.pool, "orders", None)
        .await
        .unwrap();
    let unique_idx = indexes2
        .iter()
        .find(|i| i.name == "idx_orders_id_unique")
        .expect("unique index should be found");
    assert!(unique_idx.unique);
}

#[tokio::test]
async fn test_sqlite_fetch_indexed_columns() {
    let db = setup_sqlite_test_db().await;
    db.execute("CREATE INDEX idx_products_name ON products (name)")
        .await
        .unwrap();

    let backend = crate::backend::sqlite::SqliteBackend::new();
    let indexed = backend
        .fetch_indexed_columns(&db.pool, "products", None)
        .await
        .unwrap();

    assert!(
        indexed.iter().any(|c| c == "name"),
        "name should be in indexed columns"
    );
    // id is also indexed (it's the primary key, but autoindexes are skipped)
    // so just check that our custom index shows up
}

#[tokio::test]
async fn test_sqlite_get_schema_info_with_indexes() {
    let db = setup_sqlite_test_db().await;
    let introspector = make_introspector(&db.pool).await;

    let info = introspector
        .get_schema_info("users", None, true, false, false)
        .await
        .unwrap();

    assert_eq!(info["table"], "users");
    let cols = info["columns"].as_array().unwrap();
    assert!(!cols.is_empty(), "should have columns");

    // indexes should be included
    let indexes = info.get("indexes").and_then(|v| v.as_array());
    assert!(indexes.is_some(), "indexes should be included");
}

#[tokio::test]
async fn test_sqlite_get_schema_info_with_size() {
    let db = setup_sqlite_test_db().await;
    let introspector = make_introspector(&db.pool).await;

    let info = introspector
        .get_schema_info("users", None, false, false, true)
        .await
        .unwrap();

    let size = info.get("size").and_then(|v| v.as_object());
    assert!(size.is_some(), "size should be included");
    let size_obj = size.unwrap();
    assert!(
        size_obj.contains_key("estimated_rows"),
        "size should have estimated_rows"
    );
    assert!(
        size_obj.contains_key("data_bytes"),
        "size should have data_bytes"
    );
    // We inserted 3 users
    assert_eq!(
        size_obj["estimated_rows"].as_i64().unwrap(),
        3,
        "should estimate 3 rows"
    );
}

#[tokio::test]
async fn test_sqlite_generate_index_suggestions_single_column() {
    let db = setup_sqlite_test_db().await;
    // Create a table without an index on status
    db.execute("CREATE TABLE suggest_test (id INTEGER PRIMARY KEY, status TEXT)")
        .await
        .unwrap();

    let introspector = make_introspector(&db.pool).await;
    let suggestions = introspector
        .generate_index_suggestions("suggest_test", None, &["status".to_string()])
        .await;

    assert!(
        !suggestions.is_empty(),
        "should generate a suggestion for unindexed column"
    );
    assert!(
        suggestions[0].contains("status"),
        "suggestion should mention the column"
    );
    assert!(
        suggestions[0].contains("CREATE INDEX"),
        "suggestion should include CREATE INDEX"
    );
}

#[tokio::test]
async fn test_sqlite_generate_index_suggestions_already_indexed() {
    let db = setup_sqlite_test_db().await;
    db.execute("CREATE TABLE suggest_idx_test (id INTEGER PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    db.execute("CREATE INDEX idx_email ON suggest_idx_test (email)")
        .await
        .unwrap();

    let introspector = make_introspector(&db.pool).await;
    let suggestions = introspector
        .generate_index_suggestions("suggest_idx_test", None, &["email".to_string()])
        .await;

    // email is already indexed, so no suggestion should be generated
    assert!(
        suggestions.is_empty(),
        "should NOT generate suggestion for already-indexed column"
    );
}

#[tokio::test]
async fn test_sqlite_cache_ttl_zero_reflects_changes() {
    let db = setup_sqlite_test_db().await;
    // TTL=0 means no caching — every call re-fetches
    let introspector = make_introspector(&db.pool).await;

    let tables_before = introspector.list_tables(None).await.unwrap();
    assert!(
        !tables_before.iter().any(|t| t.name == "_new_table_marker"),
        "marker table should not exist yet"
    );

    // Create a new table
    db.execute("CREATE TABLE _new_table_marker (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();

    let tables_after = introspector.list_tables(None).await.unwrap();
    assert!(
        tables_after.iter().any(|t| t.name == "_new_table_marker"),
        "TTL=0 should see the new table immediately"
    );
}
