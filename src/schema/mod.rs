//! Schema introspection module.
//!
//! # Submodules
//! - `fetch`     — raw SQL fetch functions that hit the DB (no caching) — kept for backward compat
//! - `introspect`— `SchemaIntrospector` with cache types, helper, and public methods
//! - `tests`     — integration tests (cfg(test) only)
//!
//! All public types keep the same names they had in the flat `schema.rs` file,
//! so callers in `server.rs` (`crate::schema::SchemaIntrospector`, etc.) compile unchanged.

#[cfg(feature = "mysql")]
pub(crate) mod fetch;
pub mod introspect;

#[cfg(all(test, feature = "postgres"))]
mod pg_tests;
#[cfg(all(test, feature = "sqlite"))]
mod sqlite_tests;
#[cfg(all(test, feature = "mysql"))]
mod tests;

// Re-export the public surface so `crate::schema::X` still works.
pub use introspect::SchemaIntrospector;

// --------------------------------------------------------------------------
// Caching strategy
// --------------------------------------------------------------------------
//
/// The `SchemaIntrospector` uses a multi-level TTL (time-to-live) cache to
/// avoid repeated database queries for schema information. The cache is
/// configured via `PoolConfig::cache_ttl_secs` (default: 60s). When the TTL
/// expires, the next call to any introspective method will trigger a
/// background refresh.
///
/// ### Cache hierarchy
///
/// The module maintains four independent LRU-style caches keyed by
/// `{database}\\t{table}`:
/// 1. **tables_cache**          — `Vec<TableInfo>`    (from `list_tables`)
/// 2. **columns_cache**         — `Vec<ColumnInfo>`  (from `get_columns`)
/// 3. **indexed_columns_cache** — `Vec<String>`      (from `list_indexed_columns`)
/// 4. **composite_indexes_cache** — `Vec<IndexDef>`  (from `list_composite_indexes`)
///
/// Each cache entry stores both the data and the `Instant` it was fetched.
/// On every access, the entry’s age is compared against the TTL; if stale,
/// a fresh fetch is scheduled.
///
/// ### TTL configuration
///
/// - `cache_ttl_secs = 0` — cache is effectively disabled; every call hits
///   the database.
/// - `cache_ttl_secs > 0` — entries are considered fresh for that many
///   seconds. The cache is checked under a per-map mutex, and refreshes
///   are performed outside the lock to avoid blocking concurrent readers.
///
/// ### Programmatic usage
///
/// ```ignore
/// # use autoanneal::schema::SchemaIntrospector;
/// # use std::sync::Arc;
/// # let pool: Arc<sqlx::MySqlPool> = todo!();
/// let inspector = SchemaIntrospector::new(Arc::clone(&pool), 60);
///
/// // Fetch full schema info (columns from cache, indexes+size from DB)
/// let info = inspector
///     .get_schema_info("users", None, true, true, true)
///     .await?;
///
/// // List tables (cached)
/// let tables = inspector.list_tables(None).await?;
///
/// // List indexed columns (cached)
/// let indexed = inspector.list_indexed_columns("users", None).await?;
///
/// // List composite indexes (cached)
/// let indexes = inspector.list_composite_indexes("users", None).await?;
///
/// // Invalidate after DDL
/// inspector.invalidate_table("users", None).await;
/// ```
///
/// ### Cache invalidation
///
/// Use `invalidate_table()` after DDL that affects a specific table (e.g.,
/// `ALTER TABLE`) and `invalidate_all()` after operations that span multiple
/// tables (e.g., `DROP DATABASE`). Both methods acquire all cache locks in a
/// fixed order (columns → indexed_columns → composite_indexes → tables)
/// to prevent deadlocks.
///
/// ### Index suggestions
///
/// The `generate_index_suggestions()` method combines cached column metadata
/// with live index information to produce actionable recommendations. It
/// detects:
/// - **Composite-index opportunities** — when multiple WHERE columns are
///   covered by a single existing composite index.
/// - **Low-cardinality warnings** — when a WHERE column’s type has very few
///   distinct values (e.g., `TINYINT(1)`, `ENUM`, `SET`, `BIT(N≤4)`), an index
///   may offer poor selectivity and the suggestion notes this.
///
/// ---
///
// --------------------------------------------------------------------------
// Public types
// --------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub schema: String,
    pub row_count: Option<i64>,
    pub data_size_bytes: Option<i64>,
    pub create_time: Option<String>,
    pub update_time: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    /// Full column type including display width and enum values, e.g. `"tinyint(1)"`, `"varchar(255)"`.
    /// Use this (not `data_type`) for low-cardinality checks.
    pub column_type: String,
    pub is_nullable: bool,
    pub column_default: Option<String>,
    pub column_key: Option<String>,
    pub extra: Option<String>,
}

/// Represents one index on a table: its name and the ordered list of column names.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub unique: bool,
    pub columns: Vec<String>,
}

/// Returns true if the given MySQL data type has inherently low cardinality,
/// meaning it can take only a small number of distinct values (e.g. boolean,
/// enum, set, or bit(1)). An index on such a column alone often has poor
/// selectivity and the optimizer may choose a full table scan instead.
pub fn is_low_cardinality_type(data_type: &str) -> bool {
    let dt = data_type.to_lowercase();
    dt == "bool"
        || dt == "boolean"
        || dt.starts_with("enum")
        || dt.starts_with("set")
        || dt.starts_with("bit")
        || dt.starts_with("tinyint(1)")
}

// --------------------------------------------------------------------------
// get_schema_info on SchemaIntrospector
//
// Now delegates to the backend trait for all schema operations.
// --------------------------------------------------------------------------

impl SchemaIntrospector {
    /// Detailed schema metadata for a single table: columns, indexes, foreign keys,
    /// and a size estimate.
    ///
    /// Columns are served from the cache. Indexes, foreign keys, and size are
    /// fetched through the backend trait.
    pub async fn get_schema_info(
        &self,
        table_name: &str,
        database: Option<&str>,
        include_indexes: bool,
        include_foreign_keys: bool,
        include_size: bool,
    ) -> anyhow::Result<serde_json::Value> {
        self.inner
            .backend
            .fetch_schema_details(
                &self.inner.pool,
                table_name,
                database,
                include_indexes,
                include_foreign_keys,
                include_size,
            )
            .await
    }
}
