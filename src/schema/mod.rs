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
