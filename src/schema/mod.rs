//! Schema introspection module.
//!
//! # Submodules
//! - `cache`     — `CacheEntry`, `SchemaCache` struct, `get_cached_or_refresh` generic helper
//! - `fetch`     — raw SQL fetch functions that hit the DB (no caching)
//! - `introspect`— `SchemaIntrospector` with public cache methods and `SchemaIntrospector::new`
//! - `tests`     — integration tests (cfg(test) only)
//!
//! All public types keep the same names they had in the flat `schema.rs` file,
//! so callers in `server.rs` (`crate::schema::SchemaIntrospector`, etc.) compile unchanged.

pub(crate) mod cache;
pub(crate) mod fetch;
pub mod introspect;

#[cfg(test)]
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
    // TINYINT(1) is used as BOOLEAN in MySQL; BOOL/BOOLEAN are aliases.
    // ENUM and SET have a fixed, typically small value domain.
    // BIT columns are usually 1-bit flags.
    matches!(dt.as_str(), "bool" | "boolean" | "enum" | "set" | "bit")
        || dt.starts_with("tinyint(1)")
}

// --------------------------------------------------------------------------
// get_schema_info on SchemaIntrospector
//
// Kept here (not in introspect.rs) so introspect.rs stays ≤ 500 lines.
// This method has the same signature as the original and calls cached sub-methods
// for columns; index/FK/size data goes to the DB directly since those details
// are not held in the regular cache structures.
// --------------------------------------------------------------------------

impl SchemaIntrospector {
    /// Detailed schema metadata for a single table: columns, indexes, foreign keys,
    /// and a size estimate.
    ///
    /// Columns are served from the cache. Indexes, foreign keys, and size are
    /// fetched directly from the DB on each call (they are only requested when the
    /// caller explicitly opts in via the `include_*` flags, and they carry richer
    /// per-column metadata than the composite-index cache provides).
    pub async fn get_schema_info(
        &self,
        table_name: &str,
        database: Option<&str>,
        include_indexes: bool,
        include_foreign_keys: bool,
        include_size: bool,
    ) -> anyhow::Result<serde_json::Value> {
        let pool = self.inner.pool.as_ref();

        // Columns (always included) — served from cache.
        let columns = self.get_columns(table_name, database).await?;

        // Indexes — richer than the composite-index cache (includes INDEX_TYPE, NULLABLE).
        let indexes: serde_json::Value = if include_indexes {
            let idx_sql = format!(
                "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, INDEX_TYPE, NULLABLE \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_NAME = ?{} \
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX",
                if database.is_some() { " AND TABLE_SCHEMA = ?" } else { "" }
            );
            let q = sqlx::query(&idx_sql).bind(table_name);
            let rows = if let Some(db) = database {
                q.bind(db).fetch_all(pool).await?
            } else {
                q.fetch_all(pool).await?
            };
            let mut idx_map: std::collections::BTreeMap<String, serde_json::Value> =
                std::collections::BTreeMap::new();
            for row in &rows {
                use sqlx::Row;
                let name: String = fetch::is_col_str(row, "INDEX_NAME");
                let non_unique: i64 = row.try_get("NON_UNIQUE").unwrap_or(1);
                let col: String = fetch::is_col_str(row, "COLUMN_NAME");
                let idx_type: String = fetch::is_col_str(row, "INDEX_TYPE");
                let nullable: String = fetch::is_col_str(row, "NULLABLE");
                let entry = idx_map.entry(name.clone()).or_insert_with(|| serde_json::json!({
                    "name": name, "unique": non_unique == 0, "type": idx_type, "columns": [],
                }));
                if let Some(cols) = entry.get_mut("columns").and_then(|v| v.as_array_mut()) {
                    cols.push(serde_json::json!({ "column": col, "nullable": nullable == "YES" }));
                }
            }
            serde_json::Value::Array(idx_map.into_values().collect())
        } else {
            serde_json::Value::Null
        };

        // Foreign keys.
        let foreign_keys: serde_json::Value = if include_foreign_keys {
            let fk_sql = format!(
                "SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, \
                        kcu.REFERENCED_COLUMN_NAME, rc.UPDATE_RULE, rc.DELETE_RULE \
                 FROM information_schema.KEY_COLUMN_USAGE kcu \
                 JOIN information_schema.REFERENTIAL_CONSTRAINTS rc \
                   ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
                  AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA \
                 WHERE kcu.TABLE_NAME = ?{} \
                   AND kcu.REFERENCED_TABLE_NAME IS NOT NULL",
                if database.is_some() { " AND kcu.TABLE_SCHEMA = ?" } else { "" }
            );
            let q = sqlx::query(&fk_sql).bind(table_name);
            let rows = if let Some(db) = database {
                q.bind(db).fetch_all(pool).await?
            } else {
                q.fetch_all(pool).await?
            };
            serde_json::Value::Array(rows.iter().map(|row| serde_json::json!({
                "constraint":        fetch::is_col_str(row, "CONSTRAINT_NAME"),
                "column":            fetch::is_col_str(row, "COLUMN_NAME"),
                "references_table":  fetch::is_col_str(row, "REFERENCED_TABLE_NAME"),
                "references_column": fetch::is_col_str(row, "REFERENCED_COLUMN_NAME"),
                "on_update":         fetch::is_col_str(row, "UPDATE_RULE"),
                "on_delete":         fetch::is_col_str(row, "DELETE_RULE"),
            })).collect())
        } else {
            serde_json::Value::Null
        };

        // Table size estimate.
        let size: serde_json::Value = if include_size {
            let size_sql = format!(
                "SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH \
                 FROM information_schema.TABLES \
                 WHERE TABLE_NAME = ?{}",
                if database.is_some() { " AND TABLE_SCHEMA = ?" } else { "" }
            );
            let q = sqlx::query(&size_sql).bind(table_name);
            let size_result = if let Some(db) = database {
                q.bind(db).fetch_one(pool).await
            } else {
                q.fetch_one(pool).await
            };
            match size_result {
                Ok(row) => {
                    use sqlx::Row;
                    serde_json::json!({
                        "estimated_rows": row.try_get::<Option<u64>, _>("TABLE_ROWS").ok().flatten(),
                        "data_bytes":     row.try_get::<Option<u64>, _>("DATA_LENGTH").ok().flatten(),
                        "index_bytes":    row.try_get::<Option<u64>, _>("INDEX_LENGTH").ok().flatten(),
                    })
                }
                // Table absent from information_schema — not an error.
                Err(sqlx::Error::RowNotFound) => serde_json::Value::Null,
                Err(e) => return Err(e.into()),
            }
        } else {
            serde_json::Value::Null
        };

        // Assemble result.
        let cols_json: Vec<serde_json::Value> = columns.iter().map(|c| serde_json::json!({
            "name": c.name, "type": c.data_type, "nullable": c.is_nullable,
            "default": c.column_default, "key": c.column_key, "extra": c.extra,
        })).collect();

        let mut result = serde_json::json!({ "table": table_name, "columns": cols_json });
        if !indexes.is_null()      { result["indexes"]      = indexes; }
        if !foreign_keys.is_null() { result["foreign_keys"] = foreign_keys; }
        if !size.is_null()         { result["size"]         = size; }
        Ok(result)
    }
}

