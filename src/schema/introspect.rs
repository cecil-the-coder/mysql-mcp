use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;

use super::cache::{SchemaCache, get_cached_or_refresh};
use super::fetch;
use super::{TableInfo, ColumnInfo, IndexDef, is_low_cardinality_type};

pub struct SchemaIntrospector {
    pub(crate) inner: Arc<SchemaCache>,
}

/// Returns true if the cache key's table segment matches `table` (case-insensitive).
/// Cache keys have the form "{database}.{table}" where database may be empty.
fn key_matches_table(key: &str, table: &str) -> bool {
    let key_table = key.splitn(2, '.').nth(1).unwrap_or(key);
    key_table.eq_ignore_ascii_case(table)
}

impl SchemaIntrospector {
    pub fn new(pool: Arc<sqlx::MySqlPool>, cache_ttl_secs: u64) -> Self {
        use std::collections::HashMap;
        use tokio::sync::Mutex;
        Self {
            inner: Arc::new(SchemaCache {
                pool,
                cache_ttl: Duration::from_secs(cache_ttl_secs),
                tables_cache: Arc::new(Mutex::new(HashMap::new())),
                columns_cache: Arc::new(Mutex::new(HashMap::new())),
                indexed_columns_cache: Arc::new(Mutex::new(HashMap::new())),
                composite_indexes_cache: Arc::new(Mutex::new(HashMap::new())),
            }),
        }
    }

    pub async fn list_tables(&self, database: Option<&str>) -> Result<Vec<TableInfo>> {
        let cache_key = database.unwrap_or("").to_owned();
        let pool = Arc::clone(&self.inner.pool);
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.tables_cache),
            cache_key,
            self.inner.cache_ttl,
            move || {
                let pool = Arc::clone(&pool);
                let db = owned_database.clone();
                async move { fetch::fetch_tables(&pool, db.as_deref()).await }
            },
        ).await
    }

    /// Return all columns that have at least one index on the given table.
    /// Runs `SHOW INDEX FROM {table}` (qualified with database if provided).
    /// Results are cached with the same TTL as the column cache.
    pub async fn list_indexed_columns(&self, table: &str, database: Option<&str>) -> Result<Vec<String>> {
        let cache_key = format!("{}.{}", database.unwrap_or(""), table);
        let pool = Arc::clone(&self.inner.pool);
        let owned_table = table.to_owned();
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.indexed_columns_cache),
            cache_key,
            self.inner.cache_ttl,
            move || {
                let pool = Arc::clone(&pool);
                let t = owned_table.clone();
                let db = owned_database.clone();
                async move { fetch::fetch_indexed_columns(&pool, &t, db.as_deref()).await }
            },
        ).await
    }

    /// Return composite index information for the given table.
    /// Each entry represents one index: a named, ordered list of columns.
    /// Columns are ordered by their position within the index (SEQ_IN_INDEX).
    /// The PRIMARY key is included.
    /// Results are cached with the same TTL as `list_indexed_columns`.
    pub async fn list_composite_indexes(&self, table: &str, database: Option<&str>) -> Result<Vec<IndexDef>> {
        let cache_key = format!("{}.{}", database.unwrap_or(""), table);
        let pool = Arc::clone(&self.inner.pool);
        let owned_table = table.to_owned();
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.composite_indexes_cache),
            cache_key,
            self.inner.cache_ttl,
            move || {
                let pool = Arc::clone(&pool);
                let t = owned_table.clone();
                let db = owned_database.clone();
                async move { fetch::fetch_composite_indexes(&pool, &t, db.as_deref()).await }
            },
        ).await
    }

    pub async fn get_columns(&self, table_name: &str, database: Option<&str>) -> Result<Vec<ColumnInfo>> {
        let cache_key = format!("{}.{}", database.unwrap_or(""), table_name);
        let pool = Arc::clone(&self.inner.pool);
        let owned_table = table_name.to_owned();
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.columns_cache),
            cache_key,
            self.inner.cache_ttl,
            move || {
                let pool = Arc::clone(&pool);
                let t = owned_table.clone();
                let db = owned_database.clone();
                async move { fetch::fetch_columns(&pool, &t, db.as_deref()).await }
            },
        ).await
    }

    /// Generate schema-aware index suggestions for a query with a full table scan.
    ///
    /// Takes the list of WHERE-clause column names and the table name/database, and
    /// returns a list of human-readable suggestion strings.
    ///
    /// Handles two cases beyond the basic single-column index hint:
    ///
    /// 1. **Composite indexes**: if multiple WHERE columns are already covered by a
    ///    single existing composite index, suggest using that index rather than
    ///    creating individual single-column indexes.
    ///
    /// 2. **Low-cardinality columns**: if a WHERE column has a type with very few
    ///    distinct values (TINYINT(1)/BOOLEAN, ENUM, SET, BIT), add a note that an
    ///    index on that column alone may not improve performance because the optimizer
    ///    may prefer a full scan when the selectivity is too low.
    pub async fn generate_index_suggestions(
        &self,
        table: &str,
        database: Option<&str>,
        where_cols: &[String],
    ) -> Vec<String> {
        if where_cols.is_empty() {
            return vec![];
        }

        let indexed_cols = self.list_indexed_columns(table, database).await
            .unwrap_or_else(|e| { tracing::warn!("index suggestions: failed to list indexed columns for {}: {}", table, e); Vec::new() });
        let composite_indexes = self.list_composite_indexes(table, database).await
            .unwrap_or_else(|e| { tracing::warn!("index suggestions: failed to list composite indexes for {}: {}", table, e); Vec::new() });
        let col_info: std::collections::HashMap<String, ColumnInfo> = self
            .get_columns(table, database).await
            .unwrap_or_else(|e| { tracing::warn!("index suggestions: failed to get columns for {}: {}", table, e); Vec::new() })
            .into_iter()
            .map(|c| (c.name.to_lowercase(), c))
            .collect();

        let mut suggestions: Vec<String> = Vec::new();

        // Build a set of WHERE columns that are not yet individually indexed.
        let unindexed_cols: Vec<&String> = where_cols.iter()
            .filter(|col| !indexed_cols.iter().any(|ic| ic.eq_ignore_ascii_case(col)))
            .collect();

        // --- Case 1: Composite index detection ---
        // If there are 2+ unindexed WHERE columns, check whether a composite index
        // would cover them rather than N individual indexes.
        let esc = |s: &str| format!("`{}`", super::fetch::escape_mysql_identifier(s));
        // Sanitize a string for use as part of an index name (alphanumeric + underscore only).
        let safe_name = |s: &str| -> String {
            s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect()
        };
        if unindexed_cols.len() >= 2 {
            // An existing composite index covers the WHERE columns iff all WHERE columns
            // appear as a leading prefix of that index (B-tree indexes require leftmost prefix
            // for efficient range/equality filtering). Column ORDER within the prefix doesn't
            // matter for equality predicates, so we compare sets.
            let where_col_set: std::collections::HashSet<String> =
                where_cols.iter().map(|s| s.to_lowercase()).collect();
            let covered_by_existing = composite_indexes.iter().any(|idx| {
                if idx.columns.len() < where_col_set.len() { return false; }
                let prefix_set: std::collections::HashSet<String> = idx.columns[..where_col_set.len()]
                    .iter()
                    .map(|s| s.to_lowercase())
                    .collect();
                prefix_set == where_col_set
            });
            if !covered_by_existing {
                let idx_cols: Vec<&str> = unindexed_cols.iter().map(|c| c.as_str()).collect();
                let esc_cols: Vec<String> = idx_cols.iter().map(|c| esc(c)).collect();
                let safe_cols: Vec<String> = idx_cols.iter().map(|c| safe_name(c)).collect();
                suggestions.push(format!(
                    "Multiple unindexed WHERE columns on {}: [{}]. Consider a composite index: CREATE INDEX idx_{}_{} ON {}({});",
                    esc(table), idx_cols.join(", "), safe_name(table), safe_cols.join("_"), esc(table), esc_cols.join(", ")
                ));
            } else {
                suggestions.push(format!(
                    "WHERE columns on {} are covered by an existing composite index. Ensure the query uses the index by putting the leading column first in the WHERE clause.",
                    esc(table)
                ));
            }
        } else {
            // Single unindexed column: emit the standard per-column suggestion.
            for col in &unindexed_cols {
                let low_card = col_info.get(&col.to_lowercase())
                    .map(|ci| is_low_cardinality_type(&ci.column_type))
                    .unwrap_or(false);
                if low_card {
                    suggestions.push(format!(
                        "Column {} in WHERE clause on table {} has no index, but its type has low cardinality (few distinct values). An index may not improve performance — the optimizer may prefer a full table scan. Consider filtering on a higher-cardinality column instead, or use a partial/functional index.",
                        esc(col), esc(table)
                    ));
                } else {
                    suggestions.push(format!(
                        "Column {} in WHERE clause on table {} has no index. Consider: CREATE INDEX idx_{}_{} ON {}({});",
                        esc(col), esc(table), safe_name(table), safe_name(col), esc(table), esc(col)
                    ));
                }
            }
        }

        // --- Case 2: Low-cardinality hint for individually indexed columns ---
        for col in where_cols {
            let already_noted = suggestions.iter().any(|s| s.contains(esc(col).as_str()));
            if already_noted { continue; }
            let col_entry = col_info.get(&col.to_lowercase());
            let low_card = col_entry
                .map(|ci| is_low_cardinality_type(&ci.column_type))
                .unwrap_or(false);
            if low_card && indexed_cols.iter().any(|ic| ic.eq_ignore_ascii_case(col)) {
                suggestions.push(format!(
                    "Column {} on table {} is indexed but has low cardinality (type: {}). The optimizer may skip this index and perform a full scan. Consider reviewing query selectivity.",
                    esc(col), esc(table),
                    col_entry.map(|ci| ci.column_type.as_str()).unwrap_or("unknown")
                ));
            }
        }

        suggestions
    }

    /// Invalidate cached column data for a specific table (case-insensitive match on the
    /// table-name segment of the cache key, ignoring the database qualifier).
    /// Use after DDL that targets a known table (CREATE TABLE, ALTER TABLE, TRUNCATE).
    pub async fn invalidate_table(&self, table: &str) {
        if table.is_empty() {
            // Caller bug: empty table name should not reach here. The handler calls
            // invalidate_all() directly when the target table is unknown.
            tracing::warn!("invalidate_table called with empty table name; ignoring");
            return;
        }
        {
            let mut cache = self.inner.columns_cache.lock().await;
            cache.retain(|key, _| !key_matches_table(key, table));
        }
        {
            let mut idx_cache = self.inner.indexed_columns_cache.lock().await;
            idx_cache.retain(|key, _| !key_matches_table(key, table));
        }
        {
            let mut cidx_cache = self.inner.composite_indexes_cache.lock().await;
            cidx_cache.retain(|key, _| !key_matches_table(key, table));
        }
        // Also clear the tables list so row-count / existence info is refreshed.
        let mut tables_cache = self.inner.tables_cache.lock().await;
        tables_cache.clear();
    }

    /// Invalidate ALL cached schema data (tables list + all column caches).
    /// Use after DDL that may affect multiple tables (e.g., DROP DATABASE, DROP TABLE).
    pub async fn invalidate_all(&self) {
        self.inner.tables_cache.lock().await.clear();
        self.inner.columns_cache.lock().await.clear();
        self.inner.indexed_columns_cache.lock().await.clear();
        self.inner.composite_indexes_cache.lock().await.clear();
    }
}
