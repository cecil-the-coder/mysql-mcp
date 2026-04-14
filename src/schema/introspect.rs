use anyhow::Result;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use super::{is_low_cardinality_type, ColumnInfo, IndexDef, TableInfo};
use crate::backend::{Backend, PoolHandle};

// ---------------------------------------------------------------------------
// Cache internals
// ---------------------------------------------------------------------------

pub(crate) struct CacheEntry<T> {
    pub(crate) data: T,
    pub(crate) fetched_at: Instant,
}

/// Shared cache state for SchemaIntrospector.
pub(crate) struct SchemaCache {
    pub(crate) pool: PoolHandle,
    pub(crate) backend: Arc<dyn Backend>,
    pub(crate) cache_ttl: Duration,
    pub(crate) tables_cache: Arc<Mutex<HashMap<String, CacheEntry<Vec<TableInfo>>>>>,
    pub(crate) columns_cache: Arc<Mutex<HashMap<String, CacheEntry<Vec<ColumnInfo>>>>>,
    pub(crate) indexed_columns_cache: Arc<Mutex<HashMap<String, CacheEntry<Vec<String>>>>>,
    pub(crate) composite_indexes_cache: Arc<Mutex<HashMap<String, CacheEntry<Vec<IndexDef>>>>>,
}

/// Simple TTL cache helper. Returns cached data if fresh, otherwise fetches,
/// stores, and returns.
/// When `cache_ttl == Duration::ZERO`, always re-fetches (cache disabled).
pub(crate) async fn get_cached_or_refresh<T, F, Fut>(
    cache: Arc<Mutex<HashMap<String, CacheEntry<T>>>>,
    cache_key: String,
    cache_ttl: Duration,
    fetch_fn: F,
) -> Result<T>
where
    T: Clone + Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    // Check cache under lock, return if fresh
    {
        let guard = cache.lock().await;
        if let Some(entry) = guard.get(&cache_key) {
            if cache_ttl > Duration::ZERO && entry.fetched_at.elapsed() < cache_ttl {
                return Ok(entry.data.clone());
            }
        }
    }

    // Fetch new data (lock released so we don't block readers during I/O)
    let data = fetch_fn().await?;

    // Store in cache if caching is enabled
    if cache_ttl > Duration::ZERO {
        let mut guard = cache.lock().await;
        guard.insert(
            cache_key,
            CacheEntry {
                data: data.clone(),
                fetched_at: Instant::now(),
            },
        );
    }

    Ok(data)
}

pub struct SchemaIntrospector {
    pub(crate) inner: Arc<SchemaCache>,
}

/// Returns true if the cache key's table segment matches `table` (case-insensitive).
/// Cache keys have the form "{database}\t{table}" where database may be empty.
/// Tab is used as separator because it is not a valid MySQL identifier character.
fn key_matches_table(key: &str, table: &str) -> bool {
    let key_table = key.split_once('\t').map(|x| x.1).unwrap_or(key);
    key_table.eq_ignore_ascii_case(table)
}

impl SchemaIntrospector {
    /// Create a new SchemaIntrospector with a PoolHandle and backend reference.
    pub fn new_with_backend(
        pool: PoolHandle,
        backend: Arc<dyn Backend>,
        cache_ttl_secs: u64,
    ) -> Self {
        Self {
            inner: Arc::new(SchemaCache {
                pool,
                backend,
                cache_ttl: Duration::from_secs(cache_ttl_secs),
                tables_cache: Arc::new(Mutex::new(HashMap::new())),
                columns_cache: Arc::new(Mutex::new(HashMap::new())),
                indexed_columns_cache: Arc::new(Mutex::new(HashMap::new())),
                composite_indexes_cache: Arc::new(Mutex::new(HashMap::new())),
            }),
        }
    }

    /// Create a new SchemaIntrospector from an Arc<MySqlPool> (backward compatible).
    #[cfg(feature = "mysql")]
    pub fn new(pool: Arc<sqlx::MySqlPool>, cache_ttl_secs: u64) -> Self {
        let handle = PoolHandle::new(Box::new(crate::backend::mysql::MySqlPoolWrapper::new(
            (*pool).clone(),
        )));
        let backend: Arc<dyn Backend> = Arc::new(crate::backend::mysql::MySqlBackend::new());
        Self::new_with_backend(handle, backend, cache_ttl_secs)
    }

    pub async fn list_tables(&self, database: Option<&str>) -> Result<Vec<TableInfo>> {
        let cache_key = database.unwrap_or("").to_owned();
        let pool = self.inner.pool.clone();
        let backend = self.inner.backend.clone();
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.tables_cache),
            cache_key,
            self.inner.cache_ttl,
            move || {
                let pool = pool.clone();
                let backend = backend.clone();
                async move { backend.fetch_tables(&pool, owned_database.as_deref()).await }
            },
        )
        .await
    }

    /// Return all columns that have at least one index on the given table.
    pub async fn list_indexed_columns(
        &self,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<String>> {
        let cache_key = format!("{}\t{}", database.unwrap_or(""), table);
        let pool = self.inner.pool.clone();
        let backend = self.inner.backend.clone();
        let owned_table = table.to_owned();
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.indexed_columns_cache),
            cache_key,
            self.inner.cache_ttl,
            move || {
                let pool = pool.clone();
                let backend = backend.clone();
                async move {
                    backend
                        .fetch_indexed_columns(&pool, &owned_table, owned_database.as_deref())
                        .await
                }
            },
        )
        .await
    }

    /// Return composite index information for the given table.
    pub async fn list_composite_indexes(
        &self,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<IndexDef>> {
        let cache_key = format!("{}\t{}", database.unwrap_or(""), table);
        let pool = self.inner.pool.clone();
        let backend = self.inner.backend.clone();
        let owned_table = table.to_owned();
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.composite_indexes_cache),
            cache_key,
            self.inner.cache_ttl,
            move || {
                let pool = pool.clone();
                let backend = backend.clone();
                async move {
                    backend
                        .fetch_composite_indexes(&pool, &owned_table, owned_database.as_deref())
                        .await
                }
            },
        )
        .await
    }

    pub async fn get_columns(
        &self,
        table_name: &str,
        database: Option<&str>,
    ) -> Result<Vec<ColumnInfo>> {
        let cache_key = format!("{}\t{}", database.unwrap_or(""), table_name);
        let pool = self.inner.pool.clone();
        let backend = self.inner.backend.clone();
        let owned_table = table_name.to_owned();
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.columns_cache),
            cache_key,
            self.inner.cache_ttl,
            move || {
                let pool = pool.clone();
                let backend = backend.clone();
                async move {
                    backend
                        .fetch_columns(&pool, &owned_table, owned_database.as_deref())
                        .await
                }
            },
        )
        .await
    }

    /// Generate schema-aware index suggestions for a query with a full table scan.
    pub async fn generate_index_suggestions(
        &self,
        table: &str,
        database: Option<&str>,
        where_cols: &[String],
    ) -> Vec<String> {
        if where_cols.is_empty() {
            return vec![];
        }

        let indexed_cols = self
            .list_indexed_columns(table, database)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "index suggestions: failed to list indexed columns for {}: {}",
                    table,
                    e
                );
                Vec::new()
            });
        let composite_indexes = self
            .list_composite_indexes(table, database)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "index suggestions: failed to list composite indexes for {}: {}",
                    table,
                    e
                );
                Vec::new()
            });
        let col_info: std::collections::HashMap<String, ColumnInfo> = self
            .get_columns(table, database)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "index suggestions: failed to get columns for {}: {}",
                    table,
                    e
                );
                Vec::new()
            })
            .into_iter()
            .map(|c| (c.name.to_lowercase(), c))
            .collect();

        let mut suggestions: Vec<String> = Vec::new();

        let indexed_set: std::collections::HashSet<String> =
            indexed_cols.iter().map(|s| s.to_lowercase()).collect();
        let unindexed_cols: Vec<&String> = where_cols
            .iter()
            .filter(|col| !indexed_set.contains(&col.to_lowercase()))
            .collect();

        // Use the backend's quote_identifier for proper escaping
        let esc = |s: &str| self.inner.backend.quote_identifier(s);
        // Sanitize a string for use as part of an index name (alphanumeric + underscore only).
        let safe_name = |s: &str| -> String {
            s.chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '_' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect()
        };
        if unindexed_cols.len() >= 2 {
            let where_col_set: std::collections::HashSet<String> =
                where_cols.iter().map(|s| s.to_lowercase()).collect();
            let covered_by_existing = composite_indexes.iter().any(|idx| {
                if idx.columns.len() < where_col_set.len() {
                    return false;
                }
                let prefix_set: std::collections::HashSet<String> = idx.columns
                    [..where_col_set.len()]
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
                    "WHERE columns on {} are covered by an existing composite index. No new index needed.",
                    esc(table)
                ));
            }
        } else {
            for col in &unindexed_cols {
                let low_card = col_info
                    .get(&col.to_lowercase())
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

        suggestions
    }

    /// Invalidate cached column data for a specific table (case-insensitive match on the
    /// table-name segment of the cache key, ignoring the database qualifier).
    pub async fn invalidate_table(&self, table: &str, database: Option<&str>) {
        if table.is_empty() {
            tracing::warn!("invalidate_table called with empty table name; ignoring");
            return;
        }

        let mut columns_cache = self.inner.columns_cache.lock().await;
        let mut indexed_columns_cache = self.inner.indexed_columns_cache.lock().await;
        let mut composite_indexes_cache = self.inner.composite_indexes_cache.lock().await;
        let mut tables_cache = self.inner.tables_cache.lock().await;

        columns_cache.retain(|key, _| !key_matches_table(key, table));
        indexed_columns_cache.retain(|key, _| !key_matches_table(key, table));
        composite_indexes_cache.retain(|key, _| !key_matches_table(key, table));

        match database {
            Some(db) => {
                tables_cache.remove(db);
            }
            None => {
                tables_cache.remove("");
            }
        }
    }

    /// Invalidate ALL cached schema data (tables list + all column caches).
    pub async fn invalidate_all(&self) {
        let mut columns_cache = self.inner.columns_cache.lock().await;
        let mut indexed_columns_cache = self.inner.indexed_columns_cache.lock().await;
        let mut composite_indexes_cache = self.inner.composite_indexes_cache.lock().await;
        let mut tables_cache = self.inner.tables_cache.lock().await;

        columns_cache.clear();
        indexed_columns_cache.clear();
        composite_indexes_cache.clear();
        tables_cache.clear();
    }

    /// Get the pool handle (for use by server handlers that need raw pool access).
    pub fn pool(&self) -> &PoolHandle {
        &self.inner.pool
    }

    /// Get a reference to the backend (for use by server handlers).
    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.inner.backend
    }
}
