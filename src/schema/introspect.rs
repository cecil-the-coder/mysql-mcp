use anyhow::Result;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use super::fetch;
use super::{is_low_cardinality_type, ColumnInfo, IndexDef, TableInfo};

// ---------------------------------------------------------------------------
// Cache internals
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Cache lock ordering
// ---------------------------------------------------------------------------
//
// To prevent deadlocks when multiple methods acquire cache locks concurrently,
// ALL code paths that hold more than one cache lock simultaneously MUST
// acquire them in the following order (consistent with invalidate_table):
//
//   1. columns_cache
//   2. indexed_columns_cache
//   3. composite_indexes_cache
//   4. tables_cache
//
// If you add a new cache field, add it at an appropriate position.
// NEVER acquire locks in a different order, even temporarily.
// ---------------------------------------------------------------------------

/// Construct a cache key from database name (optional) and table name.
/// The format is "{database}\t{table}" where database may be empty.
/// Tab is used as separator; table names containing tabs are rejected to prevent
/// cache key collisions. MySQL allows tabs in quoted identifiers, but they are
/// extremely rare and not supported by this cache implementation.
fn make_cache_key(database: Option<&str>, table: &str) -> String {
    // Reject table names containing the separator character to prevent cache key
    // collisions. While rare, MySQL does allow tabs in quoted identifiers.
    if table.contains('\t') {
        tracing::warn!(
            "Table name contains tab character, which is not supported by schema cache: {}",
            &table[..table.len().min(100)]
        );
    }
    format!("{}\t{}", database.unwrap_or(""), table)
}

pub(crate) struct CacheEntry<T> {
    pub(crate) data: T,
    pub(crate) fetched_at: Instant,
}

/// Shared cache state for SchemaIntrospector.
pub(crate) struct SchemaCache {
    pub(crate) pool: Arc<sqlx::MySqlPool>,
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

    // Store in cache if caching is enabled.
    // Re-check under the write lock: if another caller already refreshed the
    // entry while we were fetching, return their result to avoid stampede.
    if cache_ttl > Duration::ZERO {
        let mut guard = cache.lock().await;
        if let Some(entry) = guard.get(&cache_key) {
            if entry.fetched_at.elapsed() < cache_ttl {
                return Ok(entry.data.clone());
            }
        }
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

/// Returns true if the cache key matches the given table (and optionally database).
/// Cache keys have the form "{database}\t{table}" where database may be empty.
/// Tab is used as separator because it is not a valid MySQL identifier character.
/// When `database` is Some, both database and table must match (case-insensitive).
/// When `database` is None, the key's database portion must be empty and the table must match (case-insensitive).
fn key_matches_table_and_db(key: &str, table: &str, database: Option<&str>) -> bool {
    match key.split_once('\t') {
        Some((key_db, key_table)) => {
            let table_matches = key_table.eq_ignore_ascii_case(table);
            match database {
                Some(db) => table_matches && key_db.eq_ignore_ascii_case(db),
                None => table_matches && key_db.is_empty(),
            }
        }
        None => {
            // Key has no tab separator - shouldn't happen for table-specific caches.
            // Log a warning to help diagnose cache integrity issues, and don't match
            // so the caller's retain() will keep the entry (though it should be removed
            // manually if this occurs).
            tracing::warn!(
                "malformed cache key '{}' lacks tab separator; cache entry may persist indefinitely",
                key
            );
            false
        }
    }
}

impl SchemaIntrospector {
    pub fn new(pool: Arc<sqlx::MySqlPool>, cache_ttl_secs: u64) -> Self {
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
                async move { fetch::fetch_tables(&pool, owned_database.as_deref()).await }
            },
        )
        .await
    }

    /// Return all columns that have at least one index on the given table.
    /// Runs `SHOW INDEX FROM {table}` (qualified with database if provided).
    /// Results are cached with the same TTL as the column cache.
    pub async fn list_indexed_columns(
        &self,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<String>> {
        let cache_key = make_cache_key(database, table);
        let pool = Arc::clone(&self.inner.pool);
        let owned_table = table.to_owned();
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.indexed_columns_cache),
            cache_key,
            self.inner.cache_ttl,
            move || async move {
                fetch::fetch_indexed_columns(&pool, &owned_table, owned_database.as_deref()).await
            },
        )
        .await
    }

    /// Return composite index information for the given table.
    /// Each entry represents one index: a named, ordered list of columns.
    /// Columns are ordered by their position within the index (SEQ_IN_INDEX).
    /// The PRIMARY key is included.
    /// Results are cached with the same TTL as `list_indexed_columns`.
    pub async fn list_composite_indexes(
        &self,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<IndexDef>> {
        let cache_key = make_cache_key(database, table);
        let pool = Arc::clone(&self.inner.pool);
        let owned_table = table.to_owned();
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.composite_indexes_cache),
            cache_key,
            self.inner.cache_ttl,
            move || async move {
                fetch::fetch_composite_indexes(&pool, &owned_table, owned_database.as_deref()).await
            },
        )
        .await
    }

    pub async fn get_columns(
        &self,
        table_name: &str,
        database: Option<&str>,
    ) -> Result<Vec<ColumnInfo>> {
        let cache_key = make_cache_key(database, table_name);
        let pool = Arc::clone(&self.inner.pool);
        let owned_table = table_name.to_owned();
        let owned_database = database.map(|s| s.to_owned());

        get_cached_or_refresh(
            Arc::clone(&self.inner.columns_cache),
            cache_key,
            self.inner.cache_ttl,
            move || async move {
                fetch::fetch_columns(&pool, &owned_table, owned_database.as_deref()).await
            },
        )
        .await
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

        // Run all three independent DB lookups concurrently to avoid sequential
        // round-trip latency when the cache is cold.
        let (indexed_cols_res, composite_indexes_res, col_info_res) = tokio::join!(
            self.list_indexed_columns(table, database),
            self.list_composite_indexes(table, database),
            self.get_columns(table, database),
        );

        let indexed_cols = indexed_cols_res.unwrap_or_else(|e| {
            tracing::warn!(
                "index suggestions: failed to list indexed columns for {}: {}",
                table,
                e
            );
            Vec::new()
        });
        let composite_indexes = composite_indexes_res.unwrap_or_else(|e| {
            tracing::warn!(
                "index suggestions: failed to list composite indexes for {}: {}",
                table,
                e
            );
            Vec::new()
        });
        let col_info: std::collections::HashMap<String, ColumnInfo> = col_info_res
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

        // Build a set of WHERE columns that are not yet individually indexed.
        // Use HashSet for O(1) lookups instead of O(n) linear search.
        let indexed_set: std::collections::HashSet<String> =
            indexed_cols.iter().map(|s| s.to_lowercase()).collect();
        let unindexed_cols: Vec<&String> = where_cols
            .iter()
            .filter(|col| !indexed_set.contains(&col.to_lowercase()))
            .collect();

        // --- Case 1: Composite index detection ---
        // If there are 2+ unindexed WHERE columns, check whether a composite index
        // would cover them rather than N individual indexes.
        // Pre-compute table name formats once (reused across branches).
        let esc = |s: &str| format!("`{}`", super::fetch::escape_mysql_identifier(s));
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
        let esc_table = esc(table);
        let safe_table = safe_name(table);

        if unindexed_cols.len() >= 2 {
            // An existing composite index covers the WHERE columns iff all WHERE columns
            // appear as a leading prefix of that index (B-tree indexes require leftmost prefix
            // for efficient range/equality filtering). Column ORDER within the prefix doesn't
            // matter for equality predicates, so we compare sets.
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
                // Build joined strings directly without intermediate Vec<String>.
                let mut cols_display = String::new();
                let mut esc_cols = String::new();
                let mut safe_cols = String::new();
                for (i, col) in unindexed_cols.iter().enumerate() {
                    if i > 0 {
                        cols_display.push_str(", ");
                        esc_cols.push_str(", ");
                        safe_cols.push('_');
                    }
                    cols_display.push_str(col);
                    esc_cols.push_str(&esc(col));
                    safe_cols.push_str(&safe_name(col));
                }
                suggestions.push(format!(
                    "Multiple unindexed WHERE columns on {}: [{}]. Consider a composite index: CREATE INDEX idx_{}_{} ON {}({});",
                    esc_table, cols_display, safe_table, safe_cols, esc_table, esc_cols
                ));
            } else {
                suggestions.push(format!(
                    "WHERE columns on {} are covered by an existing composite index. No new index needed.",
                    esc_table
                ));
            }
        } else {
            // Single unindexed column: emit the standard per-column suggestion.
            for col in &unindexed_cols {
                let esc_col = esc(col);
                let low_card = col_info
                    .get(&col.to_lowercase())
                    .map(|ci| is_low_cardinality_type(&ci.column_type))
                    .unwrap_or(false);
                if low_card {
                    suggestions.push(format!(
                        "Column {} in WHERE clause on table {} has no index, but its type has low cardinality (few distinct values). An index may not improve performance — the optimizer may prefer a full table scan. Consider filtering on a higher-cardinality column instead, or use a partial/functional index.",
                        esc_col, esc_table
                    ));
                } else {
                    suggestions.push(format!(
                        "Column {} in WHERE clause on table {} has no index. Consider: CREATE INDEX idx_{}_{} ON {}({});",
                        esc_col, esc_table, safe_table, safe_name(col), esc_table, esc_col
                    ));
                }
            }
        }

        suggestions
    }

    /// Invalidate cached column data for a specific table (case-insensitive match on
    /// both database and table-name segments of the cache key when database is provided).
    /// Use after DDL that targets a known table (CREATE TABLE, ALTER TABLE, TRUNCATE).
    ///
    /// Acquires all cache locks atomically to prevent readers from observing partially
    /// invalidated state (e.g., fresh columns but stale indexes).
    pub async fn invalidate_table(&self, table: &str, database: Option<&str>) {
        if table.trim().is_empty() {
            tracing::warn!("invalidate_table called with empty table name; ignoring");
            return;
        }

        // Lock order: columns -> indexed_columns -> composite_indexes -> tables.
        // This is consistent with invalidate_all to prevent deadlock.
        let mut columns_cache = self.inner.columns_cache.lock().await;
        let mut indexed_columns_cache = self.inner.indexed_columns_cache.lock().await;
        let mut composite_indexes_cache = self.inner.composite_indexes_cache.lock().await;
        let mut tables_cache = self.inner.tables_cache.lock().await;

        columns_cache.retain(|key, _| !key_matches_table_and_db(key, table, database));
        indexed_columns_cache.retain(|key, _| !key_matches_table_and_db(key, table, database));
        composite_indexes_cache.retain(|key, _| !key_matches_table_and_db(key, table, database));

        let cache_key = database.unwrap_or("");
        tables_cache.remove(cache_key);
    }

    /// Invalidate ALL cached schema data (tables list + all column caches).
    /// Use after DDL that may affect multiple tables (e.g., DROP DATABASE, DROP TABLE).
    ///
    /// Acquires all cache locks atomically to prevent readers from observing partially
    /// invalidated state.
    pub async fn invalidate_all(&self) {
        // Lock order: columns -> indexed_columns -> composite_indexes -> tables.
        // This is consistent with invalidate_table to prevent deadlock.
        let mut columns_cache = self.inner.columns_cache.lock().await;
        let mut indexed_columns_cache = self.inner.indexed_columns_cache.lock().await;
        let mut composite_indexes_cache = self.inner.composite_indexes_cache.lock().await;
        let mut tables_cache = self.inner.tables_cache.lock().await;

        columns_cache.clear();
        indexed_columns_cache.clear();
        composite_indexes_cache.clear();
        tables_cache.clear();
    }
}
