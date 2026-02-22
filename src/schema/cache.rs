use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use anyhow::Result;

use super::{TableInfo, ColumnInfo, IndexDef};

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

/// Generic TTL cache helper. Returns a cached entry if still fresh; otherwise
/// fetches synchronously, stores the result, and returns it.
/// When `cache_ttl == Duration::ZERO`, always re-fetches (cache disabled).
pub(crate) async fn get_cached_or_refresh<T, F, Fut>(
    cache: Arc<Mutex<HashMap<String, CacheEntry<T>>>>,
    cache_key: String,
    cache_ttl: Duration,
    fetch_fn: F,
) -> Result<T>
where
    T: Clone + Send + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    {
        let cache_guard = cache.lock().await;
        if let Some(entry) = cache_guard.get(&cache_key) {
            if entry.fetched_at.elapsed() < cache_ttl {
                return Ok(entry.data.clone());
            }
        }
    }

    let data = fetch_fn().await?;
    {
        let mut cache_guard = cache.lock().await;
        cache_guard.insert(cache_key, CacheEntry { data: data.clone(), fetched_at: Instant::now() });
    }
    Ok(data)
}
