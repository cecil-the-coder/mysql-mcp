use std::collections::{HashMap, HashSet};
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

/// Shared cache state for SchemaIntrospector, kept behind Arc so it can be
/// moved into background tokio::spawn tasks.
///
/// All HashMap caches are stored as Arc<Mutex<...>> so they can be cloned
/// cheaply into spawned background-refresh tasks without cloning the whole
/// SchemaCache.
pub(crate) struct SchemaCache {
    pub(crate) pool: Arc<sqlx::MySqlPool>,
    pub(crate) cache_ttl: Duration,
    /// Tables list cache, keyed by database name (empty string when None).
    pub(crate) tables_cache: Arc<Mutex<HashMap<String, CacheEntry<Vec<TableInfo>>>>>,
    pub(crate) columns_cache: Arc<Mutex<HashMap<String, CacheEntry<Vec<ColumnInfo>>>>>,
    /// Cache for list_indexed_columns() results, keyed by "{database}.{table}".
    pub(crate) indexed_columns_cache: Arc<Mutex<HashMap<String, CacheEntry<Vec<String>>>>>,
    /// Dedicated cache for list_composite_indexes(), keyed by "{database}.{table}".
    pub(crate) composite_indexes_cache: Arc<Mutex<HashMap<String, CacheEntry<Vec<IndexDef>>>>>,
    /// Tracks which tables cache keys are currently being refreshed in the background.
    pub(crate) tables_refreshing: Arc<Mutex<HashSet<String>>>,
    /// Tracks which column cache keys are currently being refreshed in the background.
    pub(crate) columns_refreshing: Arc<Mutex<HashSet<String>>>,
    /// Tracks which indexed_columns cache keys are currently being refreshed in the background.
    pub(crate) indexed_columns_refreshing: Arc<Mutex<HashSet<String>>>,
    /// Tracks which composite_indexes cache keys are currently being refreshed in the background.
    pub(crate) composite_indexes_refreshing: Arc<Mutex<HashSet<String>>>,
}

/// Generic stale-while-revalidate cache helper for HashMap-based caches.
///
/// Checks `cache` for a fresh entry under `cache_key`. If an entry exists but is
/// stale and `cache_ttl > 0`, it spawns a background refresh via `fetch_fn` and
/// returns the stale data immediately. If no cached entry exists (cold start) or
/// `cache_ttl == 0`, it fetches synchronously and stores the result.
///
/// Concurrent background refreshes for the same key are deduplicated via `refreshing`.
/// Both `cache` and `refreshing` are `Arc`-wrapped so they can be cloned into
/// the spawned task.
pub(crate) async fn get_cached_or_refresh<T, F, Fut>(
    cache: Arc<Mutex<HashMap<String, CacheEntry<T>>>>,
    refreshing: Arc<Mutex<HashSet<String>>>,
    cache_key: String,
    cache_ttl: Duration,
    fetch_fn: F,
) -> Result<T>
where
    T: Clone + Send + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    // Phase 1: Check if a fresh or stale entry exists.
    {
        let cache_guard = cache.lock().await;
        if let Some(entry) = cache_guard.get(&cache_key) {
            if entry.fetched_at.elapsed() < cache_ttl {
                // Cache hit within TTL: return immediately.
                return Ok(entry.data.clone());
            }
            // Cache is stale but has a value.
            // If TTL > 0: stale-while-revalidate — return stale immediately,
            // spawn a background refresh.
            // If TTL == 0: always do a synchronous refresh (no stale serving).
            if cache_ttl > Duration::ZERO {
                let stale_data = entry.data.clone();
                drop(cache_guard); // release cache lock before acquiring refreshing lock

                let mut refreshing_guard = refreshing.lock().await;
                if !refreshing_guard.contains(&cache_key) {
                    refreshing_guard.insert(cache_key.clone());
                    drop(refreshing_guard);

                    let owned_key = cache_key.clone();
                    let cache_spawn = Arc::clone(&cache);
                    let refreshing_spawn = Arc::clone(&refreshing);
                    tokio::spawn(async move {
                        match fetch_fn().await {
                            Ok(data) => {
                                let mut c = cache_spawn.lock().await;
                                c.insert(owned_key.clone(), CacheEntry {
                                    data,
                                    fetched_at: Instant::now(),
                                });
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Schema cache background refresh failed for key '{}': {}",
                                    owned_key, e
                                );
                                // Leave the stale cache entry in place on error.
                            }
                        }
                        let mut r = refreshing_spawn.lock().await;
                        r.remove(&owned_key);
                    });
                }
                return Ok(stale_data);
            }
            // TTL == 0: fall through to synchronous fetch below.
        }
    }

    // Phase 2: Cold start (no cached value) or TTL==0 (always re-fetch): block synchronously.
    let data = fetch_fn().await?;

    {
        let mut cache_guard = cache.lock().await;
        cache_guard.insert(cache_key, CacheEntry { data: data.clone(), fetched_at: Instant::now() });
    }

    Ok(data)
}
