use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use sqlx::MySqlPool;
use anyhow::Result;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub schema: String,
    pub row_count: Option<i64>,
    pub data_size_bytes: Option<i64>,
    pub create_time: Option<String>,
    pub update_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub column_default: Option<String>,
    pub column_key: Option<String>,
    pub extra: Option<String>,
}

struct CacheEntry<T> {
    data: T,
    fetched_at: Instant,
}

// MySQL information_schema columns (TABLE_NAME, DATA_TYPE, COLUMN_KEY, etc.) are
// sometimes returned as binary blobs by sqlx. These helpers try String first,
// then fall back to Vec<u8> -> UTF-8 so callers always get a usable value.
fn is_col_str(row: &sqlx::mysql::MySqlRow, col: &str) -> String {
    use sqlx::Row;
    row.try_get::<String, _>(col)
        .or_else(|_| {
            row.try_get::<Vec<u8>, _>(col)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        })
        .unwrap_or_default()
}

fn is_col_str_opt(row: &sqlx::mysql::MySqlRow, col: &str) -> Option<String> {
    use sqlx::Row;
    let s = row.try_get::<Option<String>, _>(col)
        .ok()
        .flatten()
        .or_else(|| {
            row.try_get::<Option<Vec<u8>>, _>(col)
                .ok()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        })?;
    if s.is_empty() { None } else { Some(s) }
}

/// Escape a MySQL identifier for use in backtick-quoted contexts.
/// Doubles any backtick characters within the name.
fn escape_mysql_identifier(name: &str) -> String {
    name.replace('`', "``")
}

/// Shared inner state for SchemaIntrospector, kept behind Arc so it can be
/// moved into background tokio::spawn tasks.
struct Inner {
    pool: Arc<MySqlPool>,
    cache_ttl: Duration,
    tables_cache: Mutex<Option<CacheEntry<Vec<TableInfo>>>>,
    columns_cache: Mutex<HashMap<String, CacheEntry<Vec<ColumnInfo>>>>,
    /// Prevents concurrent background refreshes of the tables cache.
    tables_refreshing: AtomicBool,
    /// Tracks which column cache keys are currently being refreshed in the background.
    columns_refreshing: Mutex<HashSet<String>>,
}

pub struct SchemaIntrospector {
    inner: Arc<Inner>,
}

impl SchemaIntrospector {
    pub fn new(pool: Arc<MySqlPool>, cache_ttl_secs: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool,
                cache_ttl: Duration::from_secs(cache_ttl_secs),
                tables_cache: Mutex::new(None),
                columns_cache: Mutex::new(HashMap::new()),
                tables_refreshing: AtomicBool::new(false),
                columns_refreshing: Mutex::new(HashSet::new()),
            }),
        }
    }

    pub async fn list_tables(&self, database: Option<&str>) -> Result<Vec<TableInfo>> {
        // Check cache first.
        {
            let cache = self.inner.tables_cache.lock().await;
            if let Some(entry) = &*cache {
                if entry.fetched_at.elapsed() < self.inner.cache_ttl {
                    // Cache hit within TTL: return immediately.
                    return Ok(entry.data.clone());
                }
                // Cache is stale but has a value.
                // If TTL > 0: stale-while-revalidate — return stale immediately,
                // spawn a background refresh.
                // If TTL == 0: always do a synchronous refresh (no stale serving).
                if self.inner.cache_ttl > Duration::ZERO {
                    let stale_data = entry.data.clone();
                    // Only one background refresh at a time.
                    if !self.inner.tables_refreshing.swap(true, Ordering::AcqRel) {
                        let inner = Arc::clone(&self.inner);
                        let owned_database = database.map(|s| s.to_owned());
                        tokio::spawn(async move {
                            let db_ref = owned_database.as_deref();
                            match Inner::fetch_tables(&inner.pool, db_ref).await {
                                Ok(tables) => {
                                    let mut cache = inner.tables_cache.lock().await;
                                    *cache = Some(CacheEntry {
                                        data: tables,
                                        fetched_at: Instant::now(),
                                    });
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Schema cache refresh failed for table list (db={:?}): {}",
                                        owned_database, e
                                    );
                                    // Leave the stale cache in place on error.
                                }
                            }
                            inner.tables_refreshing.store(false, Ordering::Release);
                        });
                    }
                    return Ok(stale_data);
                }
            }
        }

        // Cold start (no cached value) or TTL==0 (always re-fetch): block synchronously.
        let tables = Inner::fetch_tables(&self.inner.pool, database).await?;

        {
            let mut cache = self.inner.tables_cache.lock().await;
            *cache = Some(CacheEntry { data: tables.clone(), fetched_at: Instant::now() });
        }

        Ok(tables)
    }

    /// Return all columns that have at least one index on the given table.
    /// Runs `SHOW INDEX FROM {table}` (qualified with database if provided).
    /// No caching — this is only called when EXPLAIN already detected a problem.
    pub async fn list_indexed_columns(&self, table: &str, database: Option<&str>) -> Result<Vec<String>> {
        let qualified = match database {
            Some(db) => format!("`{}`.`{}`", escape_mysql_identifier(db), escape_mysql_identifier(table)),
            None => format!("`{}`", escape_mysql_identifier(table)),
        };
        let sql = format!("SHOW INDEX FROM {}", qualified);
        let rows = sqlx::query(&sql).fetch_all(self.inner.pool.as_ref()).await?;
        let mut cols: Vec<String> = Vec::new();
        for row in &rows {
            let col_name = is_col_str(row, "Column_name");
            if !col_name.is_empty() && !cols.iter().any(|c: &String| c.eq_ignore_ascii_case(&col_name)) {
                cols.push(col_name);
            }
        }
        Ok(cols)
    }

    /// Invalidate cached column data for a specific table (case-insensitive match on the
    /// table-name segment of the cache key, ignoring the database qualifier).
    /// Use after DDL that targets a known table (CREATE TABLE, ALTER TABLE, TRUNCATE).
    pub async fn invalidate_table(&self, table: &str) {
        if table.is_empty() {
            // No table name known — fall back to full invalidation.
            self.invalidate_all().await;
            return;
        }
        let mut cache = self.inner.columns_cache.lock().await;
        // Cache keys have the form "{database}.{table}" where database may be empty.
        // We remove every entry whose table-name segment (after the first '.') matches.
        cache.retain(|key, _| {
            let key_table = key.splitn(2, '.').nth(1).unwrap_or(key.as_str());
            !key_table.eq_ignore_ascii_case(table)
        });
        // Also clear the tables list so row-count / existence info is refreshed.
        let mut tables_cache = self.inner.tables_cache.lock().await;
        *tables_cache = None;
    }

    /// Invalidate ALL cached schema data (tables list + all column caches).
    /// Use after DDL that may affect multiple tables (e.g., DROP DATABASE, DROP TABLE).
    pub async fn invalidate_all(&self) {
        {
            let mut tables_cache = self.inner.tables_cache.lock().await;
            *tables_cache = None;
        }
        {
            let mut columns_cache = self.inner.columns_cache.lock().await;
            columns_cache.clear();
        }
    }

    pub async fn get_columns(&self, table_name: &str, database: Option<&str>) -> Result<Vec<ColumnInfo>> {
        let cache_key = format!("{}.{}", database.unwrap_or(""), table_name);

        {
            let cache = self.inner.columns_cache.lock().await;
            if let Some(entry) = cache.get(&cache_key) {
                if entry.fetched_at.elapsed() < self.inner.cache_ttl {
                    // Cache hit within TTL: return immediately.
                    return Ok(entry.data.clone());
                }
                // Cache is stale but has a value.
                // If TTL > 0: stale-while-revalidate — return stale immediately,
                // spawn a background refresh.
                // If TTL == 0: always do a synchronous refresh (no stale serving).
                if self.inner.cache_ttl > Duration::ZERO {
                    let stale_data = entry.data.clone();
                    // Only one background refresh per cache key at a time.
                    let mut refreshing = self.inner.columns_refreshing.lock().await;
                    if !refreshing.contains(&cache_key) {
                        refreshing.insert(cache_key.clone());
                        drop(refreshing); // release lock before spawning
                        let inner = Arc::clone(&self.inner);
                        let owned_table = table_name.to_owned();
                        let owned_database = database.map(|s| s.to_owned());
                        let owned_key = cache_key.clone();
                        tokio::spawn(async move {
                            let db_ref = owned_database.as_deref();
                            match Inner::fetch_columns(&inner.pool, &owned_table, db_ref).await {
                                Ok(columns) => {
                                    let mut cache = inner.columns_cache.lock().await;
                                    cache.insert(owned_key.clone(), CacheEntry {
                                        data: columns,
                                        fetched_at: Instant::now(),
                                    });
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Schema cache refresh failed for columns ({}.{}): {}",
                                        owned_database.as_deref().unwrap_or(""), owned_table, e
                                    );
                                    // Leave the stale cache in place on error.
                                }
                            }
                            let mut refreshing = inner.columns_refreshing.lock().await;
                            refreshing.remove(&owned_key);
                        });
                    }
                    return Ok(stale_data);
                }
            }
        }

        // Cold start (no cached value) or TTL==0 (always re-fetch): block synchronously.
        let columns = Inner::fetch_columns(&self.inner.pool, table_name, database).await?;

        {
            let mut cache = self.inner.columns_cache.lock().await;
            cache.insert(cache_key, CacheEntry { data: columns.clone(), fetched_at: Instant::now() });
        }

        Ok(columns)
    }
}

impl Inner {
    async fn fetch_tables(pool: &MySqlPool, database: Option<&str>) -> Result<Vec<TableInfo>> {
        let rows = if let Some(db) = database {
            sqlx::query(
                r#"SELECT
                TABLE_NAME as name,
                TABLE_SCHEMA as `schema`,
                TABLE_ROWS as row_count,
                DATA_LENGTH as data_size_bytes,
                CREATE_TIME as create_time,
                UPDATE_TIME as update_time
            FROM information_schema.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND table_schema = ?
            ORDER BY TABLE_SCHEMA, TABLE_NAME"#,
            )
            .bind(db)
            .fetch_all(pool)
            .await?
        } else {
            sqlx::query(
                r#"SELECT
                TABLE_NAME as name,
                TABLE_SCHEMA as `schema`,
                TABLE_ROWS as row_count,
                DATA_LENGTH as data_size_bytes,
                CREATE_TIME as create_time,
                UPDATE_TIME as update_time
            FROM information_schema.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
            ORDER BY TABLE_SCHEMA, TABLE_NAME"#,
            )
            .fetch_all(pool)
            .await?
        };

        let tables = rows.iter().map(|row| {
            use sqlx::Row;
            TableInfo {
                name: is_col_str(row, "name"),
                schema: is_col_str(row, "schema"),
                row_count: row.try_get("row_count").ok(),
                data_size_bytes: row.try_get("data_size_bytes").ok(),
                create_time: row.try_get::<Option<chrono::NaiveDateTime>, _>("create_time")
                    .ok().flatten().map(|d| d.to_string()),
                update_time: row.try_get::<Option<chrono::NaiveDateTime>, _>("update_time")
                    .ok().flatten().map(|d| d.to_string()),
            }
        }).collect();

        Ok(tables)
    }

    async fn fetch_columns(pool: &MySqlPool, table_name: &str, database: Option<&str>) -> Result<Vec<ColumnInfo>> {
        let rows = if let Some(db) = database {
            sqlx::query(
                r#"SELECT
                COLUMN_NAME as name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as column_default,
                COLUMN_KEY as column_key,
                EXTRA as extra
            FROM information_schema.COLUMNS
            WHERE TABLE_NAME = ?
            AND TABLE_SCHEMA = ?
            ORDER BY ORDINAL_POSITION"#,
            )
            .bind(table_name)
            .bind(db)
            .fetch_all(pool)
            .await?
        } else {
            sqlx::query(
                r#"SELECT
                COLUMN_NAME as name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as column_default,
                COLUMN_KEY as column_key,
                EXTRA as extra
            FROM information_schema.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION"#,
            )
            .bind(table_name)
            .fetch_all(pool)
            .await?
        };

        let columns = rows.iter().map(|row| {
            let nullable_str = is_col_str(row, "is_nullable");
            ColumnInfo {
                name: is_col_str(row, "name"),
                data_type: is_col_str(row, "data_type"),
                is_nullable: nullable_str == "YES",
                column_default: is_col_str_opt(row, "column_default"),
                column_key: is_col_str_opt(row, "column_key"),
                extra: {
                    let e = is_col_str(row, "extra");
                    if e.is_empty() { None } else { Some(e) }
                },
            }
        }).collect();

        Ok(columns)
    }
}


/// Detailed schema metadata for a single table: columns, indexes, foreign keys, and size estimate.
pub async fn get_schema_info(
    pool: &sqlx::MySqlPool,
    table_name: &str,
    database: Option<&str>,
    include_indexes: bool,
    include_foreign_keys: bool,
    include_size: bool,
) -> anyhow::Result<serde_json::Value> {
    // Columns (always included)
    let columns = Inner::fetch_columns(pool, table_name, database).await?;

    // Indexes
    let indexes: serde_json::Value = if include_indexes {
        let rows = if let Some(db) = database {
            sqlx::query(
                "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, INDEX_TYPE, NULLABLE \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ? \
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            )
            .bind(table_name)
            .bind(db)
            .fetch_all(pool)
            .await?
        } else {
            sqlx::query(
                "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, INDEX_TYPE, NULLABLE \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_NAME = ? \
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            )
            .bind(table_name)
            .fetch_all(pool)
            .await?
        };
        let mut idx_map: std::collections::BTreeMap<String, serde_json::Value> = std::collections::BTreeMap::new();
        for row in &rows {
            use sqlx::Row;
            let name: String = is_col_str(row, "INDEX_NAME");
            let non_unique: i64 = row.try_get("NON_UNIQUE").unwrap_or(1);
            let col: String = is_col_str(row, "COLUMN_NAME");
            let idx_type: String = is_col_str(row, "INDEX_TYPE");
            let nullable: String = is_col_str(row, "NULLABLE");
            let entry = idx_map.entry(name.clone()).or_insert_with(|| serde_json::json!({
                "name": name,
                "unique": non_unique == 0,
                "type": idx_type,
                "columns": [],
            }));
            if let Some(cols) = entry.get_mut("columns").and_then(|v| v.as_array_mut()) {
                cols.push(serde_json::json!({
                    "column": col,
                    "nullable": nullable == "YES",
                }));
            }
        }
        serde_json::Value::Array(idx_map.into_values().collect())
    } else {
        serde_json::Value::Null
    };

    // Foreign keys
    let foreign_keys: serde_json::Value = if include_foreign_keys {
        let rows = if let Some(db) = database {
            sqlx::query(
                "SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, \
                        kcu.REFERENCED_COLUMN_NAME, rc.UPDATE_RULE, rc.DELETE_RULE \
                 FROM information_schema.KEY_COLUMN_USAGE kcu \
                 JOIN information_schema.REFERENTIAL_CONSTRAINTS rc \
                   ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
                  AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA \
                 WHERE kcu.TABLE_NAME = ? AND kcu.TABLE_SCHEMA = ? \
                   AND kcu.REFERENCED_TABLE_NAME IS NOT NULL",
            )
            .bind(table_name)
            .bind(db)
            .fetch_all(pool)
            .await?
        } else {
            sqlx::query(
                "SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, \
                        kcu.REFERENCED_COLUMN_NAME, rc.UPDATE_RULE, rc.DELETE_RULE \
                 FROM information_schema.KEY_COLUMN_USAGE kcu \
                 JOIN information_schema.REFERENTIAL_CONSTRAINTS rc \
                   ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
                  AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA \
                 WHERE kcu.TABLE_NAME = ? \
                   AND kcu.REFERENCED_TABLE_NAME IS NOT NULL",
            )
            .bind(table_name)
            .fetch_all(pool)
            .await?
        };
        let fks: Vec<serde_json::Value> = rows.iter().map(|row| {
            serde_json::json!({
                "constraint": is_col_str(row, "CONSTRAINT_NAME"),
                "column": is_col_str(row, "COLUMN_NAME"),
                "references_table": is_col_str(row, "REFERENCED_TABLE_NAME"),
                "references_column": is_col_str(row, "REFERENCED_COLUMN_NAME"),
                "on_update": is_col_str(row, "UPDATE_RULE"),
                "on_delete": is_col_str(row, "DELETE_RULE"),
            })
        }).collect();
        serde_json::Value::Array(fks)
    } else {
        serde_json::Value::Null
    };

    // Table size estimate
    let size: serde_json::Value = if include_size {
        let size_result = if let Some(db) = database {
            sqlx::query(
                "SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH \
                 FROM information_schema.TABLES \
                 WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?",
            )
            .bind(table_name)
            .bind(db)
            .fetch_one(pool)
            .await
        } else {
            sqlx::query(
                "SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH \
                 FROM information_schema.TABLES \
                 WHERE TABLE_NAME = ?",
            )
            .bind(table_name)
            .fetch_one(pool)
            .await
        };
        if let Ok(row) = size_result {
            use sqlx::Row;
            let table_rows: Option<u64> = row.try_get("TABLE_ROWS").ok();
            let data_len: Option<u64> = row.try_get("DATA_LENGTH").ok();
            let idx_len: Option<u64> = row.try_get("INDEX_LENGTH").ok();
            serde_json::json!({
                "estimated_rows": table_rows,
                "data_bytes": data_len,
                "index_bytes": idx_len,
            })
        } else {
            serde_json::Value::Null
        }
    } else {
        serde_json::Value::Null
    };

    // Build column JSON
    let cols_json: Vec<serde_json::Value> = columns.iter().map(|c| serde_json::json!({
        "name": c.name,
        "type": c.data_type,
        "nullable": c.is_nullable,
        "default": c.column_default,
        "key": c.column_key,
        "extra": c.extra,
    })).collect();

    let mut result = serde_json::json!({
        "table": table_name,
        "columns": cols_json,
    });
    if !indexes.is_null() {
        result["indexes"] = indexes;
    }
    if !foreign_keys.is_null() {
        result["foreign_keys"] = foreign_keys;
    }
    if !size.is_null() {
        result["size"] = size;
    }
    Ok(result)
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::test_helpers::setup_test_db;

    // mysql-mcp-t7a: list tables returns results
    #[tokio::test]
    async fn test_list_tables_returns_results() {
        let Some(test_db) = setup_test_db().await else { return; };
        let pool = Arc::new(test_db.pool.clone());
        let introspector = SchemaIntrospector::new(pool, 60);
        let db = test_db.config.connection.database.as_deref();
        let tables = introspector.list_tables(db).await;
        assert!(tables.is_ok());
    }

    // mysql-mcp-63f: schema cache TTL=0 disables caching
    #[tokio::test]
    async fn test_schema_cache_ttl_zero_disables_cache() {
        let Some(test_db) = setup_test_db().await else { return; };
        let pool = Arc::new(test_db.pool.clone());
        let db = test_db.config.connection.database.as_deref();

        // Use a unique table name to avoid races with other concurrent tests.
        let marker = format!("_cache_ttl_test_{}", std::process::id());

        // Ensure the marker table doesn't exist before we start.
        sqlx::query(&format!("DROP TABLE IF EXISTS {marker}"))
            .execute(pool.as_ref()).await.unwrap();

        // TTL=0 means instant expiry; every call re-fetches from the DB.
        let introspector = SchemaIntrospector::new(pool.clone(), 0);

        // First call: marker table doesn't exist yet.
        let tables_before = introspector.list_tables(db).await.unwrap();

        // Create the marker table between calls.
        sqlx::query(&format!("CREATE TABLE {marker} (id INT)"))
            .execute(pool.as_ref()).await.unwrap();

        // Second call: must re-fetch (TTL=0), so it should see the new table.
        let tables_after = introspector.list_tables(db).await.unwrap();

        // Cleanup.
        sqlx::query(&format!("DROP TABLE IF EXISTS {marker}"))
            .execute(pool.as_ref()).await.ok();

        let before_names: Vec<&str> = tables_before.iter().map(|t| t.name.as_str()).collect();
        let after_names: Vec<&str> = tables_after.iter().map(|t| t.name.as_str()).collect();
        assert!(!before_names.contains(&marker.as_str()),
            "marker table should not exist before creation");
        assert!(after_names.contains(&marker.as_str()),
            "TTL=0 must re-fetch: new table should be visible after creation");
    }

    // mysql-mcp-1au: get columns for a known table
    #[tokio::test]
    async fn test_get_columns_for_table() {
        let Some(test_db) = setup_test_db().await else { return; };
        let pool = &test_db.pool;
        let db = test_db.config.connection.database.as_deref();

        sqlx::query("CREATE TABLE IF NOT EXISTS test_schema_cols (id INT PRIMARY KEY, name VARCHAR(100))")
            .execute(pool)
            .await
            .unwrap();

        let arc_pool = Arc::new(pool.clone());
        let introspector = SchemaIntrospector::new(arc_pool.clone(), 60);
        let columns = introspector.get_columns("test_schema_cols", db).await;
        assert!(columns.is_ok());

        sqlx::query("DROP TABLE IF EXISTS test_schema_cols")
            .execute(arc_pool.as_ref())
            .await
            .ok();
    }

    // mysql-mcp-26b: pool behavior under concurrent load - stub
    #[tokio::test]
    async fn test_pool_concurrent_queries_stub() {
        eprintln!("TODO: requires special setup (concurrent load testing with controlled pool size)");
    }
}
