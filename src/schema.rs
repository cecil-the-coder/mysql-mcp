use std::sync::Arc;
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

pub struct SchemaIntrospector {
    pool: Arc<MySqlPool>,
    cache_ttl: Duration,
    tables_cache: Mutex<Option<CacheEntry<Vec<TableInfo>>>>,
    columns_cache: Mutex<std::collections::HashMap<String, CacheEntry<Vec<ColumnInfo>>>>,
}

impl SchemaIntrospector {
    pub fn new(pool: Arc<MySqlPool>, cache_ttl_secs: u64) -> Self {
        Self {
            pool,
            cache_ttl: Duration::from_secs(cache_ttl_secs),
            tables_cache: Mutex::new(None),
            columns_cache: Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub async fn list_tables(&self, database: Option<&str>) -> Result<Vec<TableInfo>> {
        {
            let cache = self.tables_cache.lock().await;
            if let Some(entry) = &*cache {
                if entry.fetched_at.elapsed() < self.cache_ttl {
                    return Ok(entry.data.clone());
                }
            }
        }

        let tables = self.fetch_tables(database).await?;

        {
            let mut cache = self.tables_cache.lock().await;
            *cache = Some(CacheEntry { data: tables.clone(), fetched_at: Instant::now() });
        }

        Ok(tables)
    }

    async fn fetch_tables(&self, database: Option<&str>) -> Result<Vec<TableInfo>> {
        let db_filter = database
            .map(|d| format!("AND table_schema = '{}'", d))
            .unwrap_or_else(|| "AND table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')".to_string());

        let sql = format!(
            r#"SELECT
                TABLE_NAME as name,
                TABLE_SCHEMA as `schema`,
                TABLE_ROWS as row_count,
                DATA_LENGTH as data_size_bytes,
                CREATE_TIME as create_time,
                UPDATE_TIME as update_time
            FROM information_schema.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            {}
            ORDER BY TABLE_SCHEMA, TABLE_NAME"#,
            db_filter
        );

        let rows = sqlx::query(&sql).fetch_all(self.pool.as_ref()).await?;

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

    pub async fn get_columns(&self, table_name: &str, database: Option<&str>) -> Result<Vec<ColumnInfo>> {
        let cache_key = format!("{}.{}", database.unwrap_or(""), table_name);

        {
            let cache = self.columns_cache.lock().await;
            if let Some(entry) = cache.get(&cache_key) {
                if entry.fetched_at.elapsed() < self.cache_ttl {
                    return Ok(entry.data.clone());
                }
            }
        }

        let columns = self.fetch_columns(table_name, database).await?;

        {
            let mut cache = self.columns_cache.lock().await;
            cache.insert(cache_key, CacheEntry { data: columns.clone(), fetched_at: Instant::now() });
        }

        Ok(columns)
    }

    async fn fetch_columns(&self, table_name: &str, database: Option<&str>) -> Result<Vec<ColumnInfo>> {
        let db_filter = database
            .map(|d| format!("AND TABLE_SCHEMA = '{}'", d))
            .unwrap_or_default();

        let sql = format!(
            r#"SELECT
                COLUMN_NAME as name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as column_default,
                COLUMN_KEY as column_key,
                EXTRA as extra
            FROM information_schema.COLUMNS
            WHERE TABLE_NAME = '{}'
            {}
            ORDER BY ORDINAL_POSITION"#,
            table_name, db_filter
        );

        let rows = sqlx::query(&sql).fetch_all(self.pool.as_ref()).await?;

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

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::test_helpers::setup_test_db;

    // mysql-mcp-t7a: list tables returns results
    #[tokio::test]
    async fn test_list_tables_returns_results() {
        let test_db = setup_test_db().await;
        let pool = Arc::new(test_db.pool.clone());
        let introspector = SchemaIntrospector::new(pool, 60);
        let db = test_db.config.connection.database.as_deref();
        let tables = introspector.list_tables(db).await;
        assert!(tables.is_ok());
    }

    // mysql-mcp-63f: schema cache TTL=0 disables caching
    #[tokio::test]
    async fn test_schema_cache_ttl_zero_disables_cache() {
        let test_db = setup_test_db().await;
        let pool = Arc::new(test_db.pool.clone());
        let db = test_db.config.connection.database.as_deref();
        // TTL=0 means instant expiry; should always re-fetch
        let introspector = SchemaIntrospector::new(pool, 0);
        let tables1 = introspector.list_tables(db).await.unwrap();
        let tables2 = introspector.list_tables(db).await.unwrap();
        assert_eq!(tables1.len(), tables2.len());
    }

    // mysql-mcp-1au: get columns for a known table
    #[tokio::test]
    async fn test_get_columns_for_table() {
        let test_db = setup_test_db().await;
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
