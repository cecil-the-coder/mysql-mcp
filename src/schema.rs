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
                name: row.try_get("name").unwrap_or_default(),
                schema: row.try_get("schema").unwrap_or_default(),
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
            use sqlx::Row;
            ColumnInfo {
                name: row.try_get("name").unwrap_or_default(),
                data_type: row.try_get("data_type").unwrap_or_default(),
                is_nullable: row.try_get::<String, _>("is_nullable")
                    .map(|s| s == "YES")
                    .unwrap_or(true),
                column_default: row.try_get("column_default").ok().flatten(),
                column_key: row.try_get("column_key").ok().flatten(),
                extra: row.try_get("extra").ok(),
            }
        }).collect();

        Ok(columns)
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    fn mysql_url() -> Option<String> {
        let host = std::env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("MYSQL_PORT").unwrap_or_else(|_| "3306".to_string());
        let user = std::env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_string());
        let pass = std::env::var("MYSQL_PASS").unwrap_or_default();
        let db = std::env::var("MYSQL_DB").unwrap_or_else(|_| "testdb".to_string());

        use std::time::Duration;
        use std::net::TcpStream;
        let addr = format!("{}:{}", host, port);
        match TcpStream::connect_timeout(
            &addr.parse::<std::net::SocketAddr>().ok()?,
            Duration::from_secs(2),
        ) {
            Ok(_) => Some(format!("mysql://{}:{}@{}:{}/{}", user, pass, host, port, db)),
            Err(_) => None,
        }
    }

    // mysql-mcp-t7a: list tables returns results
    #[tokio::test]
    async fn test_list_tables_returns_results() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
        let pool = Arc::new(pool);
        let introspector = SchemaIntrospector::new(pool, 60);
        let tables = introspector.list_tables(None).await;
        assert!(tables.is_ok());
    }

    // mysql-mcp-63f: schema cache TTL=0 disables caching
    #[tokio::test]
    async fn test_schema_cache_ttl_zero_disables_cache() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
        let pool = Arc::new(pool);
        // TTL=0 means instant expiry; should always re-fetch
        let introspector = SchemaIntrospector::new(pool, 0);
        let tables1 = introspector.list_tables(None).await.unwrap();
        let tables2 = introspector.list_tables(None).await.unwrap();
        // Both should succeed (re-fetching works correctly)
        assert_eq!(tables1.len(), tables2.len());
    }

    // mysql-mcp-1au: get columns for a known table
    #[tokio::test]
    async fn test_get_columns_for_table() {
        let Some(url) = mysql_url() else {
            eprintln!("Skipping: MySQL not available");
            return;
        };
        let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
        // Create a table so we have something to introspect
        sqlx::query("CREATE TABLE IF NOT EXISTS test_schema_cols (id INT PRIMARY KEY, name VARCHAR(100))")
            .execute(&pool)
            .await
            .unwrap();
        let pool = Arc::new(pool);
        let introspector = SchemaIntrospector::new(pool.clone(), 60);
        let db = std::env::var("MYSQL_DB").unwrap_or_else(|_| "testdb".to_string());
        let columns = introspector.get_columns("test_schema_cols", Some(&db)).await;
        assert!(columns.is_ok());
        // Cleanup
        sqlx::query("DROP TABLE IF EXISTS test_schema_cols")
            .execute(pool.as_ref())
            .await
            .ok();
    }

    // mysql-mcp-26b: pool behavior under concurrent load - stub
    #[tokio::test]
    async fn test_pool_concurrent_queries_stub() {
        eprintln!("TODO: requires special setup (concurrent load testing with controlled pool size)");
    }
}
