//! Criterion benchmarks for end-to-end query execution and row serialization.
//!
//! Run with:
//!   cargo bench --bench query_execution
//!
//! These benchmarks require a live MySQL connection. Set env vars before running:
//!   MYSQL_HOST=<host> MYSQL_USER=<user> MYSQL_PASS=<pass> MYSQL_DB=<db> \
//!   MYSQL_SSL=true MYSQL_SSL_CA=<path/to/ca.pem> \
//!   cargo bench --bench query_execution
//!
//! If MYSQL_HOST is not set, all benchmarks in this file are silently skipped.
//!
//! These benchmarks measure the full execute_read_query pipeline:
//! sqlx query → MySQL → row fetch → column_to_json (type dispatch) → Vec<Map>
//! This is the hottest path for large result sets — DB round-trip + serialization.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mysql_mcp::query::read::execute_read_query;
use mysql_mcp::sql_parser::parse_sql;

struct BenchDb {
    pool: sqlx::MySqlPool,
    rt: tokio::runtime::Runtime,
}

fn try_connect() -> Option<BenchDb> {
    let host = match std::env::var("MYSQL_HOST") {
        Ok(h) if !h.is_empty() => h,
        _ => {
            eprintln!("  [skip] MYSQL_HOST not set — skipping DB benchmarks");
            eprintln!("         Set MYSQL_HOST, MYSQL_USER, MYSQL_PASS, MYSQL_DB, MYSQL_SSL, MYSQL_SSL_CA");
            return None;
        }
    };
    let port = std::env::var("MYSQL_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3306);
    let user = std::env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_string());
    let pass = std::env::var("MYSQL_PASS").unwrap_or_default();
    let database = std::env::var("MYSQL_DB").ok().filter(|s| !s.is_empty());
    let ssl = std::env::var("MYSQL_SSL").map_or(false, |v| v == "true" || v == "1");
    let ssl_ca_str = std::env::var("MYSQL_SSL_CA").unwrap_or_default();
    let ssl_ca = if ssl_ca_str.is_empty() { None } else { Some(ssl_ca_str.as_str()) };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let pool = rt.block_on(async {
        mysql_mcp::db::build_session_pool(
            &host, port, &user, &pass,
            database.as_deref(), ssl, false, ssl_ca,
            30_000,
        ).await
    });

    match pool {
        Ok(p) => Some(BenchDb { pool: p, rt }),
        Err(e) => {
            eprintln!("  [skip] Could not connect to MySQL for DB benchmarks: {}", e);
            None
        }
    }
}

fn bench_query_execution(c: &mut Criterion) {
    let Some(db) = try_connect() else { return };

    let mut group = c.benchmark_group("execute_read_query");

    // 1. Constant SELECT — MySQL const-optimizes this; measures round-trip + minimal serialization
    {
        let sql = "SELECT 1 AS n";
        let parsed = parse_sql(sql).unwrap();
        group.throughput(Throughput::Elements(1));
        group.bench_function("const_select_1row", |b| {
            b.iter(|| {
                db.rt.block_on(async {
                    execute_read_query(&db.pool, sql, &parsed, false, 0, "none", 10_000)
                        .await
                        .unwrap()
                })
            });
        });
    }

    // 2. Mixed types, 1 row — exercises multiple branches of column_to_json type dispatch
    {
        let sql = "SELECT 1 AS int_col, 3.14 AS float_col, 'hello' AS str_col, NOW() AS ts_col, NULL AS null_col, CAST(42 AS DECIMAL(10,2)) AS dec_col";
        let parsed = parse_sql(sql).unwrap();
        group.throughput(Throughput::Elements(1));
        group.bench_function("mixed_types_1row", |b| {
            b.iter(|| {
                db.rt.block_on(async {
                    execute_read_query(&db.pool, sql, &parsed, false, 0, "none", 10_000)
                        .await
                        .unwrap()
                })
            });
        });
    }

    // 3. 10 rows from information_schema.COLUMNS — string-heavy, tests iteration overhead
    {
        let sql = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE \
                   FROM information_schema.COLUMNS \
                   WHERE TABLE_SCHEMA = 'information_schema' \
                   LIMIT 10";
        let parsed = parse_sql(sql).unwrap();
        group.throughput(Throughput::Elements(10));
        group.bench_function("info_schema_10rows", |b| {
            b.iter(|| {
                db.rt.block_on(async {
                    execute_read_query(&db.pool, sql, &parsed, false, 0, "none", 10_000)
                        .await
                        .unwrap()
                })
            });
        });
    }

    // 4. 100 rows — measures per-row serialization cost at scale
    {
        let sql = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE \
                   FROM information_schema.COLUMNS \
                   WHERE TABLE_SCHEMA = 'information_schema' \
                   LIMIT 100";
        let parsed = parse_sql(sql).unwrap();
        group.throughput(Throughput::Elements(100));
        group.bench_function("info_schema_100rows", |b| {
            b.iter(|| {
                db.rt.block_on(async {
                    execute_read_query(&db.pool, sql, &parsed, false, 0, "none", 10_000)
                        .await
                        .unwrap()
                })
            });
        });
    }

    // 5. Wide row — many columns, exercises column iteration and HashMap building
    {
        let sql = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, \
                          ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, \
                          CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, \
                          NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, \
                          CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE, \
                          COLUMN_KEY, EXTRA, PRIVILEGES, COLUMN_COMMENT \
                   FROM information_schema.COLUMNS \
                   WHERE TABLE_SCHEMA = 'information_schema' \
                   LIMIT 10";
        let parsed = parse_sql(sql).unwrap();
        group.throughput(Throughput::Elements(10));
        group.bench_function("wide_20col_10rows", |b| {
            b.iter(|| {
                db.rt.block_on(async {
                    execute_read_query(&db.pool, sql, &parsed, false, 0, "none", 10_000)
                        .await
                        .unwrap()
                })
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_query_execution);
criterion_main!(benches);
