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
//! Benchmark groups:
//!
//! execute_read_query — full pipeline: sqlx query → MySQL → row fetch → column_to_json
//!   const_select_1row, mixed_types_1row, info_schema_10rows, info_schema_100rows,
//!   wide_20col_10rows
//!
//! index_suggestions — generate_index_suggestions on a cache-hot path (pure Rust HashMap
//!   work after the first DB call warms the column/index caches).
//!   single_unindexed_col: 1 WHERE col, no existing index → single-column suggestion
//!   two_unindexed_cols:   2 WHERE cols, no composite index → composite-index suggestion
//!
//! session_connect — build_session_pool including TCP + TLS handshake + auth.
//!   Note: the handle_connect clone→move change saves ~100 ns per call, invisible
//!   against the ~100 ms connection setup measured here.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mysql_mcp::query::read::execute_read_query;
use mysql_mcp::schema::SchemaIntrospector;
use mysql_mcp::sql_parser::parse_sql;
use std::sync::Arc;

struct BenchDb {
    pool: sqlx::MySqlPool,
    rt: tokio::runtime::Runtime,
    host: String,
    port: u16,
    user: String,
    pass: String,
    database: Option<String>,
    ssl: bool,
    ssl_ca: Option<String>,
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
    let ssl = std::env::var("MYSQL_SSL").is_ok_and(|v| v == "true" || v == "1");
    let ssl_ca_str = std::env::var("MYSQL_SSL_CA").unwrap_or_default();
    let ssl_ca = if ssl_ca_str.is_empty() {
        None
    } else {
        Some(ssl_ca_str)
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let pool = rt.block_on(async {
        mysql_mcp::db::build_session_pool(
            &host,
            port,
            &user,
            &pass,
            database.as_deref(),
            ssl,
            false,
            ssl_ca.as_deref(),
            30_000,
        )
        .await
    });

    match pool {
        Ok(p) => Some(BenchDb {
            pool: p,
            rt,
            host,
            port,
            user,
            pass,
            database,
            ssl,
            ssl_ca,
        }),
        Err(e) => {
            eprintln!(
                "  [skip] Could not connect to MySQL for DB benchmarks: {}",
                e
            );
            None
        }
    }
}

// ── execute_read_query benchmarks ────────────────────────────────────────────

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
                    execute_read_query(&db.pool, sql, &parsed, false, 0, "none", 10_000, 0, 0)
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
                    execute_read_query(&db.pool, sql, &parsed, false, 0, "none", 10_000, 0, 0)
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
                    execute_read_query(&db.pool, sql, &parsed, false, 0, "none", 10_000, 0, 0)
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
                    execute_read_query(&db.pool, sql, &parsed, false, 0, "none", 10_000, 0, 0)
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
                    execute_read_query(&db.pool, sql, &parsed, false, 0, "none", 10_000, 0, 0)
                        .await
                        .unwrap()
                })
            });
        });
    }

    group.finish();
}

// ── index_suggestions benchmarks ─────────────────────────────────────────────
//
// generate_index_suggestions is called after execute_read_query detects a full
// table scan with no index. It does three async DB calls (list_indexed_columns,
// list_composite_indexes, get_columns) then iterates over WHERE columns doing
// HashMap lookups.
//
// These benchmarks measure the cache-hot path: caches are primed in setup so
// each iteration runs only the pure-Rust loop (HashMap lookups + string compares).
// The fix in this session cached col_info.get(&col.to_lowercase()) to avoid a
// duplicate HashMap lookup + allocation per WHERE column.
//
// Table: mcp_test.explain_test_fts (id PK, val VARCHAR(50) — no index on val)

fn bench_index_suggestions(c: &mut Criterion) {
    let Some(db) = try_connect() else { return };

    let pool_arc = Arc::new(db.pool.clone());
    let introspector = SchemaIntrospector::new(pool_arc, 300);

    // Prime all three caches (indexed_columns, composite_indexes, columns) for
    // explain_test_fts so benchmark iterations hit only the Rust loop.
    db.rt.block_on(async {
        let _ = introspector
            .generate_index_suggestions("explain_test_fts", Some("mcp_test"), &["val".to_string()])
            .await;
    });
    // Let the stale-while-revalidate background spawn settle.
    std::thread::sleep(std::time::Duration::from_millis(200));

    let mut group = c.benchmark_group("index_suggestions");

    // 1 unindexed WHERE column → single-column CREATE INDEX suggestion
    {
        let where_cols = vec!["val".to_string()];
        group.bench_function("single_unindexed_col_cached", |b| {
            b.iter(|| {
                db.rt.block_on(async {
                    introspector
                        .generate_index_suggestions(
                            "explain_test_fts",
                            Some("mcp_test"),
                            &where_cols,
                        )
                        .await
                })
            });
        });
    }

    // 2 unindexed WHERE columns → composite index suggestion path
    // (uses the same table; both columns are unindexed so the composite branch fires)
    {
        let where_cols = vec!["val".to_string(), "id".to_string()];
        // Prime cache for this key too (same table, already cached, but prime suggestion path)
        db.rt.block_on(async {
            let _ = introspector
                .generate_index_suggestions("explain_test_fts", Some("mcp_test"), &where_cols)
                .await;
        });
        std::thread::sleep(std::time::Duration::from_millis(100));

        group.bench_function("two_unindexed_cols_cached", |b| {
            b.iter(|| {
                db.rt.block_on(async {
                    introspector
                        .generate_index_suggestions(
                            "explain_test_fts",
                            Some("mcp_test"),
                            &where_cols,
                        )
                        .await
                })
            });
        });
    }

    group.finish();
}

// ── session_connect benchmark ─────────────────────────────────────────────────
//
// Measures build_session_pool: TCP connect + TLS handshake + MySQL auth.
// This is the dominant cost in handle_connect (~100 ms on WAN).
// The handle_connect clone→move change saves ~100 ns per call — 6 orders of
// magnitude smaller than the connection setup and not measurable here.

fn bench_session_connect(c: &mut Criterion) {
    let Some(db) = try_connect() else { return };

    let mut group = c.benchmark_group("session_connect");
    // Connection setup is ~100 ms; use minimum sample size to keep bench time reasonable.
    group.sample_size(10);

    let host = db.host.clone();
    let port = db.port;
    let user = db.user.clone();
    let pass = db.pass.clone();
    let database = db.database.clone();
    let ssl = db.ssl;
    let ssl_ca = db.ssl_ca.clone();

    group.bench_function("build_session_pool", |b| {
        b.iter(|| {
            db.rt.block_on(async {
                mysql_mcp::db::build_session_pool(
                    &host,
                    port,
                    &user,
                    &pass,
                    database.as_deref(),
                    ssl,
                    false,
                    ssl_ca.as_deref(),
                    30_000,
                )
                .await
                .unwrap()
            })
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_query_execution,
    bench_index_suggestions,
    bench_session_connect
);
criterion_main!(benches);
