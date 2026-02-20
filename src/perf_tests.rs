/// Performance tests for mysql-mcp.
///
/// Run with: cargo test perf_ -- --nocapture
/// (or against real DB): MYSQL_HOST=... cargo test perf_ -- --nocapture
///
/// These tests always produce output; use --nocapture to see it.
/// Assertions catch gross regressions (not tight latency SLOs).
#[cfg(test)]
mod perf_tests {
    use crate::test_helpers::setup_test_db;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::task::JoinSet;

    // ── Stats helpers ────────────────────────────────────────────────────────

    struct Stats {
        count: usize,
        min_ms: f64,
        max_ms: f64,
        mean_ms: f64,
        p50_ms: f64,
        p95_ms: f64,
        p99_ms: f64,
        wall_ms: f64,
        qps: f64,
    }

    fn compute(mut samples_ms: Vec<f64>, wall_ms: f64) -> Stats {
        samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = samples_ms.len();
        let pct = |p: f64| samples_ms[((n as f64 * p) as usize).min(n - 1)];
        Stats {
            count: n,
            min_ms: samples_ms[0],
            max_ms: samples_ms[n - 1],
            mean_ms: samples_ms.iter().sum::<f64>() / n as f64,
            p50_ms: pct(0.50),
            p95_ms: pct(0.95),
            p99_ms: pct(0.99),
            wall_ms,
            qps: n as f64 / (wall_ms / 1000.0),
        }
    }

    fn print(label: &str, s: &Stats) {
        eprintln!(
            "\n┌─ PERF: {label}\n\
             │  n={count}  wall={wall:.0}ms  throughput={qps:.1} qps\n\
             │  latency  min={min:.1}ms  mean={mean:.1}ms  p50={p50:.1}ms  p95={p95:.1}ms  p99={p99:.1}ms  max={max:.1}ms\n\
             └──────────────────────────────────────────────────────────",
            label = label,
            count = s.count,
            wall = s.wall_ms,
            qps = s.qps,
            min = s.min_ms,
            mean = s.mean_ms,
            p50 = s.p50_ms,
            p95 = s.p95_ms,
            p99 = s.p99_ms,
            max = s.max_ms,
        );
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    /// Sequential SELECT 1 — measures raw round-trip latency.
    #[tokio::test]
    async fn perf_sequential_select_latency() {
        let test_db = setup_test_db().await;
        // Use an isolated pool (not the shared ENV_DB pool) to avoid cross-runtime
        // interference when multiple perf tests run in parallel.
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;
        const WARMUP: usize = 3;
        // N=20 keeps total duration to ~15s even at 500ms/query (high-latency remote DB),
        // so concurrent perf tests (pool_saturation) can connect without being starved.
        const N: usize = 20;

        // Warm-up connections — hold semaphore permit while the pool creates new TCP+SSL
        // connections so that concurrent perf/integration tests don't overwhelm MySQL's
        // connection handler (server connect_timeout=10s).
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire().await.expect("DB semaphore closed");
            for _ in 0..WARMUP {
                crate::query::read::execute_read_query(pool, "SELECT 1", true).await.unwrap();
            }
        }

        let wall = Instant::now();
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            crate::query::read::execute_read_query(pool, "SELECT 1", true).await.unwrap();
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let stats = compute(samples, wall.elapsed().as_secs_f64() * 1000.0);
        print(&format!("Sequential SELECT 1 (n={N})"), &stats);

        assert!(stats.p99_ms < 5_000.0, "p99 too high: {:.1}ms", stats.p99_ms);
    }

    /// Sequential SELECT on a real table with joins.
    #[tokio::test]
    async fn perf_sequential_join_query() {
        let test_db = setup_test_db().await;
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;

        // Establish the pool connection through the semaphore before the check query
        // creates it lazily, so new TCP+SSL connections are throttled under parallel load.
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire().await.expect("DB semaphore closed");
            sqlx::query("SELECT 1").fetch_one(pool).await.unwrap();
        }

        // Ensure the table we're joining exists; skip gracefully if in container
        // (container has an empty "test" DB, no users/products/orders tables).
        let check = sqlx::query("SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'users'")
            .fetch_optional(pool)
            .await
            .unwrap();
        if check.is_none() {
            eprintln!("[PERF] perf_sequential_join_query: skipping — no 'users' table in this DB");
            return;
        }

        // N=20 keeps total duration to ~10s even at 500ms/query.
        const N: usize = 20;
        let sql = "SELECT u.name, p.name AS product, o.quantity \
                   FROM orders o \
                   JOIN users u ON o.user_id = u.id \
                   JOIN products p ON o.product_id = p.id";

        let wall = Instant::now();
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            crate::query::read::execute_read_query(pool, sql, true).await.unwrap();
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let stats = compute(samples, wall.elapsed().as_secs_f64() * 1000.0);
        print(&format!("Sequential 3-table JOIN (n={N})"), &stats);

        assert!(stats.p99_ms < 5_000.0, "p99 too high: {:.1}ms", stats.p99_ms);
    }

    /// Concurrent SELECTs — measures throughput and tail latency under load.
    /// Uses a dedicated pool so it doesn't starve other tests running in parallel.
    /// Kept modest (CONCURRENCY=10) so the DB remains responsive for other perf
    /// tests (pool_saturation) that run concurrently in the same test binary.
    #[tokio::test]
    async fn perf_concurrent_select_throughput() {
        let test_db = setup_test_db().await;
        let pool = crate::test_helpers::make_pool(&test_db, 15).await;
        const CONCURRENCY: usize = 10;
        const PER_TASK: usize = 5;

        // Pre-warm the pool: acquire CONCURRENCY connections sequentially while
        // holding them all, forcing the pool to create them one at a time.
        // Each creation takes 1 RTT for TCP + TLS + MySQL auth; on a remote DB
        // this can be seconds per connection, so we establish them all before
        // the timed test to avoid connection-creation delays inflating results.
        // Each acquire is throttled through the shared semaphore so that the
        // total simultaneous connection-creation attempts across all tests stays
        // within MySQL's connection-handler capacity (server connect_timeout=10s).
        {
            let mut warm_conns: Vec<_> = Vec::with_capacity(CONCURRENCY);
            for _ in 0..CONCURRENCY {
                let _permit = crate::test_helpers::db_semaphore()
                    .acquire().await.expect("DB semaphore closed");
                warm_conns.push(
                    pool.acquire()
                        .await
                        .expect("pool warmup: failed to acquire connection"),
                );
                drop(_permit);
            }
            // Drop releases all connections back to the pool as idle.
        }

        let wall = Instant::now();
        let mut set = JoinSet::new();
        for _ in 0..CONCURRENCY {
            let pool = pool.clone();
            set.spawn(async move {
                let mut v = Vec::with_capacity(PER_TASK);
                for _ in 0..PER_TASK {
                    let t = Instant::now();
                    crate::query::read::execute_read_query(&pool, "SELECT 1", true).await.unwrap();
                    v.push(t.elapsed().as_secs_f64() * 1000.0);
                }
                v
            });
        }

        let mut all = Vec::with_capacity(CONCURRENCY * PER_TASK);
        while let Some(r) = set.join_next().await {
            all.extend(r.unwrap());
        }
        let stats = compute(all, wall.elapsed().as_secs_f64() * 1000.0);
        print(&format!("Concurrent SELECT 1 (concurrency={CONCURRENCY}, n={})", CONCURRENCY * PER_TASK), &stats);

        assert!(stats.p99_ms < 10_000.0, "p99 under load too high: {:.1}ms", stats.p99_ms);
    }

    /// Write throughput — INSERT / UPDATE / DELETE cycle, sequential.
    /// N=20 keeps total write time modest (~20s) so concurrent tests aren't
    /// starved while this test holds the DB under heavy write load.
    #[tokio::test]
    async fn perf_write_cycle_latency() {
        let test_db = setup_test_db().await;
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;
        const N: usize = 20;

        // Establish pool connection through semaphore before the first query creates it lazily.
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire().await.expect("DB semaphore closed");
            sqlx::query("SELECT 1").fetch_one(pool).await.unwrap();
        }

        sqlx::query("CREATE TABLE IF NOT EXISTS perf_write_test (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(64))")
            .execute(pool)
            .await
            .unwrap();

        let mut insert_ms = Vec::with_capacity(N);
        let mut update_ms = Vec::with_capacity(N);
        let mut delete_ms = Vec::with_capacity(N);

        let wall = Instant::now();
        for i in 0..N {
            let val = format!("val_{i}");

            let t = Instant::now();
            let r = crate::query::write::execute_write_query(
                pool,
                &format!("INSERT INTO perf_write_test (v) VALUES ('{val}')"),
            ).await.unwrap();
            insert_ms.push(t.elapsed().as_secs_f64() * 1000.0);
            let id = r.last_insert_id.unwrap();

            let t = Instant::now();
            crate::query::write::execute_write_query(
                pool,
                &format!("UPDATE perf_write_test SET v='updated_{i}' WHERE id={id}"),
            ).await.unwrap();
            update_ms.push(t.elapsed().as_secs_f64() * 1000.0);

            let t = Instant::now();
            crate::query::write::execute_write_query(
                pool,
                &format!("DELETE FROM perf_write_test WHERE id={id}"),
            ).await.unwrap();
            delete_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let wall_ms = wall.elapsed().as_secs_f64() * 1000.0;

        sqlx::query("DROP TABLE IF EXISTS perf_write_test").execute(pool).await.ok();

        print(&format!("INSERT (n={N})"), &compute(insert_ms, wall_ms / 3.0));
        print(&format!("UPDATE (n={N})"), &compute(update_ms, wall_ms / 3.0));
        print(&format!("DELETE (n={N})"), &compute(delete_ms, wall_ms / 3.0));
    }

    /// Schema introspection: cold (TTL=0) vs warm (TTL=60s) cache.
    #[tokio::test]
    async fn perf_schema_cache_comparison() {
        let test_db = setup_test_db().await;
        let pool = Arc::new(crate::test_helpers::make_pool(&test_db, 3).await);
        let db = test_db.config.connection.database.as_deref();
        const N: usize = 20;

        // Establish the pool connection through the semaphore before the first DB call
        // creates it lazily, so this doesn't race with other tests' connection creation.
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire().await.expect("DB semaphore closed");
            sqlx::query("SELECT 1").fetch_one(pool.as_ref()).await.unwrap();
        }

        // Cold: TTL=0 forces a re-fetch every call
        let cold_intr = crate::schema::SchemaIntrospector::new(pool.clone(), 0);
        let mut cold_ms = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            cold_intr.list_tables(db).await.unwrap();
            cold_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }

        // Warm: TTL=60 serves from cache after first fetch
        let warm_intr = crate::schema::SchemaIntrospector::new(pool.clone(), 60);
        warm_intr.list_tables(db).await.unwrap(); // prime the cache
        let mut warm_ms = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            warm_intr.list_tables(db).await.unwrap();
            warm_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }

        let cold_wall = cold_ms.iter().sum::<f64>();
        let warm_wall = warm_ms.iter().sum::<f64>();
        let cold_stats = compute(cold_ms, cold_wall);
        let warm_stats = compute(warm_ms, warm_wall);

        print("Schema list_tables — COLD (TTL=0, n=20)", &cold_stats);
        print("Schema list_tables — WARM (TTL=60s cached, n=20)", &warm_stats);

        let speedup = cold_stats.mean_ms / warm_stats.mean_ms.max(0.001);
        eprintln!("\n  Cache speedup: {speedup:.1}x faster (cold mean={:.2}ms vs warm mean={:.2}ms)",
            cold_stats.mean_ms, warm_stats.mean_ms);

        // Warm cache should be faster than cold
        assert!(warm_stats.mean_ms <= cold_stats.mean_ms,
            "Cache hit should be faster than cold fetch");
    }

    /// Pool saturation: submit more concurrent queries than pool connections.
    /// Uses its own deliberately small pool to observe queueing behaviour.
    /// CONCURRENCY=15 (5x pool_size) keeps the worst-case queue wait under
    /// acquire_timeout even when the DB is responding slowly under parallel load.
    ///
    /// Connections are pre-warmed before the timed phase: SSL+auth handshakes at
    /// ~130ms RTT can take 2+ seconds each, and connection creation failures under
    /// concurrent DB load would obscure the queueing measurement.
    #[tokio::test]
    async fn perf_pool_saturation() {
        let test_db = setup_test_db().await;
        let small_pool = crate::test_helpers::make_pool(&test_db, 3).await;

        const CONCURRENCY: usize = 15; // 5x over pool size
        const PER_TASK: usize = 5;

        // Pre-warm all 3 pool connections before spawning the concurrent tasks.
        // Connection creation (TCP+TLS+MySQL auth) is sensitive to concurrent server
        // load; establishing connections upfront lets the timed phase focus purely on
        // queue-wait latency rather than connection-creation time.
        // Each acquire goes through the shared semaphore to cap total simultaneous
        // connection-creation attempts across all tests.
        {
            let mut warm_conns: Vec<_> = Vec::with_capacity(3);
            for _ in 0..3 {
                let _permit = crate::test_helpers::db_semaphore()
                    .acquire().await.expect("DB semaphore closed");
                warm_conns.push(
                    small_pool.acquire()
                        .await
                        .expect("pool_saturation warmup: failed to acquire connection"),
                );
                drop(_permit);
            }
            // Drop releases all connections back to the pool as idle.
        }

        let wall = Instant::now();
        let mut set = JoinSet::new();
        for _ in 0..CONCURRENCY {
            let pool = small_pool.clone();
            set.spawn(async move {
                let mut v = Vec::with_capacity(PER_TASK);
                for _ in 0..PER_TASK {
                    let t = Instant::now();
                    crate::query::read::execute_read_query(&pool, "SELECT 1", true).await.unwrap();
                    v.push(t.elapsed().as_secs_f64() * 1000.0);
                }
                v
            });
        }

        let mut all = Vec::with_capacity(CONCURRENCY * PER_TASK);
        while let Some(r) = set.join_next().await {
            all.extend(r.unwrap());
        }
        let stats = compute(all, wall.elapsed().as_secs_f64() * 1000.0);
        print(
            &format!("Pool saturation (pool_size=3, concurrency={CONCURRENCY}, n={})", CONCURRENCY * PER_TASK),
            &stats,
        );
        eprintln!("  (high latency expected — queries queue for a connection)");

        assert!(stats.p99_ms < 30_000.0, "p99 under saturation too high: {:.1}ms", stats.p99_ms);
    }

    /// Read-isolation comparison: 4-RTT transaction path vs bare 1-RTT fetch_all.
    /// Expected delta ≈ 3 RTTs on high-latency remote DBs.
    #[tokio::test]
    async fn perf_read_isolation_comparison() {
        let test_db = setup_test_db().await;
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;
        const N: usize = 20;

        // Warm up connections before timing (throttled through shared semaphore).
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire().await.expect("DB semaphore closed");
            for _ in 0..3 {
                crate::query::read::execute_read_query(pool, "SELECT 1", true).await.unwrap();
            }
        }

        // With readonly_transaction=true (4-RTT path)
        let wall_with = Instant::now();
        let mut with_tx_ms = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            crate::query::read::execute_read_query(pool, "SELECT 1", true).await.unwrap();
            with_tx_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let wall_with_ms = wall_with.elapsed().as_secs_f64() * 1000.0;

        // With readonly_transaction=false (1-RTT path)
        let wall_without = Instant::now();
        let mut no_tx_ms = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            crate::query::read::execute_read_query(pool, "SELECT 1", false).await.unwrap();
            no_tx_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let wall_without_ms = wall_without.elapsed().as_secs_f64() * 1000.0;

        let with_stats = compute(with_tx_ms, wall_with_ms);
        let no_stats = compute(no_tx_ms, wall_without_ms);

        print(&format!("SELECT 1 — WITH readonly_transaction (4-RTT, n={N})"), &with_stats);
        print(&format!("SELECT 1 — NO  readonly_transaction (1-RTT, n={N})"), &no_stats);

        let delta = with_stats.mean_ms - no_stats.mean_ms;
        eprintln!("\n  Delta (with - without): {delta:.1}ms/query (≈3 RTTs on remote DB)");
        eprintln!("  Speedup: {:.1}x", with_stats.mean_ms / no_stats.mean_ms.max(0.001));

        // no-transaction path must be at least as fast (usually much faster on remote DBs)
        assert!(
            no_stats.mean_ms <= with_stats.mean_ms,
            "no-transaction mean ({:.1}ms) should be <= with-transaction mean ({:.1}ms)",
            no_stats.mean_ms,
            with_stats.mean_ms,
        );
    }

    /// Serialization overhead: measure DB time vs JSON serialization time separately.
    /// Uses a wide table (10 varchar + 10 int columns) with 1 000 and 100 rows.
    #[tokio::test]
    async fn perf_serialization_overhead() {
        let test_db = setup_test_db().await;
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;

        // Establish pool connection through semaphore before first DB call.
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire().await.expect("DB semaphore closed");
            sqlx::query("SELECT 1").fetch_one(pool).await.unwrap();
        }

        // Create wide temp table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS perf_ser_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                s1 VARCHAR(100), s2 VARCHAR(100), s3 VARCHAR(100), s4 VARCHAR(100), s5 VARCHAR(100),
                s6 VARCHAR(100), s7 VARCHAR(100), s8 VARCHAR(100), s9 VARCHAR(100), s10 VARCHAR(100),
                n1 INT, n2 INT, n3 INT, n4 INT, n5 INT,
                n6 INT, n7 INT, n8 INT, n9 INT, n10 INT
            )"
        ).execute(pool).await.unwrap();

        // Populate 1 000 rows
        for chunk in 0..10usize {
            let mut values = Vec::with_capacity(100);
            for i in 0..100usize {
                let row_i = chunk * 100 + i;
                values.push(format!(
                    "('str{row_i}a','str{row_i}b','str{row_i}c','str{row_i}d','str{row_i}e',\
                     'str{row_i}f','str{row_i}g','str{row_i}h','str{row_i}i','str{row_i}j',\
                     {row_i},{row_i},{row_i},{row_i},{row_i},{row_i},{row_i},{row_i},{row_i},{row_i})"
                ));
            }
            let sql = format!(
                "INSERT INTO perf_ser_test (s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10) VALUES {}",
                values.join(",")
            );
            sqlx::query(&sql).execute(pool).await.unwrap();
        }

        // Benchmark with 1 000 rows
        let result_1000 = crate::query::read::execute_read_query(
            pool,
            "SELECT * FROM perf_ser_test LIMIT 1000",
            false,
        ).await.unwrap();

        // Benchmark with 100 rows
        let result_100 = crate::query::read::execute_read_query(
            pool,
            "SELECT * FROM perf_ser_test LIMIT 100",
            false,
        ).await.unwrap();

        // Drop table
        sqlx::query("DROP TABLE IF EXISTS perf_ser_test").execute(pool).await.ok();

        eprintln!(
            "\n┌─ PERF: Serialization overhead — 1 000 rows (20 cols)\n\
             │  execution_time_ms (DB):     {}ms\n\
             │  serialization_time_ms (JSON): {}ms\n\
             └──────────────────────────────────────────────────────────",
            result_1000.execution_time_ms,
            result_1000.serialization_time_ms,
        );
        eprintln!(
            "\n┌─ PERF: Serialization overhead — 100 rows (20 cols)\n\
             │  execution_time_ms (DB):     {}ms\n\
             │  serialization_time_ms (JSON): {}ms\n\
             └──────────────────────────────────────────────────────────",
            result_100.execution_time_ms,
            result_100.serialization_time_ms,
        );

        // Serialization of a local in-memory operation should be far less than a DB round-trip
        // (holds universally — even on localhost the DB has syscall overhead).
        assert!(
            result_1000.serialization_time_ms <= result_1000.execution_time_ms + 100,
            "serialization ({} ms) surprisingly close to or exceeding DB time ({} ms)",
            result_1000.serialization_time_ms,
            result_1000.execution_time_ms,
        );
    }
}
