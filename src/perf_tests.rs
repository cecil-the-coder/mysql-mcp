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
        const WARMUP: usize = 5;
        const N: usize = 100;

        // Warm-up connections
        for _ in 0..WARMUP {
            crate::query::read::execute_read_query(pool, "SELECT 1").await.unwrap();
        }

        let wall = Instant::now();
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            crate::query::read::execute_read_query(pool, "SELECT 1").await.unwrap();
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let stats = compute(samples, wall.elapsed().as_secs_f64() * 1000.0);
        print("Sequential SELECT 1 (n=100)", &stats);

        assert!(stats.p99_ms < 5_000.0, "p99 too high: {:.1}ms", stats.p99_ms);
    }

    /// Sequential SELECT on a real table with joins.
    #[tokio::test]
    async fn perf_sequential_join_query() {
        let test_db = setup_test_db().await;
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;

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

        const N: usize = 50;
        let sql = "SELECT u.name, p.name AS product, o.quantity \
                   FROM orders o \
                   JOIN users u ON o.user_id = u.id \
                   JOIN products p ON o.product_id = p.id";

        let wall = Instant::now();
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            crate::query::read::execute_read_query(pool, sql).await.unwrap();
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let stats = compute(samples, wall.elapsed().as_secs_f64() * 1000.0);
        print("Sequential 3-table JOIN (n=50)", &stats);

        assert!(stats.p99_ms < 5_000.0, "p99 too high: {:.1}ms", stats.p99_ms);
    }

    /// Concurrent SELECTs — measures throughput and tail latency under load.
    /// Uses a dedicated pool so it doesn't starve other tests running in parallel.
    #[tokio::test]
    async fn perf_concurrent_select_throughput() {
        let test_db = setup_test_db().await;
        let pool = crate::test_helpers::make_pool(&test_db, 25).await;
        const CONCURRENCY: usize = 20;
        const PER_TASK: usize = 10;

        // Pre-warm the pool: acquire CONCURRENCY connections sequentially while
        // holding them all, forcing the pool to create them one at a time.
        // Each creation takes 1 RTT for TCP + TLS + MySQL auth; on a remote DB
        // this can be seconds per connection, so we establish them all before
        // the timed test to avoid connection-creation delays inflating results.
        {
            let mut warm_conns: Vec<_> = Vec::with_capacity(CONCURRENCY);
            for _ in 0..CONCURRENCY {
                warm_conns.push(
                    pool.acquire()
                        .await
                        .expect("pool warmup: failed to acquire connection"),
                );
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
                    crate::query::read::execute_read_query(&pool, "SELECT 1").await.unwrap();
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
    #[tokio::test]
    async fn perf_write_cycle_latency() {
        let test_db = setup_test_db().await;
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;
        const N: usize = 50;

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

        print("INSERT (n=50)", &compute(insert_ms, wall_ms / 3.0));
        print("UPDATE (n=50)", &compute(update_ms, wall_ms / 3.0));
        print("DELETE (n=50)", &compute(delete_ms, wall_ms / 3.0));
    }

    /// Schema introspection: cold (TTL=0) vs warm (TTL=60s) cache.
    #[tokio::test]
    async fn perf_schema_cache_comparison() {
        let test_db = setup_test_db().await;
        let pool = Arc::new(crate::test_helpers::make_pool(&test_db, 3).await);
        let db = test_db.config.connection.database.as_deref();
        const N: usize = 20;

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
    #[tokio::test]
    async fn perf_pool_saturation() {
        let test_db = setup_test_db().await;
        let small_pool = crate::test_helpers::make_pool(&test_db, 3).await;

        const CONCURRENCY: usize = 30; // 10x over pool size
        const PER_TASK: usize = 5;

        let wall = Instant::now();
        let mut set = JoinSet::new();
        for _ in 0..CONCURRENCY {
            let pool = small_pool.clone();
            set.spawn(async move {
                let mut v = Vec::with_capacity(PER_TASK);
                for _ in 0..PER_TASK {
                    let t = Instant::now();
                    crate::query::read::execute_read_query(&pool, "SELECT 1").await.unwrap();
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
}
