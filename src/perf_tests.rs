/// Performance tests for mysql-mcp.
///
/// Run with: cargo test perf_ -- --nocapture
/// (or against real DB): MYSQL_HOST=... cargo test perf_ -- --nocapture
///
/// These tests always produce output; use --nocapture to see it.
/// Assertions catch gross regressions (not tight latency SLOs).
#[cfg(test)]
pub mod perf_tests {
    use crate::test_helpers::setup_test_db;
    use std::time::Instant;
    use tokio::task::JoinSet;

    // ── Stats helpers ────────────────────────────────────────────────────────

    pub(crate) struct Stats {
        pub count: usize,
        pub min_ms: f64,
        pub max_ms: f64,
        pub mean_ms: f64,
        pub p50_ms: f64,
        pub p95_ms: f64,
        pub p99_ms: f64,
        pub wall_ms: f64,
        pub qps: f64,
    }

    pub(crate) fn compute(mut samples_ms: Vec<f64>, wall_ms: f64) -> Stats {
        samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
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

    pub(crate) fn print(label: &str, s: &Stats) {
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
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        // Use an isolated pool (not the shared ENV_DB pool) to avoid cross-runtime
        // interference when multiple perf tests run in parallel.
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;
        const WARMUP: usize = 3;
        // N=20 keeps total duration to ~15s even at 500ms/query (high-latency remote DB),
        // so concurrent perf tests (pool_saturation) can connect without being starved.
        const N: usize = 20;

        let parsed_select1 = crate::sql_parser::parse_sql("SELECT 1").unwrap();

        // Warm-up connections — hold semaphore permit while the pool creates new TCP+SSL
        // connections so that concurrent perf/integration tests don't overwhelm MySQL's
        // connection handler (server connect_timeout=10s).
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire()
                .await
                .expect("DB semaphore closed");
            for _ in 0..WARMUP {
                crate::query::read::execute_read_query(
                    pool,
                    "SELECT 1",
                    &parsed_select1,
                    false,
                    0,
                    "none",
                    0,
                    0,
                    0,
                    0,
                )
                .await
                .unwrap();
            }
        }

        let wall = Instant::now();
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            crate::query::read::execute_read_query(
                pool,
                "SELECT 1",
                &parsed_select1,
                false,
                0,
                "none",
                0,
                0,
                0,
                0,
            )
            .await
            .unwrap();
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let stats = compute(samples, wall.elapsed().as_secs_f64() * 1000.0);
        print(&format!("Sequential SELECT 1 (n={N})"), &stats);

        assert!(
            stats.p99_ms < 5_000.0,
            "p99 too high: {:.1}ms",
            stats.p99_ms
        );
    }

    /// Sequential SELECT on a real table with joins.
    #[tokio::test]
    async fn perf_sequential_join_query() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;

        // Establish the pool connection through the semaphore before the check query
        // creates it lazily, so new TCP+SSL connections are throttled under parallel load.
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire()
                .await
                .expect("DB semaphore closed");
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

        let parsed_join = crate::sql_parser::parse_sql(sql).unwrap();

        let wall = Instant::now();
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            crate::query::read::execute_read_query(pool, sql, &parsed_join, false, 0, "none", 0, 0, 0, 0)
                .await
                .unwrap();
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let stats = compute(samples, wall.elapsed().as_secs_f64() * 1000.0);
        print(&format!("Sequential 3-table JOIN (n={N})"), &stats);

        assert!(
            stats.p99_ms < 5_000.0,
            "p99 too high: {:.1}ms",
            stats.p99_ms
        );
    }

    /// Concurrent SELECTs — measures throughput and tail latency under load.
    /// Uses a dedicated pool so it doesn't starve other tests running in parallel.
    /// Kept modest (CONCURRENCY=10) so the DB remains responsive for other perf
    /// tests (pool_saturation) that run concurrently in the same test binary.
    #[tokio::test]
    async fn perf_concurrent_select_throughput() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
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
                    .acquire()
                    .await
                    .expect("DB semaphore closed");
                warm_conns.push(
                    pool.acquire()
                        .await
                        .expect("pool warmup: failed to acquire connection"),
                );
                drop(_permit);
            }
            // Drop releases all connections back to the pool as idle.
        }

        let parsed_select1 = crate::sql_parser::parse_sql("SELECT 1").unwrap();

        let wall = Instant::now();
        let mut set = JoinSet::new();
        for _ in 0..CONCURRENCY {
            let pool = pool.clone();
            let parsed = parsed_select1.clone();
            set.spawn(async move {
                let mut v = Vec::with_capacity(PER_TASK);
                for _ in 0..PER_TASK {
                    let t = Instant::now();
                    crate::query::read::execute_read_query(
                        &pool, "SELECT 1", &parsed, false, 0, "none", 0, 0, 0, 0,
                    )
                    .await
                    .unwrap();
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
            &format!(
                "Concurrent SELECT 1 (concurrency={CONCURRENCY}, n={})",
                CONCURRENCY * PER_TASK
            ),
            &stats,
        );

        assert!(
            stats.p99_ms < 10_000.0,
            "p99 under load too high: {:.1}ms",
            stats.p99_ms
        );
    }
}
