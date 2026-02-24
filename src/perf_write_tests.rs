/// Write, schema-cache, pool-saturation, read-isolation, and serialization perf tests.
///
/// Run with: cargo test perf_ -- --nocapture
/// (or against real DB): MYSQL_HOST=... cargo test perf_ -- --nocapture
#[cfg(test)]
mod perf_write_tests {
    use crate::perf_tests::perf_tests::{compute, print};
    use crate::test_helpers::setup_test_db;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::task::JoinSet;

    // ── Tests ────────────────────────────────────────────────────────────────

    /// Write throughput — INSERT / UPDATE / DELETE cycle, sequential.
    /// N=20 keeps total write time modest (~20s) so concurrent tests aren't
    /// starved while this test holds the DB under heavy write load.
    #[tokio::test]
    async fn perf_write_cycle_latency() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;
        const N: usize = 20;

        // Establish pool connection through semaphore before the first query creates it lazily.
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire()
                .await
                .expect("DB semaphore closed");
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

            let insert_sql = format!("INSERT INTO perf_write_test (v) VALUES ('{val}')");
            let insert_parsed = crate::sql_parser::parse_sql(&insert_sql).unwrap();
            let t = Instant::now();
            let r =
                crate::query::write::execute_write_query(pool, &insert_sql, &insert_parsed, 0, 0)
                    .await
                    .unwrap();
            insert_ms.push(t.elapsed().as_secs_f64() * 1000.0);
            let id = r.last_insert_id.unwrap();

            let update_sql = format!("UPDATE perf_write_test SET v='updated_{i}' WHERE id={id}");
            let update_parsed = crate::sql_parser::parse_sql(&update_sql).unwrap();
            let t = Instant::now();
            crate::query::write::execute_write_query(pool, &update_sql, &update_parsed, 0, 0)
                .await
                .unwrap();
            update_ms.push(t.elapsed().as_secs_f64() * 1000.0);

            let delete_sql = format!("DELETE FROM perf_write_test WHERE id={id}");
            let delete_parsed = crate::sql_parser::parse_sql(&delete_sql).unwrap();
            let t = Instant::now();
            crate::query::write::execute_write_query(pool, &delete_sql, &delete_parsed, 0, 0)
                .await
                .unwrap();
            delete_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let _wall_ms = wall.elapsed().as_secs_f64() * 1000.0;

        sqlx::query("DROP TABLE IF EXISTS perf_write_test")
            .execute(pool)
            .await
            .ok();

        let insert_wall = insert_ms.iter().sum::<f64>();
        let update_wall = update_ms.iter().sum::<f64>();
        let delete_wall = delete_ms.iter().sum::<f64>();
        print(&format!("INSERT (n={N})"), &compute(insert_ms, insert_wall));
        print(&format!("UPDATE (n={N})"), &compute(update_ms, update_wall));
        print(&format!("DELETE (n={N})"), &compute(delete_ms, delete_wall));
    }

    /// Schema introspection: cold (TTL=0) vs warm (TTL=60s) cache.
    #[tokio::test]
    async fn perf_schema_cache_comparison() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let pool = Arc::new(crate::test_helpers::make_pool(&test_db, 3).await);
        let db = test_db.config.connection.database.as_deref();
        const N: usize = 20;

        // Establish the pool connection through the semaphore before the first DB call
        // creates it lazily, so this doesn't race with other tests' connection creation.
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire()
                .await
                .expect("DB semaphore closed");
            sqlx::query("SELECT 1")
                .fetch_one(pool.as_ref())
                .await
                .unwrap();
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
        print(
            "Schema list_tables — WARM (TTL=60s cached, n=20)",
            &warm_stats,
        );

        let speedup = cold_stats.mean_ms / warm_stats.mean_ms.max(0.001);
        eprintln!(
            "\n  Cache speedup: {speedup:.1}x faster (cold mean={:.2}ms vs warm mean={:.2}ms)",
            cold_stats.mean_ms, warm_stats.mean_ms
        );

        // Warm cache should be faster than cold
        assert!(
            warm_stats.mean_ms <= cold_stats.mean_ms,
            "Cache hit should be faster than cold fetch"
        );
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
        let Some(test_db) = setup_test_db().await else {
            return;
        };
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
                    .acquire()
                    .await
                    .expect("DB semaphore closed");
                warm_conns.push(
                    small_pool
                        .acquire()
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
                    crate::query::read::execute_read_query(
                        &pool,
                        "SELECT 1",
                        &crate::sql_parser::parse_sql("SELECT 1").unwrap(),
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
                "Pool saturation (pool_size=3, concurrency={CONCURRENCY}, n={})",
                CONCURRENCY * PER_TASK
            ),
            &stats,
        );
        eprintln!("  (high latency expected — queries queue for a connection)");

        assert!(
            stats.p99_ms < 30_000.0,
            "p99 under saturation too high: {:.1}ms",
            stats.p99_ms
        );
    }

    /// Read-isolation comparison: 4-RTT transaction path vs bare 1-RTT fetch_all.
    /// Expected delta ≈ 3 RTTs on high-latency remote DBs.
    #[tokio::test]
    async fn perf_read_isolation_comparison() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;
        const N: usize = 20;

        // Warm up connections before timing (throttled through shared semaphore).
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire()
                .await
                .expect("DB semaphore closed");
            for _ in 0..3 {
                crate::query::read::execute_read_query(
                    pool,
                    "SELECT 1",
                    &crate::sql_parser::parse_sql("SELECT 1").unwrap(),
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

        // With force_readonly_transaction=true (4-RTT path — paranoia mode)
        let wall_with = Instant::now();
        let mut with_tx_ms = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            crate::query::read::execute_read_query(
                pool,
                "SELECT 1",
                &crate::sql_parser::parse_sql("SELECT 1").unwrap(),
                true,
                0,
                "none",
                0,
                0,
                0,
                0,
            )
            .await
            .unwrap();
            with_tx_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let wall_with_ms = wall_with.elapsed().as_secs_f64() * 1000.0;

        // With force_readonly_transaction=false (1-RTT path — SELECT is known safe)
        let wall_without = Instant::now();
        let mut no_tx_ms = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            crate::query::read::execute_read_query(
                pool,
                "SELECT 1",
                &crate::sql_parser::parse_sql("SELECT 1").unwrap(),
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
            no_tx_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let wall_without_ms = wall_without.elapsed().as_secs_f64() * 1000.0;

        let with_stats = compute(with_tx_ms, wall_with_ms);
        let no_stats = compute(no_tx_ms, wall_without_ms);

        print(
            &format!("SELECT 1 — WITH readonly_transaction (4-RTT, n={N})"),
            &with_stats,
        );
        print(
            &format!("SELECT 1 — NO  readonly_transaction (1-RTT, n={N})"),
            &no_stats,
        );

        let delta = with_stats.mean_ms - no_stats.mean_ms;
        eprintln!("\n  Delta (with - without): {delta:.1}ms/query (≈3 RTTs on remote DB)");
        eprintln!(
            "  Speedup: {:.1}x",
            with_stats.mean_ms / no_stats.mean_ms.max(0.001)
        );

        // On a high-latency remote DB (mean > 5ms) the 3 extra RTTs are clearly visible and
        // the no-transaction path must be faster. On a local/container DB (sub-ms RTT) the
        // 3 extra round trips are lost in noise, so we skip the ordering assertion and just
        // verify both paths completed successfully (row_count checked implicitly by unwrap).
        if with_stats.mean_ms > 5.0 {
            assert!(
                no_stats.mean_ms <= with_stats.mean_ms,
                "no-transaction mean ({:.1}ms) should be <= with-transaction mean ({:.1}ms)",
                no_stats.mean_ms,
                with_stats.mean_ms,
            );
        } else {
            eprintln!("  (local DB: RTT too small to assert ordering — both paths ok)");
        }
    }

    /// Serialization overhead: measure DB time vs JSON serialization time separately.
    /// Uses a wide table (10 varchar + 10 int columns) with 1 000 and 100 rows.
    #[tokio::test]
    async fn perf_serialization_overhead() {
        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let pool = crate::test_helpers::make_pool(&test_db, 3).await;
        let pool = &pool;

        // Establish pool connection through semaphore before first DB call.
        {
            let _permit = crate::test_helpers::db_semaphore()
                .acquire()
                .await
                .expect("DB semaphore closed");
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
            &crate::sql_parser::parse_sql("SELECT * FROM perf_ser_test LIMIT 1000").unwrap(),
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

        // Benchmark with 100 rows
        let result_100 = crate::query::read::execute_read_query(
            pool,
            "SELECT * FROM perf_ser_test LIMIT 100",
            &crate::sql_parser::parse_sql("SELECT * FROM perf_ser_test LIMIT 100").unwrap(),
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

        // Drop table
        sqlx::query("DROP TABLE IF EXISTS perf_ser_test")
            .execute(pool)
            .await
            .ok();

        eprintln!(
            "\n┌─ PERF: Serialization overhead — 1 000 rows (20 cols)\n\
             │  execution_time_ms (DB):     {}ms\n\
             │  serialization_time_ms (JSON): {}ms\n\
             └──────────────────────────────────────────────────────────",
            result_1000.execution_time_ms, result_1000.serialization_time_ms,
        );
        eprintln!(
            "\n┌─ PERF: Serialization overhead — 100 rows (20 cols)\n\
             │  execution_time_ms (DB):     {}ms\n\
             │  serialization_time_ms (JSON): {}ms\n\
             └──────────────────────────────────────────────────────────",
            result_100.execution_time_ms, result_100.serialization_time_ms,
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
