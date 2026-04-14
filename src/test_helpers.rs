use crate::backend::PoolHandle;
use crate::config::Config;
use std::sync::{Arc, OnceLock};
#[cfg(feature = "mysql")]
use testcontainers_modules::mysql::Mysql;
#[cfg(feature = "postgres")]
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};

/// Global semaphore that limits how many tests may CREATE a new MySQL connection
/// simultaneously. MySQL's server-side `connect_timeout` (default 10 s) drops
/// connections whose SSL+auth handshake takes too long. Under heavy parallel
/// load, all 121 test connections being created at once can overwhelm MySQL's
/// connection handler, causing handshakes to queue past the 10 s deadline and
/// making the client retry repeatedly for the full 120 s acquire_timeout window.
///
/// 4 permits ≈ 4 concurrent SSL handshakes, each completing within MySQL's 10 s
/// window even at 130 ms network RTT (4 RTTs × 260 ms ≈ 1 s per connection).
static DB_SEMAPHORE: OnceLock<tokio::sync::Semaphore> = OnceLock::new();

pub fn db_semaphore() -> &'static tokio::sync::Semaphore {
    DB_SEMAPHORE.get_or_init(|| tokio::sync::Semaphore::new(4))
}

/// Build a new pool with a custom max_connections using the same credentials as
/// an existing TestDb. Useful for perf tests that want more connections than the
/// shared pool allows without starving other tests. The caller must keep `test_db`
/// alive (so any testcontainer stays running) for the lifetime of the returned pool.
///
/// Uses connect_lazy_with so NO connection is established at creation time.
/// This prevents a thundering herd when many perf tests call make_pool simultaneously
/// right after setup_test_db() returns. Connections are created on-demand the first
/// time the pool is used.
///
/// acquire_timeout is generous (120s) because perf tests involve heavy parallel load
/// that can slow the DB, and some saturation tests intentionally queue for connections.
pub async fn make_pool(test_db: &TestDb, max_connections: u32) -> sqlx::MySqlPool {
    use sqlx::mysql::MySqlPoolOptions;
    MySqlPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(std::time::Duration::from_secs(120))
        .connect_lazy_with({
            let mut opts = crate::db::build_connect_options(&test_db.config)
                .expect("build_connect_options failed in test helper");
            opts = opts.statement_cache_capacity(crate::backend::mysql::STATEMENT_CACHE_CAPACITY);
            opts
        })
}

/// Holds a test MySQL pool and optionally the container keeping it alive.
/// Dropping this value tears down the container.
pub struct TestDb {
    pub pool: sqlx::MySqlPool,
    pub config: Arc<Config>,
    /// True when the pool was opened against a testcontainers instance.
    /// E2E tests use this to know they should enable SSL for the subprocess.
    pub using_container: bool,
    _container: Option<ContainerAsync<Mysql>>,
}

/// Build a MySQL pool for integration tests.
///
/// - If `DB_HOST` is set, creates a **fresh per-test** pool using
///   connect_with. Each test runtime owns its own connections, so there is
///   no cross-runtime issue: when a test completes and its tokio runtime shuts
///   down, the connections in that test's pool go with it rather than sitting in
///   a shared pool where the next runtime would try to ping them through a dead
///   I/O reactor.
///
///   Connection creation is serialised through `DB_SEMAPHORE` (4 permits) to
///   prevent overwhelming MySQL's connection handler when all 121 tests start
///   simultaneously on a high-latency remote DB.
///
/// - Otherwise, starts a throwaway MySQL 8.1 container via testcontainers (requires Docker).
///
/// Container auth: MySQL 8.1 defaults to `caching_sha2_password` which requires SSL
/// for its first-connection full-authentication exchange, even with an empty password.
/// We therefore connect with `MySqlSslMode::Preferred` (SSL when available, no cert
/// verification) rather than going through `db.rs`'s Disabled path.
pub async fn setup_test_db() -> Option<TestDb> {
    if std::env::var("DB_HOST").is_ok() {
        let config = Arc::new(crate::config::load_config().unwrap());

        // Acquire a semaphore permit before creating the MySQL connection.
        // This throttles simultaneous SSL handshakes so they complete within
        // MySQL's server-side connect_timeout window even under heavy test
        // parallelism.  The permit is released as soon as connect_with()
        // returns, so it only serialises connection *creation*, not the
        // entire test.
        let _permit = db_semaphore().acquire().await.expect("DB semaphore closed");

        // max_connections=1: integration tests issue queries sequentially, so
        // a single connection is always sufficient.
        // test_before_acquire=false: skips the 500 ms ping that sqlx normally
        // sends to validate an idle connection before returning it — unnecessary
        // for a fresh per-test pool where the one connection stays alive for the
        // duration of the test.
        use sqlx::mysql::MySqlPoolOptions;
        let pool = match MySqlPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(120))
            .test_before_acquire(false)
            .connect_with({
                let mut opts = crate::db::build_connect_options(&config)
                    .expect("build_connect_options failed in test helper");
                opts =
                    opts.statement_cache_capacity(crate::backend::mysql::STATEMENT_CACHE_CAPACITY);
                opts
            })
            .await
        {
            Ok(p) => p,
            Err(e) => {
                eprintln!(
                    "[SKIP] Failed to connect to MySQL ({}), skipping integration test",
                    e
                );
                return None;
            }
        };

        // Permit released here; the next waiting test can now connect.
        drop(_permit);

        return Some(TestDb {
            pool,
            config,
            using_container: false,
            _container: None,
        });
    }

    start_mysql_container("8.1").await
}

/// Start a MySQL container using the specified Docker image tag and return a
/// connected `TestDb`.  Skips (returns `None`) when Docker is unavailable.
///
/// The tag can be any valid Docker Hub MySQL tag, e.g. `"8.1"` or `"9.2"`.
/// Useful for running the same integration tests against multiple MySQL versions.
pub async fn start_mysql_container(tag: &str) -> Option<TestDb> {
    let container = match Mysql::default().with_tag(tag).start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "[SKIP] Docker not available for MySQL {} ({}), skipping integration test",
                tag, e
            );
            return None;
        }
    };

    let host = match container.get_host().await {
        Ok(h) => h.to_string(),
        Err(e) => {
            eprintln!(
                "[SKIP] Failed to get container host ({}), skipping integration test",
                e
            );
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(3306).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "[SKIP] Failed to get container port ({}), skipping integration test",
                e
            );
            return None;
        }
    };

    // Build pool directly so we can use Preferred SSL mode.
    // caching_sha2_password (MySQL 8 default) requires SSL for its full-auth
    // handshake; Preferred enables SSL without verifying the server certificate.
    use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode};
    let opts = MySqlConnectOptions::new()
        .host(&host)
        .port(port)
        .username("root")
        .password("") // MYSQL_ALLOW_EMPTY_PASSWORD=yes
        .database("test") // MYSQL_DATABASE=test
        .ssl_mode(MySqlSslMode::Preferred);

    let pool = match MySqlPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect_with(opts)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "[SKIP] Failed to connect to MySQL {} container ({}), skipping integration test",
                tag, e
            );
            return None;
        }
    };

    // Build a config that reflects the container's coordinates.
    // Used by e2e tests to pass env vars to the spawned binary.
    let mut config = Config::default();
    config.connection.host = host;
    config.connection.port = Some(port);
    config.connection.user = "root".to_string();
    config.connection.password = String::new();
    config.connection.database = Some("test".to_string());
    // ssl_accept_invalid_certs=true → binary will use Required mode (SSL, no cert check),
    // which works with MySQL's auto-generated container certificates.
    config.security.ssl = true;
    config.security.ssl_accept_invalid_certs = true;

    Some(TestDb {
        pool,
        config: Arc::new(config),
        using_container: true,
        _container: Some(container),
    })
}

// ---------------------------------------------------------------------------
// PostgreSQL test helper
// ---------------------------------------------------------------------------

/// Global semaphore that limits how many tests may CREATE a new PostgreSQL
/// connection simultaneously. Prevents overwhelming PG's connection handler
/// under heavy parallel test load.
#[cfg(all(test, feature = "postgres"))]
static PG_SEMAPHORE: OnceLock<tokio::sync::Semaphore> = OnceLock::new();

#[cfg(all(test, feature = "postgres"))]
pub fn pg_semaphore() -> &'static tokio::sync::Semaphore {
    PG_SEMAPHORE.get_or_init(|| tokio::sync::Semaphore::new(4))
}

/// Holds a test PostgreSQL pool and optionally the container keeping it alive.
/// Dropping this value tears down the container.
#[cfg(all(test, feature = "postgres"))]
pub struct PgTestDb {
    pub pool: sqlx::PgPool,
    pub pool_handle: crate::backend::PoolHandle,
    pub backend: std::sync::Arc<dyn crate::backend::Backend>,
    /// The schema to use for test tables (defaults to "public").
    pub schema: String,
    _container: Option<ContainerAsync<Postgres>>,
}

/// Build a PostgreSQL pool for integration tests using testcontainers.
///
/// Spins up a PostgreSQL 16-alpine container via testcontainers (requires Docker).
/// Default auth: user=postgres, password=postgres, database=postgres.
/// Uses `PgSslMode::Prefer` for testcontainers.
///
/// Returns `None` (skip test) if Docker is unavailable.
#[cfg(all(test, feature = "postgres"))]
pub async fn setup_pg_test_db() -> Option<PgTestDb> {
    let container = match Postgres::default().with_tag("16-alpine").start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "[SKIP] Docker not available for PostgreSQL ({}), skipping integration test",
                e
            );
            return None;
        }
    };

    let host = match container.get_host().await {
        Ok(h) => h.to_string(),
        Err(e) => {
            eprintln!(
                "[SKIP] Failed to get PG container host ({}), skipping integration test",
                e
            );
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(5432).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "[SKIP] Failed to get PG container port ({}), skipping integration test",
                e
            );
            return None;
        }
    };

    let _permit = pg_semaphore().acquire().await.expect("PG semaphore closed");

    use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
    let opts = PgConnectOptions::new()
        .host(&host)
        .port(port)
        .username("postgres")
        .password("postgres")
        .database("postgres")
        .ssl_mode(PgSslMode::Prefer);

    let pool = match PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect_with(opts)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "[SKIP] Failed to connect to PostgreSQL container ({}), skipping integration test",
                e
            );
            return None;
        }
    };

    drop(_permit);

    let pool_handle = crate::backend::PoolHandle::new(Box::new(
        crate::backend::postgres::PgPoolWrapper::new(pool.clone()),
    ));
    let backend: std::sync::Arc<dyn crate::backend::Backend> =
        std::sync::Arc::new(crate::backend::postgres::PgBackend::new());

    Some(PgTestDb {
        pool,
        pool_handle,
        backend,
        schema: "public".to_string(),
        _container: Some(container),
    })
}

// ---------------------------------------------------------------------------
// SQLite test helpers
// ---------------------------------------------------------------------------

/// Holds a SQLite test pool backed by a temporary file.
/// Dropping this value closes the pool and removes the temp file.
#[cfg(all(test, feature = "sqlite"))]
pub struct SqliteTestDb {
    pub pool: PoolHandle,
    pub config: Arc<Config>,
    /// Path to the temp database file (removed on drop).
    _db_path: tempfile::NamedTempFile,
}

#[cfg(all(test, feature = "sqlite"))]
impl std::ops::Deref for SqliteTestDb {
    type Target = PoolHandle;
    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

#[cfg(all(test, feature = "sqlite"))]
/// Build a SQLite pool for integration tests.
///
/// Creates a temporary file-backed database (no Docker needed),
/// seeds it with test tables, and returns a `SqliteTestDb`.
/// This function always succeeds — SQLite never needs external services.
pub async fn setup_sqlite_test_db() -> SqliteTestDb {
    use crate::backend::Backend;

    // Create a named temp file; the file is automatically cleaned up when dropped.
    let tmp =
        tempfile::NamedTempFile::new().expect("failed to create temp file for SQLite test DB");
    let db_path = tmp.path().to_string_lossy().to_string();

    let mut config = Config::default();
    config.backend = crate::backend::BackendKind::Sqlite;
    config.connection.path = Some(db_path);
    config.pool.size = 1;
    config.pool.max_rows = 1000;

    let backend = crate::backend::sqlite::SqliteBackend::new();
    let pool = backend
        .create_pool(&config, &config.security)
        .await
        .expect("SQLite pool creation should never fail");

    // Seed test data
    pool.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER,
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        )",
    )
    .await
    .expect("seed CREATE TABLE users");

    pool.execute(
        "INSERT INTO users (name, email, age, active) VALUES
            ('Alice', 'alice@example.com', 30, 1),
            ('Bob', 'bob@example.com', 25, 0),
            ('Charlie', 'charlie@example.com', 35, 1)",
    )
    .await
    .expect("seed INSERT users");

    pool.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            price REAL,
            data BLOB
        )",
    )
    .await
    .expect("seed CREATE TABLE products");

    pool.execute(
        "INSERT INTO products (name, price, data) VALUES
            ('Widget', 9.99, X'48454C4C4F'),
            ('Gadget', 24.99, NULL),
            ('Doohickey', 14.50, X'DEADBEEF')",
    )
    .await
    .expect("seed INSERT products");

    pool.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            total REAL NOT NULL,
            status TEXT DEFAULT 'pending'
        )",
    )
    .await
    .expect("seed CREATE TABLE orders");

    pool.execute(
        "INSERT INTO orders (user_id, total, status) VALUES
            (1, 59.99, 'completed'),
            (1, 29.99, 'pending'),
            (2, 14.99, 'shipped')",
    )
    .await
    .expect("seed INSERT orders");

    SqliteTestDb {
        pool,
        config: Arc::new(config),
        _db_path: tmp,
    }
}
