use std::sync::Arc;
use crate::config::Config;
use crate::db::DbPool;
use testcontainers_modules::{
    mysql::Mysql,
    testcontainers::{runners::AsyncRunner, ContainerAsync},
};

/// Build a new pool with a custom max_connections using the same credentials as
/// an existing TestDb. Useful for perf tests that want more connections than the
/// shared pool allows without starving other tests. The caller must keep `test_db`
/// alive (so any testcontainer stays running) for the lifetime of the returned pool.
pub async fn make_pool(test_db: &TestDb, max_connections: u32) -> sqlx::MySqlPool {
    let cfg = &test_db.config;

    use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode};
    // Mirror the same logic as db.rs build_connect_options.
    let ssl_mode = match (cfg.security.ssl, cfg.security.ssl_accept_invalid_certs) {
        (false, _) => MySqlSslMode::Disabled,
        (true, true) => MySqlSslMode::Required,
        (true, false) => MySqlSslMode::VerifyIdentity,
    };
    let mut opts = MySqlConnectOptions::new()
        .host(&cfg.connection.host)
        .port(cfg.connection.port)
        .username(&cfg.connection.user)
        .password(&cfg.connection.password)
        .ssl_mode(ssl_mode);
    if let Some(db) = &cfg.connection.database {
        opts = opts.database(db);
    }

    MySqlPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect_with(opts)
        .await
        .expect("make_pool: failed to connect")
}

/// Shared pool + config for the env-var path.
/// All parallel tests reuse one pool rather than each opening their own,
/// which avoids saturating the remote server's connection limit.
static ENV_DB: tokio::sync::OnceCell<(sqlx::MySqlPool, Arc<Config>)> =
    tokio::sync::OnceCell::const_new();

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
/// - If `MYSQL_HOST` is set, uses env-var config (same production code path as `db.rs`).
/// - Otherwise, starts a throwaway MySQL 8.1 container via testcontainers (requires Docker).
///
/// Container auth: MySQL 8.1 defaults to `caching_sha2_password` which requires SSL
/// for its first-connection full-authentication exchange, even with an empty password.
/// We therefore connect with `MySqlSslMode::Preferred` (SSL when available, no cert
/// verification) rather than going through `db.rs`'s Disabled path.
pub async fn setup_test_db() -> TestDb {
    if std::env::var("MYSQL_HOST").is_ok() {
        let (pool, config) = ENV_DB.get_or_init(|| async {
            let config = Arc::new(crate::config::merge::load_config().unwrap());
            let db = DbPool::new(config.clone()).await
                .expect("Failed to connect to env-var MySQL");
            (db.pool().clone(), config)
        }).await;
        return TestDb {
            pool: pool.clone(),
            config: config.clone(),
            using_container: false,
            _container: None,
        };
    }

    let container = Mysql::default()
        .start()
        .await
        .expect("Failed to start MySQL container — is Docker running?");

    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(3306).await.unwrap();

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

    let pool = MySqlPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect_with(opts)
        .await
        .expect("Failed to connect to MySQL container");

    // Build a config that reflects the container's coordinates.
    // Used by e2e tests to pass env vars to the spawned binary.
    let mut config = Config::default();
    config.connection.host = host;
    config.connection.port = port;
    config.connection.user = "root".to_string();
    config.connection.password = String::new();
    config.connection.database = Some("test".to_string());
    // ssl_accept_invalid_certs=true → binary will use Required mode (SSL, no cert check),
    // which works with MySQL's auto-generated container certificates.
    config.security.ssl = true;
    config.security.ssl_accept_invalid_certs = true;

    TestDb {
        pool,
        config: Arc::new(config),
        using_container: true,
        _container: Some(container),
    }
}
