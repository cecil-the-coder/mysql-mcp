//! Shared helpers for E2E tests that spawn the mysql-mcp binary over stdio.
//! All items are `pub(crate)` so they can be used from any `#[cfg(test)]` module.

use serde_json::{json, Value};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

pub(crate) fn binary_path() -> Option<std::path::PathBuf> {
    // Pick the most recently modified binary so that both `cargo test` (which
    // rebuilds debug) and `cargo build --release` (which rebuilds release) are
    // handled correctly. Preferring one profile unconditionally causes E2E tests
    // to run against a stale binary when only the other profile was rebuilt.
    ["./target/release/mysql-mcp", "./target/debug/mysql-mcp"]
        .iter()
        .filter_map(|p| {
            let path = std::path::Path::new(p);
            path.metadata().ok().map(|m| (path.to_path_buf(), m))
        })
        .max_by_key(|(_, m)| m.modified().ok())
        .map(|(p, _)| p)
}

pub(crate) async fn send_message(stdin: &mut tokio::process::ChildStdin, msg: &Value) {
    let line = format!("{}\n", serde_json::to_string(msg).unwrap());
    stdin.write_all(line.as_bytes()).await.unwrap();
    stdin.flush().await.unwrap();
}

pub(crate) async fn read_response(
    reader: &mut BufReader<tokio::process::ChildStdout>,
) -> Option<Value> {
    let mut line = String::new();
    // 45s gives the binary time to connect to MySQL even under heavy parallel load.
    match tokio::time::timeout(Duration::from_secs(45), reader.read_line(&mut line)).await {
        Ok(Ok(n)) if n > 0 => serde_json::from_str(line.trim()).ok(),
        _ => None,
    }
}

/// Spawns the mysql-mcp binary with the given test DB credentials.
/// `extra_env` is a slice of `(key, value)` pairs applied after
/// the standard MySQL connection env vars.
pub(crate) fn spawn_server(
    binary: &std::path::Path,
    test_db: &crate::test_helpers::TestDb,
    extra_env: &[(&str, &str)],
) -> tokio::process::Child {
    let cfg = &test_db.config;
    let mut cmd = Command::new(binary);
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .env("MYSQL_HOST", &cfg.connection.host)
        .env("MYSQL_PORT", cfg.connection.port.to_string())
        .env("MYSQL_USER", &cfg.connection.user)
        .env("MYSQL_PASS", &cfg.connection.password)
        .env("MYSQL_SSL", if cfg.security.ssl { "true" } else { "false" })
        .env(
            "MYSQL_SSL_ACCEPT_INVALID_CERTS",
            if cfg.security.ssl_accept_invalid_certs {
                "true"
            } else {
                "false"
            },
        );
    // Only set MYSQL_DB/MYSQL_SSL_CA when non-empty: the env_config reader treats
    // Some("") as a database/CA override, which would fail at connect time.
    if let Some(db) = cfg.connection.database.as_deref().filter(|s| !s.is_empty()) {
        cmd.env("MYSQL_DB", db);
    }
    if let Some(ca) = cfg.security.ssl_ca.as_deref().filter(|s| !s.is_empty()) {
        cmd.env("MYSQL_SSL_CA", ca);
    }
    cmd
        // Give the binary generous connection headroom: the production
        // default (10 s) can be exhausted on high-latency remote DBs when
        // other tests are simultaneously establishing connections.
        .env("MYSQL_CONNECT_TIMEOUT", "120000");
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("Failed to spawn mysql-mcp")
}

/// Extract stdin and stdout from a freshly spawned child process.
/// Panics if the child was not spawned with piped stdin/stdout.
pub(crate) fn setup_io(
    child: &mut tokio::process::Child,
) -> (
    tokio::process::ChildStdin,
    BufReader<tokio::process::ChildStdout>,
) {
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    (stdin, BufReader::new(stdout))
}

/// Helper that performs the standard initialize + notifications/initialized
/// handshake so subsequent tool calls are accepted by the server.
pub(crate) async fn do_handshake(
    stdin: &mut tokio::process::ChildStdin,
    reader: &mut BufReader<tokio::process::ChildStdout>,
) {
    send_message(
        stdin,
        &json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1"}
            }
        }),
    )
    .await;
    let _ = read_response(reader).await; // consume initialize response
    send_message(
        stdin,
        &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    )
    .await;
}
