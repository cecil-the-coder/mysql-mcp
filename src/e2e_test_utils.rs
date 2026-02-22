//! Shared helpers for E2E tests that spawn the mysql-mcp binary over stdio.
//! All items are `pub(crate)` so they can be used from any `#[cfg(test)]` module.

use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::process::Command;
use serde_json::{json, Value};

pub(crate) fn binary_path() -> Option<std::path::PathBuf> {
    // Prefer the release binary: it reflects `cargo build --release` runs and is
    // the same binary used in production. The debug binary can lag behind if only
    // the release target was rebuilt.
    [
        "./target/release/mysql-mcp",
        "./target/debug/mysql-mcp",
    ]
    .iter()
    .find(|path| std::path::Path::new(path).exists())
    .map(std::path::PathBuf::from)
}

pub(crate) async fn send_message(stdin: &mut tokio::process::ChildStdin, msg: &Value) {
    let line = format!("{}\n", serde_json::to_string(msg).unwrap());
    stdin.write_all(line.as_bytes()).await.unwrap();
    stdin.flush().await.unwrap();
}

pub(crate) async fn read_response(reader: &mut BufReader<tokio::process::ChildStdout>) -> Option<Value> {
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
        .env("MYSQL_DB", cfg.connection.database.as_deref().unwrap_or(""))
        .env("MYSQL_SSL", if cfg.security.ssl { "true" } else { "false" })
        .env("MYSQL_SSL_ACCEPT_INVALID_CERTS", if cfg.security.ssl_accept_invalid_certs { "true" } else { "false" })
        .env("MYSQL_SSL_CA", cfg.security.ssl_ca.as_deref().unwrap_or(""))
        // Give the binary generous connection headroom: the production
        // default (10 s) can be exhausted on high-latency remote DBs when
        // other tests are simultaneously establishing connections.
        .env("MYSQL_CONNECT_TIMEOUT", "120000");
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("Failed to spawn mysql-mcp")
}

/// Helper that performs the standard initialize + notifications/initialized
/// handshake so subsequent tool calls are accepted by the server.
pub(crate) async fn do_handshake(
    stdin: &mut tokio::process::ChildStdin,
    reader: &mut BufReader<tokio::process::ChildStdout>,
) {
    send_message(stdin, &json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "1"}
        }
    })).await;
    let _ = read_response(reader).await; // consume initialize response
    send_message(stdin, &json!({"jsonrpc": "2.0", "method": "notifications/initialized"})).await;
}
