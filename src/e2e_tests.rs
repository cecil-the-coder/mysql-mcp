//! E2E tests that spawn the compiled mysql-mcp binary and test the full MCP protocol.
//! These tests require the binary to be compiled first (cargo build).
//! MySQL is provided via testcontainers (Docker) or env vars — no manual setup needed.

#[cfg(test)]
mod e2e_tests {
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
    use tokio::process::Command;
    use serde_json::{json, Value};
    use crate::test_helpers::setup_test_db;

    fn binary_path() -> Option<std::path::PathBuf> {
        let candidates = [
            "./target/debug/mysql-mcp",
            "./target/release/mysql-mcp",
        ];
        for path in &candidates {
            let p = std::path::PathBuf::from(path);
            if p.exists() {
                return Some(p);
            }
        }
        None
    }

    async fn send_message(stdin: &mut tokio::process::ChildStdin, msg: &Value) {
        let line = format!("{}\n", serde_json::to_string(msg).unwrap());
        stdin.write_all(line.as_bytes()).await.unwrap();
        stdin.flush().await.unwrap();
    }

    async fn read_response(reader: &mut BufReader<tokio::process::ChildStdout>) -> Option<Value> {
        let mut line = String::new();
        // 45s gives the binary time to connect to MySQL even under heavy parallel load.
        match tokio::time::timeout(Duration::from_secs(45), reader.read_line(&mut line)).await {
            Ok(Ok(n)) if n > 0 => serde_json::from_str(line.trim()).ok(),
            _ => None,
        }
    }

    fn spawn_server(binary: &std::path::Path, test_db: &crate::test_helpers::TestDb) -> tokio::process::Child {
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
            // Give the binary generous connection headroom: the production
            // default (10 s) can be exhausted on high-latency remote DBs when
            // other tests are simultaneously establishing connections.
            .env("MYSQL_CONNECT_TIMEOUT", "120000");
        cmd.spawn().expect("Failed to spawn mysql-mcp")
    }

    #[tokio::test]
    async fn test_mcp_initialize_handshake() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else { return; };
        let mut child = spawn_server(&binary, &test_db);

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "1.0"}
            }
        })).await;

        let response = read_response(&mut reader).await;
        child.kill().await.ok();

        let resp = response.expect("No response from server within timeout");
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["id"], 1);
        assert!(resp.get("result").is_some(), "Expected result, got: {}", resp);
    }

    #[tokio::test]
    async fn test_mcp_list_tools() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found.");
            return;
        };

        let Some(test_db) = setup_test_db().await else { return; };
        let mut child = spawn_server(&binary, &test_db);

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}}
        })).await;
        let _ = read_response(&mut reader).await;

        send_message(&mut stdin, &json!({"jsonrpc": "2.0", "method": "notifications/initialized"})).await;
        send_message(&mut stdin, &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})).await;

        let response = read_response(&mut reader).await;
        child.kill().await.ok();

        let resp = response.expect("No tools/list response");
        let tools = &resp["result"]["tools"];
        assert!(tools.is_array(), "tools should be an array");
        let has_mysql_query = tools.as_array().unwrap().iter().any(|t| t["name"] == "mysql_query");
        assert!(has_mysql_query, "Should have mysql_query tool");
    }
}
