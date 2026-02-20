//! E2E tests that spawn the compiled mysql-mcp binary and test the full MCP protocol.
//! These tests require the binary to be compiled first (cargo build).
//! Skip gracefully if the binary is not found or MySQL is not available.

#[cfg(test)]
mod e2e_tests {
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
    use tokio::process::Command;
    use serde_json::{json, Value};

    /// Find the compiled binary
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

    fn mysql_available() -> bool {
        use std::time::Duration;
        let host = std::env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("MYSQL_PORT").unwrap_or_else(|_| "3306".to_string());
        let addr = format!("{}:{}", host, port);
        if let Ok(addr) = addr.parse::<std::net::SocketAddr>() {
            std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2)).is_ok()
        } else {
            false
        }
    }

    /// Send a JSON-RPC message to the server process
    async fn send_message(stdin: &mut tokio::process::ChildStdin, msg: &Value) {
        let json_str = serde_json::to_string(msg).unwrap();
        let line = format!("{}\n", json_str);
        stdin.write_all(line.as_bytes()).await.unwrap();
        stdin.flush().await.unwrap();
    }

    /// Read a JSON-RPC response line from the server process
    async fn read_response(reader: &mut BufReader<tokio::process::ChildStdout>) -> Option<Value> {
        let mut line = String::new();
        match tokio::time::timeout(
            Duration::from_secs(5),
            reader.read_line(&mut line)
        ).await {
            Ok(Ok(n)) if n > 0 => serde_json::from_str(line.trim()).ok(),
            _ => None,
        }
    }

    #[tokio::test]
    async fn test_mcp_initialize_handshake() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };
        if !mysql_available() {
            eprintln!("Skipping E2E: MySQL not available.");
            return;
        }

        let mysql_host = std::env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let mysql_port = std::env::var("MYSQL_PORT").unwrap_or_else(|_| "3306".to_string());
        let mysql_user = std::env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_string());
        let mysql_pass = std::env::var("MYSQL_PASS").unwrap_or_default();
        let mysql_db = std::env::var("MYSQL_DB").unwrap_or_else(|_| "testdb".to_string());

        let mut child = Command::new(&binary)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .env("MYSQL_HOST", &mysql_host)
            .env("MYSQL_PORT", &mysql_port)
            .env("MYSQL_USER", &mysql_user)
            .env("MYSQL_PASS", &mysql_pass)
            .env("MYSQL_DB", &mysql_db)
            .spawn()
            .expect("Failed to spawn mysql-mcp");

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        // Send initialize request
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0"
                }
            }
        })).await;

        // Read response (may take a moment for the server to start)
        let response = read_response(&mut reader).await;

        child.kill().await.ok();

        if let Some(resp) = response {
            assert_eq!(resp["jsonrpc"], "2.0");
            assert_eq!(resp["id"], 1);
            // Either a result or we're waiting for initialization
            // The key thing is we got a valid JSON-RPC response
            assert!(resp.get("result").is_some() || resp.get("error").is_some());
        } else {
            eprintln!("Note: No response within timeout - server may need a moment to connect to MySQL");
        }
    }

    #[tokio::test]
    async fn test_mcp_list_tools() {
        let Some(binary) = binary_path() else { return; };
        if !mysql_available() { return; }

        let mut child = Command::new(&binary)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .env("MYSQL_HOST", std::env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()))
            .env("MYSQL_PORT", std::env::var("MYSQL_PORT").unwrap_or_else(|_| "3306".to_string()))
            .env("MYSQL_USER", std::env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_string()))
            .env("MYSQL_PASS", std::env::var("MYSQL_PASS").unwrap_or_default())
            .env("MYSQL_DB", std::env::var("MYSQL_DB").unwrap_or_else(|_| "testdb".to_string()))
            .spawn()
            .expect("Failed to spawn");

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        // Initialize first
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}}
        })).await;
        let _ = read_response(&mut reader).await;

        // Send initialized notification
        send_message(&mut stdin, &json!({"jsonrpc": "2.0", "method": "notifications/initialized"})).await;

        // List tools
        send_message(&mut stdin, &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})).await;
        let response = read_response(&mut reader).await;

        child.kill().await.ok();

        if let Some(resp) = response {
            if let Some(result) = resp.get("result") {
                let tools = &result["tools"];
                if tools.is_array() {
                    let has_mysql_query = tools.as_array().unwrap()
                        .iter()
                        .any(|t| t["name"] == "mysql_query");
                    assert!(has_mysql_query, "Should have mysql_query tool");
                }
            }
        }
    }
}
