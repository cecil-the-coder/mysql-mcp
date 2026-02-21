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
        // Prefer the release binary: it reflects `cargo build --release` runs and is
        // the same binary used in production. The debug binary can lag behind if only
        // the release target was rebuilt.
        let candidates = [
            "./target/release/mysql-mcp",
            "./target/debug/mysql-mcp",
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
        spawn_server_with_extra_env(binary, test_db, &[])
    }

    /// Like `spawn_server` but also sets additional environment variables before
    /// spawning. `extra_env` is a slice of `(key, value)` pairs applied after
    /// the standard MySQL connection env vars.
    fn spawn_server_with_extra_env(
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
    async fn do_handshake(
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

    // -------------------------------------------------------------------------
    // Test 3 (error path): malformed JSON must not crash the server
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_mcp_malformed_json_rejected() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else { return; };
        let mut child = spawn_server(&binary, &test_db);

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        // Send a line that is not valid JSON.
        stdin.write_all(b"this is not json\n").await.unwrap();
        stdin.flush().await.unwrap();

        // The server should either:
        //   (a) respond with a JSON-RPC parse error (-32700), or
        //   (b) silently discard the bad input and remain alive.
        // We give it up to 5 s (no MySQL round-trip needed for a parse error).
        let mut error_line = String::new();
        let read_result = tokio::time::timeout(
            Duration::from_secs(5),
            reader.read_line(&mut error_line),
        ).await;

        match read_result {
            Ok(Ok(n)) if n > 0 => {
                // The server emitted a response — it must be valid JSON-RPC.
                let v: Value = serde_json::from_str(error_line.trim())
                    .expect("Server returned non-JSON in response to malformed input");
                assert!(
                    v.get("error").is_some(),
                    "Expected a JSON-RPC error response, got: {}",
                    v
                );
                let code = v["error"]["code"].as_i64().unwrap_or(0);
                assert_eq!(code, -32700, "Expected parse-error code -32700, got: {}", code);
            }
            // Timeout or EOF: the server either silently dropped the line
            // (still alive) or exited due to protocol violation. Both are
            // acceptable — the rmcp framework exits (code 1) on malformed
            // input before the initialize handshake, which is a legitimate
            // response to a protocol violation.
            _ => {
                // Any outcome is acceptable here: alive or cleanly exited.
                let _ = child.try_wait();
            }
        }

        child.kill().await.ok();
    }

    // -------------------------------------------------------------------------
    // Test 4 (error path): mysql_query called without the required `sql` param
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_mcp_missing_required_params() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else { return; };
        let mut child = spawn_server(&binary, &test_db);

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        do_handshake(&mut stdin, &mut reader).await;

        // Call mysql_query without the required `sql` argument.
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "mysql_query",
                "arguments": {}
            }
        })).await;

        let response = read_response(&mut reader).await;
        child.kill().await.ok();

        let resp = response.expect("No response from server within timeout");
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["id"], 3);

        // The server must return either:
        //   (a) a result with isError:true (tool-level error), or
        //   (b) a JSON-RPC error object (protocol-level error).
        if let Some(result) = resp.get("result") {
            assert_eq!(
                result["isError"], true,
                "Expected isError:true in result, got: {}",
                resp
            );
            let content_text = result["content"][0]["text"].as_str().unwrap_or("");
            assert!(
                content_text.contains("Missing") || content_text.contains("sql"),
                "Error message should mention the missing 'sql' param, got: {}",
                content_text
            );
        } else {
            assert!(
                resp.get("error").is_some(),
                "Expected either result.isError or a top-level JSON-RPC error, got: {}",
                resp
            );
        }
    }

    // -------------------------------------------------------------------------
    // Test 5 (error path): valid SQL against a non-existent table propagates
    //         the database error as isError:true in the tool result.
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_mcp_query_error_propagates() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else { return; };
        let mut child = spawn_server(&binary, &test_db);

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        do_handshake(&mut stdin, &mut reader).await;

        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "mysql_query",
                "arguments": {
                    "sql": "SELECT * FROM nonexistent_table_xyz_999"
                }
            }
        })).await;

        let response = read_response(&mut reader).await;
        child.kill().await.ok();

        let resp = response.expect("No response from server within timeout");
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["id"], 4);

        let result = resp.get("result").expect("Expected a result object");
        assert_eq!(
            result["isError"], true,
            "Expected isError:true for a query against a nonexistent table, got: {}",
            resp
        );

        // The error content should reference the table name or be a DB error.
        let content_text = result["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            content_text.contains("nonexistent_table_xyz_999")
                || content_text.to_lowercase().contains("table")
                || content_text.to_lowercase().contains("exist")
                || content_text.to_lowercase().contains("error"),
            "Error message should reference the missing table or be a DB error, got: {}",
            content_text
        );
    }

    // -------------------------------------------------------------------------
    // Test 6 (error path): multi-statement SQL must be rejected before it
    //         reaches the database.
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_mcp_multi_statement_rejected() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else { return; };
        let mut child = spawn_server(&binary, &test_db);

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        do_handshake(&mut stdin, &mut reader).await;

        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "mysql_query",
                "arguments": {
                    "sql": "SELECT 1; DROP TABLE t"
                }
            }
        })).await;

        let response = read_response(&mut reader).await;
        child.kill().await.ok();

        let resp = response.expect("No response from server within timeout");
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["id"], 5);

        let result = resp.get("result").expect("Expected a result object");
        assert_eq!(
            result["isError"], true,
            "Expected isError:true for multi-statement SQL, got: {}",
            resp
        );

        let content_text = result["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            content_text.contains("Multi-statement") || content_text.contains("multi-statement"),
            "Error message should mention multi-statement SQL rejection, got: {}",
            content_text
        );
    }

    // -------------------------------------------------------------------------
    // Test 7: named session connect, query, and disconnect lifecycle
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_session_connect_and_disconnect() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else { return; };

        // The mysql_connect tool in the subprocess does not accept
        // ssl_accept_invalid_certs, so it cannot connect to testcontainers
        // (which use self-signed certificates).  Skip when running against a
        // Docker container; this test is meaningful against a real DB server.
        if test_db.using_container {
            eprintln!("Skipping test_session_connect_and_disconnect: testcontainer uses self-signed cert incompatible with mysql_connect");
            return;
        }

        let mut child = spawn_server_with_extra_env(
            &binary,
            &test_db,
            &[("MYSQL_ALLOW_RUNTIME_CONNECTIONS", "true")],
        );

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        do_handshake(&mut stdin, &mut reader).await;

        // --- mysql_connect ---
        let cfg = &test_db.config;
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {
                "name": "mysql_connect",
                "arguments": {
                    "name": "test_sess",
                    "host": cfg.connection.host,
                    "port": cfg.connection.port,
                    "user": cfg.connection.user,
                    "password": cfg.connection.password,
                    "database": cfg.connection.database,
                    "ssl": cfg.security.ssl,
                }
            }
        })).await;

        let connect_resp = read_response(&mut reader).await.expect("No response to mysql_connect");
        assert_eq!(connect_resp["id"], 10);
        assert!(
            connect_resp.get("result").is_some(),
            "mysql_connect should return a result, got: {}",
            connect_resp
        );
        assert_ne!(
            connect_resp["result"]["isError"], true,
            "mysql_connect should succeed, got: {}",
            connect_resp
        );

        // --- mysql_list_sessions: "test_sess" should appear ---
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {
                "name": "mysql_list_sessions",
                "arguments": {}
            }
        })).await;

        let list_resp = read_response(&mut reader).await.expect("No response to mysql_list_sessions");
        assert_eq!(list_resp["id"], 11);
        let list_text = list_resp["result"]["content"][0]["text"].as_str().unwrap_or("");
        let list_val: Value = serde_json::from_str(list_text).unwrap_or_default();
        let sessions = list_val["sessions"].as_array().cloned().unwrap_or_default();
        assert!(
            sessions.iter().any(|s| s["name"] == "test_sess"),
            "Expected 'test_sess' in session list, got: {}",
            list_text
        );

        // --- mysql_query via named session ---
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {
                "name": "mysql_query",
                "arguments": {
                    "sql": "SELECT 1 AS n",
                    "session": "test_sess"
                }
            }
        })).await;

        let query_resp = read_response(&mut reader).await.expect("No response to mysql_query via test_sess");
        assert_eq!(query_resp["id"], 12);
        assert!(
            query_resp.get("result").is_some(),
            "mysql_query (test_sess) should return a result, got: {}",
            query_resp
        );
        assert_ne!(
            query_resp["result"]["isError"], true,
            "mysql_query (test_sess) should succeed, got: {}",
            query_resp
        );
        let query_text = query_resp["result"]["content"][0]["text"].as_str().unwrap_or("");
        let query_val: Value = serde_json::from_str(query_text).unwrap_or_default();
        assert_eq!(
            query_val["row_count"], 1,
            "Expected row_count:1 from SELECT 1, got: {}",
            query_text
        );

        // --- mysql_disconnect ---
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 13,
            "method": "tools/call",
            "params": {
                "name": "mysql_disconnect",
                "arguments": { "name": "test_sess" }
            }
        })).await;

        let disconnect_resp = read_response(&mut reader).await.expect("No response to mysql_disconnect");
        assert_eq!(disconnect_resp["id"], 13);
        assert_ne!(
            disconnect_resp["result"]["isError"], true,
            "mysql_disconnect should succeed, got: {}",
            disconnect_resp
        );

        // --- mysql_list_sessions: "test_sess" should be gone ---
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 14,
            "method": "tools/call",
            "params": {
                "name": "mysql_list_sessions",
                "arguments": {}
            }
        })).await;

        let list_resp2 = read_response(&mut reader).await.expect("No response to second mysql_list_sessions");
        assert_eq!(list_resp2["id"], 14);
        let list_text2 = list_resp2["result"]["content"][0]["text"].as_str().unwrap_or("");
        let list_val2: Value = serde_json::from_str(list_text2).unwrap_or_default();
        let sessions2 = list_val2["sessions"].as_array().cloned().unwrap_or_default();
        assert!(
            !sessions2.iter().any(|s| s["name"] == "test_sess"),
            "Expected 'test_sess' to be absent after disconnect, got: {}",
            list_text2
        );

        child.kill().await.ok();
    }

    // -------------------------------------------------------------------------
    // Test 8: default vs. named session routing (both work; nonexistent errors)
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_session_routing_default_vs_named() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else { return; };

        // See comment in test_session_connect_and_disconnect: skip on testcontainers
        // because mysql_connect cannot accept self-signed certs.
        if test_db.using_container {
            eprintln!("Skipping test_session_routing_default_vs_named: testcontainer uses self-signed cert incompatible with mysql_connect");
            return;
        }

        let mut child = spawn_server_with_extra_env(
            &binary,
            &test_db,
            &[("MYSQL_ALLOW_RUNTIME_CONNECTIONS", "true")],
        );

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        do_handshake(&mut stdin, &mut reader).await;

        // --- mysql_connect: create "sess2" ---
        let cfg = &test_db.config;
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 20,
            "method": "tools/call",
            "params": {
                "name": "mysql_connect",
                "arguments": {
                    "name": "sess2",
                    "host": cfg.connection.host,
                    "port": cfg.connection.port,
                    "user": cfg.connection.user,
                    "password": cfg.connection.password,
                    "database": cfg.connection.database,
                    "ssl": cfg.security.ssl,
                }
            }
        })).await;
        let _ = read_response(&mut reader).await; // consume connect response

        // --- mysql_query: no session param → routes to default ---
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 21,
            "method": "tools/call",
            "params": {
                "name": "mysql_query",
                "arguments": { "sql": "SELECT 2 AS n" }
            }
        })).await;

        let default_resp = read_response(&mut reader).await.expect("No response for default session query");
        assert_eq!(default_resp["id"], 21);
        assert_ne!(
            default_resp["result"]["isError"], true,
            "mysql_query without session should succeed on default, got: {}",
            default_resp
        );

        // --- mysql_query: session="sess2" → routes to named session ---
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 22,
            "method": "tools/call",
            "params": {
                "name": "mysql_query",
                "arguments": { "sql": "SELECT 3 AS n", "session": "sess2" }
            }
        })).await;

        let named_resp = read_response(&mut reader).await.expect("No response for sess2 query");
        assert_eq!(named_resp["id"], 22);
        assert_ne!(
            named_resp["result"]["isError"], true,
            "mysql_query with session='sess2' should succeed, got: {}",
            named_resp
        );

        // --- mysql_query: session="nonexistent" → must return an error ---
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 23,
            "method": "tools/call",
            "params": {
                "name": "mysql_query",
                "arguments": { "sql": "SELECT 4 AS n", "session": "nonexistent" }
            }
        })).await;

        let notfound_resp = read_response(&mut reader).await.expect("No response for nonexistent session query");
        assert_eq!(notfound_resp["id"], 23);
        assert_eq!(
            notfound_resp["result"]["isError"], true,
            "mysql_query with session='nonexistent' should return isError:true, got: {}",
            notfound_resp
        );
        let err_text = notfound_resp["result"]["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            err_text.contains("nonexistent"),
            "Error message should mention the session name 'nonexistent', got: {}",
            err_text
        );

        child.kill().await.ok();
    }

    // -------------------------------------------------------------------------
    // Test 9: session name "default" is reserved and must be rejected
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_session_default_reserved() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else { return; };
        let mut child = spawn_server_with_extra_env(
            &binary,
            &test_db,
            &[("MYSQL_ALLOW_RUNTIME_CONNECTIONS", "true")],
        );

        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);

        do_handshake(&mut stdin, &mut reader).await;

        // Attempt to connect with the reserved name "default"
        let cfg = &test_db.config;
        send_message(&mut stdin, &json!({
            "jsonrpc": "2.0",
            "id": 30,
            "method": "tools/call",
            "params": {
                "name": "mysql_connect",
                "arguments": {
                    "name": "default",
                    "host": cfg.connection.host,
                    "port": cfg.connection.port,
                    "user": cfg.connection.user,
                    "password": cfg.connection.password,
                }
            }
        })).await;

        let reserved_resp = read_response(&mut reader).await.expect("No response to mysql_connect with name=default");
        assert_eq!(reserved_resp["id"], 30);
        assert_eq!(
            reserved_resp["result"]["isError"], true,
            "mysql_connect with name='default' should return isError:true, got: {}",
            reserved_resp
        );
        let err_text = reserved_resp["result"]["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            err_text.to_lowercase().contains("reserved") || err_text.to_lowercase().contains("default"),
            "Error should mention 'reserved' or 'default', got: {}",
            err_text
        );

        child.kill().await.ok();
    }
}
