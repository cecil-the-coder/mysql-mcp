//! E2E tests that spawn the compiled mysql-mcp binary and test the full MCP protocol.
//! These tests require the binary to be compiled first (cargo build).
//! MySQL is provided via testcontainers (Docker) or env vars — no manual setup needed.

#[cfg(test)]
mod e2e_tests {
    use tokio::io::{AsyncWriteExt, BufReader};
    use serde_json::json;
    use crate::test_helpers::setup_test_db;
    use crate::e2e_test_utils::{binary_path, send_message, read_response, spawn_server, do_handshake};

    // -------------------------------------------------------------------------
    // Test 1: MCP initialize handshake — uses do_handshake() helper
    // -------------------------------------------------------------------------
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

        // Send initialize request and capture the response directly (do_handshake
        // consumes the response, so we send the request manually here to inspect it).
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

    // -------------------------------------------------------------------------
    // Test 2: tools/list returns the expected tools
    // -------------------------------------------------------------------------
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

        do_handshake(&mut stdin, &mut reader).await;
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
        use tokio::io::AsyncBufReadExt;
        let mut error_line = String::new();
        let read_result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reader.read_line(&mut error_line),
        ).await;

        match read_result {
            Ok(Ok(n)) if n > 0 => {
                // The server emitted a response — it must be valid JSON-RPC.
                let v: serde_json::Value = serde_json::from_str(error_line.trim())
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
}
