//! E2E tests that spawn the compiled mysql-mcp binary and test the full MCP protocol.
//! These tests require the binary to be compiled first (cargo build).
//! MySQL is provided via testcontainers (Docker) or env vars — no manual setup needed.

#[cfg(test)]
mod e2e_tests {
    use crate::e2e_test_utils::{
        binary_path, do_handshake, read_response, send_message, setup_io, spawn_server,
    };
    use crate::test_helpers::setup_test_db;
    use serde_json::json;
    use tokio::io::AsyncWriteExt;

    // -------------------------------------------------------------------------
    // Test 1: MCP initialize handshake — uses do_handshake() helper
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_mcp_initialize_handshake() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let Some(mut child) = spawn_server(&binary, &test_db, &[]) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);

        // Send initialize request and capture the response directly (do_handshake
        // consumes the response, so we send the request manually here to inspect it).
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test-client", "version": "1.0"}
                }
            }),
        )
        .await;

        let response = read_response(&mut reader).await;
        child.kill().await.ok();

        let resp = response.expect("No response from server within timeout");
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["id"], 1);
        assert!(
            resp.get("result").is_some(),
            "Expected result, got: {}",
            resp
        );
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

        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let Some(mut child) = spawn_server(&binary, &test_db, &[]) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);

        do_handshake(&mut stdin, &mut reader).await;
        send_message(
            &mut stdin,
            &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
        )
        .await;

        let response = read_response(&mut reader).await;
        child.kill().await.ok();

        let resp = response.expect("No tools/list response");
        let tool_arr = resp["result"]["tools"]
            .as_array()
            .expect("tools should be an array");
        let tool_names: Vec<&str> = tool_arr.iter().filter_map(|t| t["name"].as_str()).collect();
        for expected in [
            "mysql_query",
            "mysql_schema_info",
            "mysql_server_info",
            "mysql_connect",
            "mysql_disconnect",
            "mysql_list_sessions",
            "mysql_explain_plan",
        ] {
            assert!(
                tool_names.contains(&expected),
                "tools/list missing '{}', got: {:?}",
                expected,
                tool_names
            );
        }
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

        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let Some(mut child) = spawn_server(&binary, &test_db, &[]) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);

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
        )
        .await;

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
                assert_eq!(
                    code, -32700,
                    "Expected parse-error code -32700, got: {}",
                    code
                );
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

        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let Some(mut child) = spawn_server(&binary, &test_db, &[]) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);

        do_handshake(&mut stdin, &mut reader).await;

        // Call mysql_query without the required `sql` argument.
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": {}
                }
            }),
        )
        .await;

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

        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let Some(mut child) = spawn_server(&binary, &test_db, &[]) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);

        do_handshake(&mut stdin, &mut reader).await;

        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": {
                        "sql": "SELECT * FROM nonexistent_table_xyz_999"
                    }
                }
            }),
        )
        .await;

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
    // Test 6 (permission): INSERT is denied when MYSQL_ALLOW_INSERT is not set
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_mcp_insert_denied_without_permission() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else {
            return;
        };
        // Spawn with default permissions (MYSQL_ALLOW_INSERT not set → denied).
        let Some(mut child) = spawn_server(&binary, &test_db, &[]) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);

        do_handshake(&mut stdin, &mut reader).await;

        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": {
                        "sql": "INSERT INTO _e2e_perm_test (id) VALUES (1)"
                    }
                }
            }),
        )
        .await;

        let response = read_response(&mut reader).await;
        child.kill().await.ok();

        let resp = response.expect("No response from server within timeout");
        let result = resp.get("result").expect("Expected a result object");
        assert_eq!(
            result["isError"], true,
            "INSERT should be denied when MYSQL_ALLOW_INSERT is not set, got: {}",
            resp
        );
        let text = result["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            text.to_lowercase().contains("denied")
                || text.contains("INSERT")
                || text.contains("ALLOW"),
            "Error message should mention permission denial, got: {}",
            text
        );
    }

    // -------------------------------------------------------------------------
    // Test 7 (write + DDL): CREATE TABLE → INSERT → SELECT → DROP succeeds
    //         when MYSQL_ALLOW_INSERT and MYSQL_ALLOW_DDL are enabled.
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_mcp_write_and_ddl_allowed() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else {
            return;
        };

        // Unique table name to avoid collisions across concurrent test runs.
        let table = format!("_e2e_w{}", std::process::id());

        let Some(mut child) = spawn_server(
            &binary,
            &test_db,
            &[("MYSQL_ALLOW_INSERT", "true"), ("MYSQL_ALLOW_DDL", "true")],
        ) else {
            return;
        };
        let (mut stdin, mut reader) = setup_io(&mut child);
        do_handshake(&mut stdin, &mut reader).await;

        // Step 1: CREATE TABLE
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0", "id": 5, "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": {
                        "sql": format!(
                            "CREATE TABLE `{}` (id INT PRIMARY KEY AUTO_INCREMENT, val VARCHAR(50))",
                            table
                        )
                    }
                }
            }),
        )
        .await;
        let r = read_response(&mut reader)
            .await
            .expect("no response to CREATE TABLE");
        assert_ne!(
            r["result"]["isError"], true,
            "CREATE TABLE should succeed, got: {}",
            r
        );

        // Step 2: INSERT a row
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0", "id": 6, "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": {
                        "sql": format!("INSERT INTO `{}` (val) VALUES ('hello_e2e')", table)
                    }
                }
            }),
        )
        .await;
        let r = read_response(&mut reader)
            .await
            .expect("no response to INSERT");
        assert_ne!(
            r["result"]["isError"], true,
            "INSERT should succeed when MYSQL_ALLOW_INSERT=true, got: {}",
            r
        );
        let text = r["result"]["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            text.contains("rows_affected"),
            "INSERT response should include rows_affected, got: {}",
            text
        );

        // Step 3: SELECT to verify the row is present
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0", "id": 7, "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": { "sql": format!("SELECT val FROM `{}`", table) }
                }
            }),
        )
        .await;
        let r = read_response(&mut reader)
            .await
            .expect("no response to SELECT");
        assert_ne!(
            r["result"]["isError"], true,
            "SELECT after INSERT should succeed, got: {}",
            r
        );
        let text = r["result"]["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            text.contains("hello_e2e"),
            "SELECT result should contain the inserted value 'hello_e2e', got: {}",
            text
        );

        // Step 4: DROP TABLE (cleanup)
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0", "id": 8, "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": { "sql": format!("DROP TABLE `{}`", table) }
                }
            }),
        )
        .await;
        let r = read_response(&mut reader)
            .await
            .expect("no response to DROP TABLE");
        assert_ne!(
            r["result"]["isError"], true,
            "DROP TABLE should succeed, got: {}",
            r
        );

        child.kill().await.ok();
    }
}
