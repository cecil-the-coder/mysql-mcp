//! E2E session tests: named session lifecycle, routing, and reserved-name rejection.
//! Requires the compiled mysql-mcp binary and a real (non-container) MySQL DB because
//! mysql_connect cannot accept self-signed certificates used by testcontainers.

#[cfg(test)]
mod e2e_session_tests {
    use crate::e2e_test_utils::{
        binary_path, do_handshake, read_response, send_message, setup_io, spawn_server,
    };
    use crate::test_helpers::setup_test_db;
    use serde_json::json;

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
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": {
                        "sql": "SELECT 1; DROP TABLE t"
                    }
                }
            }),
        )
        .await;

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

        let Some(test_db) = setup_test_db().await else {
            return;
        };

        // The mysql_connect tool in the subprocess does not accept
        // ssl_accept_invalid_certs, so it cannot connect to testcontainers
        // (which use self-signed certificates).  Skip when running against a
        // Docker container; this test is meaningful against a real DB server.
        if test_db.using_container {
            eprintln!("Skipping test_session_connect_and_disconnect: testcontainer uses self-signed cert incompatible with mysql_connect");
            return;
        }

        let Some(mut child) = spawn_server(
            &binary,
            &test_db,
            &[("MYSQL_ALLOW_RUNTIME_CONNECTIONS", "true")],
        ) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);

        do_handshake(&mut stdin, &mut reader).await;

        // --- mysql_connect ---
        let cfg = &test_db.config;
        send_message(
            &mut stdin,
            &json!({
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
                        "ssl_ca": cfg.security.ssl_ca,
                    }
                }
            }),
        )
        .await;

        let connect_resp = read_response(&mut reader)
            .await
            .expect("No response to mysql_connect");
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
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {
                    "name": "mysql_list_sessions",
                    "arguments": {}
                }
            }),
        )
        .await;

        let list_resp = read_response(&mut reader)
            .await
            .expect("No response to mysql_list_sessions");
        assert_eq!(list_resp["id"], 11);
        let list_text = list_resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("");
        let list_val: serde_json::Value = serde_json::from_str(list_text).unwrap_or_default();
        let sessions = list_val["sessions"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default();
        assert!(
            sessions.iter().any(|s| s["name"] == "test_sess"),
            "Expected 'test_sess' in session list, got: {}",
            list_text
        );

        // --- mysql_query via named session ---
        send_message(
            &mut stdin,
            &json!({
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
            }),
        )
        .await;

        let query_resp = read_response(&mut reader)
            .await
            .expect("No response to mysql_query via test_sess");
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
        let query_text = query_resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("");
        let query_val: serde_json::Value = serde_json::from_str(query_text).unwrap_or_default();
        assert_eq!(
            query_val["row_count"], 1,
            "Expected row_count:1 from SELECT 1, got: {}",
            query_text
        );

        // --- mysql_disconnect ---
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 13,
                "method": "tools/call",
                "params": {
                    "name": "mysql_disconnect",
                    "arguments": { "name": "test_sess" }
                }
            }),
        )
        .await;

        let disconnect_resp = read_response(&mut reader)
            .await
            .expect("No response to mysql_disconnect");
        assert_eq!(disconnect_resp["id"], 13);
        assert_ne!(
            disconnect_resp["result"]["isError"], true,
            "mysql_disconnect should succeed, got: {}",
            disconnect_resp
        );

        // --- mysql_list_sessions: "test_sess" should be gone ---
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 14,
                "method": "tools/call",
                "params": {
                    "name": "mysql_list_sessions",
                    "arguments": {}
                }
            }),
        )
        .await;

        let list_resp2 = read_response(&mut reader)
            .await
            .expect("No response to second mysql_list_sessions");
        assert_eq!(list_resp2["id"], 14);
        let list_text2 = list_resp2["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("");
        let list_val2: serde_json::Value = serde_json::from_str(list_text2).unwrap_or_default();
        let sessions2 = list_val2["sessions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
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

        let Some(test_db) = setup_test_db().await else {
            return;
        };

        // See comment in test_session_connect_and_disconnect: skip on testcontainers
        // because mysql_connect cannot accept self-signed certs.
        if test_db.using_container {
            eprintln!("Skipping test_session_routing_default_vs_named: testcontainer uses self-signed cert incompatible with mysql_connect");
            return;
        }

        let Some(mut child) = spawn_server(
            &binary,
            &test_db,
            &[("MYSQL_ALLOW_RUNTIME_CONNECTIONS", "true")],
        ) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);

        do_handshake(&mut stdin, &mut reader).await;

        // --- mysql_connect: create "sess2" ---
        let cfg = &test_db.config;
        send_message(
            &mut stdin,
            &json!({
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
                        "ssl_ca": cfg.security.ssl_ca,
                    }
                }
            }),
        )
        .await;
        let connect_resp = read_response(&mut reader)
            .await
            .expect("No response to mysql_connect for sess2");
        assert_ne!(
            connect_resp["result"]["isError"], true,
            "mysql_connect (sess2) should succeed, got: {}",
            connect_resp
        );

        // --- mysql_query: no session param → routes to default ---
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 21,
                "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": { "sql": "SELECT 2 AS n" }
                }
            }),
        )
        .await;

        let default_resp = read_response(&mut reader)
            .await
            .expect("No response for default session query");
        assert_eq!(default_resp["id"], 21);
        assert_ne!(
            default_resp["result"]["isError"], true,
            "mysql_query without session should succeed on default, got: {}",
            default_resp
        );

        // --- mysql_query: session="sess2" → routes to named session ---
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 22,
                "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": { "sql": "SELECT 3 AS n", "session": "sess2" }
                }
            }),
        )
        .await;

        let named_resp = read_response(&mut reader)
            .await
            .expect("No response for sess2 query");
        assert_eq!(named_resp["id"], 22);
        assert_ne!(
            named_resp["result"]["isError"], true,
            "mysql_query with session='sess2' should succeed, got: {}",
            named_resp
        );

        // --- mysql_query: session="nonexistent" → must return an error ---
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 23,
                "method": "tools/call",
                "params": {
                    "name": "mysql_query",
                    "arguments": { "sql": "SELECT 4 AS n", "session": "nonexistent" }
                }
            }),
        )
        .await;

        let notfound_resp = read_response(&mut reader)
            .await
            .expect("No response for nonexistent session query");
        assert_eq!(notfound_resp["id"], 23);
        assert_eq!(
            notfound_resp["result"]["isError"], true,
            "mysql_query with session='nonexistent' should return isError:true, got: {}",
            notfound_resp
        );
        let err_text = notfound_resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("");
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

        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let Some(mut child) = spawn_server(
            &binary,
            &test_db,
            &[("MYSQL_ALLOW_RUNTIME_CONNECTIONS", "true")],
        ) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);

        do_handshake(&mut stdin, &mut reader).await;

        // Attempt to connect with the reserved name "default"
        let cfg = &test_db.config;
        send_message(
            &mut stdin,
            &json!({
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
            }),
        )
        .await;

        let reserved_resp = read_response(&mut reader)
            .await
            .expect("No response to mysql_connect with name=default");
        assert_eq!(reserved_resp["id"], 30);
        assert_eq!(
            reserved_resp["result"]["isError"], true,
            "mysql_connect with name='default' should return isError:true, got: {}",
            reserved_resp
        );
        let err_text = reserved_resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("");
        assert!(
            err_text.to_lowercase().contains("reserved")
                || err_text.to_lowercase().contains("default"),
            "Error should mention 'reserved' or 'default', got: {}",
            err_text
        );

        child.kill().await.ok();
    }

    // -------------------------------------------------------------------------
    // Test 10: max_sessions limit is enforced at runtime
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_session_max_sessions_enforced() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else {
            return;
        };
        let Some(mut child) = spawn_server(
            &binary,
            &test_db,
            &[
                ("MYSQL_ALLOW_RUNTIME_CONNECTIONS", "true"),
                ("MYSQL_MAX_SESSIONS", "1"),
            ],
        ) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);
        do_handshake(&mut stdin, &mut reader).await;

        let cfg = &test_db.config;

        // First session: should succeed (limit is 1)
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0", "id": 40, "method": "tools/call",
                "params": {
                    "name": "mysql_connect",
                    "arguments": {
                        "name": "sess_a",
                        "host": cfg.connection.host,
                        "port": cfg.connection.port,
                        "user": cfg.connection.user,
                        "password": cfg.connection.password,
                        "database": cfg.connection.database,
                        "ssl": cfg.security.ssl,
                        "ssl_ca": cfg.security.ssl_ca,
                    }
                }
            }),
        )
        .await;
        let resp1 = read_response(&mut reader)
            .await
            .expect("No response to first mysql_connect");
        assert_ne!(
            resp1["result"]["isError"], true,
            "First session should succeed, got: {}",
            resp1
        );

        // Second session: should fail because max_sessions=1 is already reached
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0", "id": 41, "method": "tools/call",
                "params": {
                    "name": "mysql_connect",
                    "arguments": {
                        "name": "sess_b",
                        "host": cfg.connection.host,
                        "port": cfg.connection.port,
                        "user": cfg.connection.user,
                        "password": cfg.connection.password,
                        "database": cfg.connection.database,
                        "ssl": cfg.security.ssl,
                        "ssl_ca": cfg.security.ssl_ca,
                    }
                }
            }),
        )
        .await;
        let resp2 = read_response(&mut reader)
            .await
            .expect("No response to second mysql_connect");
        assert_eq!(
            resp2["result"]["isError"], true,
            "Second session should fail when limit reached, got: {}",
            resp2
        );
        let err = resp2["result"]["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            err.contains("limit") || err.contains("Maximum"),
            "Error should mention limit, got: {}",
            err
        );

        child.kill().await.ok();
    }

    // -------------------------------------------------------------------------
    // Test 11: duplicate session name is rejected
    // -------------------------------------------------------------------------
    #[tokio::test]
    async fn test_session_duplicate_name_rejected() {
        let Some(binary) = binary_path() else {
            eprintln!("Skipping E2E: binary not found. Run `cargo build` first.");
            return;
        };

        let Some(test_db) = setup_test_db().await else {
            return;
        };

        // The mysql_connect tool cannot accept self-signed certs from testcontainers.
        if test_db.using_container {
            eprintln!("Skipping test_session_duplicate_name_rejected: testcontainer cert incompatible with mysql_connect");
            return;
        }

        let Some(mut child) = spawn_server(
            &binary,
            &test_db,
            &[("MYSQL_ALLOW_RUNTIME_CONNECTIONS", "true")],
        ) else {
            return;
        };

        let (mut stdin, mut reader) = setup_io(&mut child);
        do_handshake(&mut stdin, &mut reader).await;

        let cfg = &test_db.config;

        // First connect: should succeed.
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0", "id": 50, "method": "tools/call",
                "params": {
                    "name": "mysql_connect",
                    "arguments": {
                        "name": "dup_sess",
                        "host": cfg.connection.host,
                        "port": cfg.connection.port,
                        "user": cfg.connection.user,
                        "password": cfg.connection.password,
                        "database": cfg.connection.database,
                        "ssl": cfg.security.ssl,
                        "ssl_ca": cfg.security.ssl_ca,
                    }
                }
            }),
        )
        .await;
        let resp1 = read_response(&mut reader)
            .await
            .expect("no response to first mysql_connect");
        assert_ne!(
            resp1["result"]["isError"], true,
            "first connect should succeed, got: {}",
            resp1
        );

        // Second connect with the same name: must fail.
        send_message(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0", "id": 51, "method": "tools/call",
                "params": {
                    "name": "mysql_connect",
                    "arguments": {
                        "name": "dup_sess",
                        "host": cfg.connection.host,
                        "port": cfg.connection.port,
                        "user": cfg.connection.user,
                        "password": cfg.connection.password,
                        "database": cfg.connection.database,
                        "ssl": cfg.security.ssl,
                        "ssl_ca": cfg.security.ssl_ca,
                    }
                }
            }),
        )
        .await;
        let resp2 = read_response(&mut reader)
            .await
            .expect("no response to second mysql_connect");
        assert_eq!(
            resp2["result"]["isError"], true,
            "duplicate session name should be rejected, got: {}",
            resp2
        );
        let err_text = resp2["result"]["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            err_text.contains("already exists"),
            "error should say 'already exists', got: {}",
            err_text
        );

        child.kill().await.ok();
    }
}
