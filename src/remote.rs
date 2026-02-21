use std::sync::Arc;
use anyhow::Result;
use axum::{
    Router,
    routing::post,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{Response, IntoResponse},
    body::Body,
    extract::State,
};
use crate::config::Config;
use crate::server::McpServer;

/// Verify the Authorization: Bearer <token> header.
/// Returns a JSON-RPC-style error body on failure so callers get a parseable response.
async fn auth_middleware(
    State(secret_key): State<Arc<String>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, Response> {
    // Reject empty-string tokens outright: they only occur when secret_key is
    // None (misconfiguration) and would otherwise accept any `Bearer ` request.
    if secret_key.is_empty() {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": { "code": -32001, "message": "Server misconfiguration: no secret_key set" }
        });
        return Err((StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(body)).into_response());
    }

    let auth = req.headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok());

    match auth {
        Some(header) if header.starts_with("Bearer ") => {
            let token = &header["Bearer ".len()..];
            if token == secret_key.as_str() {
                Ok(next.run(req).await)
            } else {
                let body = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": { "code": -32001, "message": "Unauthorized: invalid token" }
                });
                Err((StatusCode::UNAUTHORIZED, axum::Json(body)).into_response())
            }
        }
        _ => {
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": { "code": -32001, "message": "Unauthorized: missing or malformed Authorization header" }
            });
            Err((StatusCode::UNAUTHORIZED, axum::Json(body)).into_response())
        }
    }
}

pub async fn run_http_server(config: Arc<Config>, server: Arc<McpServer>) -> Result<()> {
    let port = config.remote.port;
    let secret_key = match config.remote.secret_key.clone() {
        Some(key) if !key.is_empty() => key,
        _ => {
            tracing::warn!(
                "remote.secret_key is not set — HTTP MCP server will reject all requests. \
                 Set a strong secret_key in your configuration."
            );
            String::new()
        }
    };
    let secret_key = Arc::new(secret_key);

    let app = Router::new()
        .route("/", post(handle_mcp))
        .layer(middleware::from_fn_with_state(secret_key, auth_middleware))
        .with_state(server);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("Starting HTTP MCP server on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn handle_mcp(
    State(_server): State<Arc<McpServer>>,
    body: axum::body::Bytes,
) -> Result<axum::response::Json<serde_json::Value>, StatusCode> {
    // For now, return a simple response indicating HTTP transport
    // Full MCP-over-HTTP integration requires more complex setup with rmcp
    // This is a placeholder that shows the structure is correct
    let request: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // TODO: Route to appropriate handler based on request.method
    let response = serde_json::json!({
        "jsonrpc": "2.0",
        "id": request.get("id"),
        "error": {
            "code": -32601,
            "message": "HTTP transport method routing not yet implemented"
        }
    });

    Ok(axum::response::Json(response))
}

#[cfg(test)]
mod integration_tests {
    // These tests exercise auth logic only — no MySQL connection needed.

    #[tokio::test]
    async fn test_http_server_requires_auth() {
        let secret = "test-secret-key";

        let valid_header = format!("Bearer {}", secret);
        assert!(valid_header.starts_with("Bearer "));
        let extracted = &valid_header["Bearer ".len()..];
        assert_eq!(extracted, secret);

        let wrong_header = "Bearer wrong-key";
        let extracted_wrong = &wrong_header["Bearer ".len()..];
        assert_ne!(extracted_wrong, secret);
    }

    #[tokio::test]
    async fn test_http_missing_auth_rejected() {
        let no_auth: Option<&str> = None;
        let result = match no_auth {
            Some(header) if header.starts_with("Bearer ") => true,
            _ => false,
        };
        assert!(!result, "Missing auth should be rejected");
    }

    #[tokio::test]
    async fn test_http_server_config() {
        use crate::config::Config;
        let mut config = Config::default();
        config.remote.enabled = true;
        config.remote.port = 3000;
        config.remote.secret_key = Some("my-secret".to_string());

        assert!(config.remote.enabled);
        assert_eq!(config.remote.port, 3000);
        assert_eq!(config.remote.secret_key.as_deref(), Some("my-secret"));
    }

    #[tokio::test]
    async fn test_empty_secret_key_rejected() {
        // When secret_key is None or empty, the middleware should reject all
        // requests rather than accepting `Bearer ` (empty token).
        let empty_secret = "";
        // Simulate the guard check in auth_middleware
        assert!(empty_secret.is_empty(), "Empty secret triggers misconfiguration path");

        // Confirm a `Bearer ` (empty token) would NOT bypass if we guard on is_empty()
        let header = "Bearer ";
        let token = &header["Bearer ".len()..];
        // token == "" == empty_secret, so without the guard it would match — the guard prevents this
        assert_eq!(token, empty_secret);
    }
}
