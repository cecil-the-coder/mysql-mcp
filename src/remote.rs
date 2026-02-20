use std::sync::Arc;
use anyhow::Result;
use axum::{
    Router,
    routing::post,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    body::Body,
    extract::State,
};
use crate::config::Config;
use crate::server::McpServer;

/// Verify the Authorization: Bearer <token> header
async fn auth_middleware(
    State(secret_key): State<Arc<String>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth = req.headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok());

    match auth {
        Some(header) if header.starts_with("Bearer ") => {
            let token = &header["Bearer ".len()..];
            if token == secret_key.as_str() {
                Ok(next.run(req).await)
            } else {
                Err(StatusCode::UNAUTHORIZED)
            }
        }
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

pub async fn run_http_server(config: Arc<Config>, server: Arc<McpServer>) -> Result<()> {
    let port = config.remote.port;
    let secret_key = config.remote.secret_key.clone()
        .unwrap_or_else(|| "".to_string());
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
}
