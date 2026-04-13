# Architecture

This document provides detailed technical documentation on the Model Context Protocol (MCP) implementation in mysql-mcp. For high-level architecture and user documentation, see [README.md](README.md).

## Table of Contents

- [MCP Protocol Overview](#mcp-protocol-overview)
- [JSON-RPC Message Flow](#json-rpc-message-flow)
- [Request/Response Lifecycle](#requestresponse-lifecycle)
- [Tool Registration Mechanism](#tool-registration-mechanism)
- [rmcp Library Integration](#rmcp-library-integration)
- [Server Initialization](#server-initialization)
- [Error Handling](#error-handling)

## MCP Protocol Overview

mysql-mcp implements the [Model Context Protocol](https://modelcontextprotocol.io) (MCP), a standardized protocol for exposing data and tools to AI assistants. MCP uses JSON-RPC 2.0 over stdio as its transport layer.

### Key Protocol Concepts

| Concept | Description | Implementation |
|---------|-------------|----------------|
| **Server** | Exposes tools and capabilities to clients | `McpServer` struct in `src/server/mod.rs` |
| **Tools** | Functions that perform operations (e.g., `mysql_query`) | Defined in `src/server/tool_schemas.rs` |
| **Requests** | JSON-RPC method calls from client to server | Handled via `ServerHandler` trait |
| **Responses** | JSON-RPC result or error returned to client | Serialized via `serialize_response()` |

### Protocol Version

The server implements MCP protocol version `2024-11-05` via the `rmcp` crate, using `ProtocolVersion::default()` in the server info response.

## JSON-RPC Message Flow

### Message Format

All MCP communication uses JSON-RPC 2.0 messages on stdin/stdout:

```
Client (AI Assistant)          Server (mysql-mcp)
        |                               |
        |-- {jsonrpc: "2.0", id: 1,    |
        |    method: "tools/list"} --> |
        |                               |
        |    <-- {jsonrpc: "2.0",       |
        |         id: 1, result: {...}} |
        |                               |
        |-- {jsonrpc: "2.0", id: 2,    |
        |    method: "tools/call",      |
        |    params: {...}} ----------> |
        |                               |
        |    <-- {jsonrpc: "2.0",       |
        |         id: 2, result: {...}}|
```

### Stdio Transport

The server uses stdio transport (`rmcp::transport::stdio`), meaning:

- **Input**: MCP client writes JSON-RPC requests to the server's stdin
- **Output**: Server writes JSON-RPC responses to stdout
- **Logging**: All logs MUST go to stderr to avoid corrupting the protocol stream

This is enforced in `src/main.rs`:

```rust
tracing_subscriber::fmt()
    .with_writer(std::io::stderr)  // Critical: logs to stderr only
    .init();
```

### Message Types

| MCP Method | Direction | Handler | Purpose |
|------------|-----------|---------|---------|
| `initialize` | Client → Server | `ServerHandler::get_info` | Server capability negotiation |
| `tools/list` | Client → Server | `ServerHandler::list_tools` | Discover available tools |
| `tools/call` | Client → Server | `ServerHandler::call_tool` | Execute a tool |
| `ping` | Bidirectional | Built-in | Connection health check |

## Request/Response Lifecycle

### 1. Initialization

When an MCP client connects, it sends an `initialize` request:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "clientInfo": {"name": "Claude", "version": "1.0"}
  }
}
```

The server responds with `ServerInfo` from `get_info()`:

```rust
fn get_info(&self) -> ServerInfo {
    ServerInfo {
        protocol_version: ProtocolVersion::default(),
        capabilities: ServerCapabilities::builder().enable_tools().build(),
        server_info: Implementation { /* ... */ },
        instructions: Some("Workflow documentation...".to_string()),
    }
}
```

### 2. Tool Discovery

After initialization, the client calls `tools/list` to discover available tools:

```rust
async fn list_tools(
    &self,
    _request: Option<PaginatedRequestParams>,
    _context: RequestContext<RoleServer>,
) -> Result<ListToolsResult, McpError> {
    let tools = vec![
        Tool::new("mysql_query", "Execute SQL...", mysql_query_schema()),
        Tool::new("mysql_schema_info", "Get table metadata...", mysql_schema_info_schema()),
        // ... 8 tools total
    ];
    Ok(ListToolsResult { tools, next_cursor: None, meta: None })
}
```

### 3. Tool Invocation

The client calls `tools/call` to execute a tool:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "mysql_query",
    "arguments": {
      "sql": "SELECT * FROM users LIMIT 10"
    }
  }
}
```

The server routes to the appropriate handler:

```rust
async fn call_tool(
    &self,
    request: CallToolRequestParams,
    _context: RequestContext<RoleServer>,
) -> Result<CallToolResult, McpError> {
    let args = request.arguments.unwrap_or_default();
    match request.name.as_ref() {
        "mysql_query" => self.store.handle_query(args).await,
        "mysql_schema_info" => self.store.handle_schema_info(args).await,
        // ... routing for all tools
        name => Err(McpError::new(ErrorCode::METHOD_NOT_FOUND, ...)),
    }
}
```

### 4. Response Serialization

Tool handlers return `CallToolResult` with JSON content:

```rust
pub(crate) fn serialize_response(value: &serde_json::Value) -> CallToolResult {
    match serde_json::to_string_pretty(value) {
        Ok(s) => CallToolResult::success(vec![Content::text(s)]),
        Err(e) => CallToolResult::error(vec![Content::text(format!("...", e))]),
    }
}
```

Example response:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\n  \"rows\": [...],\n  \"row_count\": 10\n}"
      }
    ],
    "isError": false
  }
}
```

## Tool Registration Mechanism

### Schema Definition

Each tool has a JSON Schema defining its input parameters in `src/server/tool_schemas.rs`:

```rust
pub(crate) fn mysql_query_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    Arc::new(rmcp::model::object(json!({
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SQL query to execute"
            },
            "session": {
                "type": "string",
                "description": "Named session to route this query to"
            },
            "explain": {
                "type": "boolean",
                "description": "Force an EXPLAIN run for this query"
            }
        },
        "required": ["sql"]
    })))
}
```

### Tool Registration Flow

```
┌─────────────────────────┐
│   ServerHandler::       │
│   list_tools()          │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│   Tool::new(             │
│     name,                 │
│     description,          │
│     input_schema          │
│   )                       │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│   Returns ListToolsResult│
│   to MCP client          │
└─────────────────────────┘
```

### Available Tools

| Tool Name | Purpose | Key Parameters |
|-----------|---------|----------------|
| `mysql_query` | Execute SQL | `sql`, `session`, `explain` |
| `mysql_schema_info` | Get table metadata | `table`, `database`, `include` |
| `mysql_server_info` | Server metadata | `session` |
| `mysql_list_tables` | List tables | `database`, `session` |
| `mysql_explain_plan` | Get execution plan | `sql`, `session` |
| `mysql_connect` | Create named session | `name`, `host`, `user`, `password` |
| `mysql_disconnect` | Close session | `name` |
| `mysql_list_sessions` | List active sessions | (none) |

## rmcp Library Integration

### Dependency

```toml
[dependencies]
rmcp = { version = "0.16", features = ["server", "transport-io"] }
```

### Key Traits and Types

| rmcp Type | Purpose | Usage in mysql-mcp |
|-----------|---------|---------------------|
| `ServerHandler` | Core server trait | Implemented by `McpServer` |
| `RoleServer` | Server role marker | Used in `RequestContext` |
| `CallToolRequestParams` | Tool call parameters | Parsed in `call_tool()` |
| `CallToolResult` | Tool response | Returned by all handlers |
| `ServerInfo` | Server capabilities | Returned by `get_info()` |
| `Tool` | Tool metadata | Created in `list_tools()` |
| `Content` | Response content | Wrapped in `CallToolResult` |

### Server Trait Implementation

```rust
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        // Return server capabilities and metadata
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        // Return list of available tools
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        // Route to specific tool handler
    }
}
```

### Running the Server

The server is started via `serve()` and `waiting()`:

```rust
pub async fn run(self) -> Result<()> {
    let service = self.serve(stdio()).await?;  // Bind to stdio transport
    service.waiting().await?;                   // Run until shutdown
    Ok(())
}
```

## Server Initialization

### Startup Sequence

```
┌─────────────────────────────────────────────────────┐
│ main.rs                                             │
│ 1. Initialize tracing to stderr                       │
│ 2. Load configuration (env + TOML + defaults)       │
│ 3. Build database pool (with optional SSH tunnel)   │
│ 4. Warm up pool with test connection                │
└──────────────┬──────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────┐
│ McpServer::new(config, pool, tunnel)              │
│ 1. Create SchemaIntrospector with cache             │
│ 2. Initialize SessionStore for named sessions       │
│ 3. Start session reaper background task             │
│ 4. Return McpServer instance                        │
└──────────────┬──────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────┐
│ mcp_server.run()                                    │
│ 1. Bind to stdio transport                          │
│ 2. Serve MCP requests                               │
│ 3. Wait for shutdown signal (Ctrl-C/SIGTERM)      │
└─────────────────────────────────────────────────────┘
```

### Session Reaper

A background task manages session lifecycle:

```rust
// Spawned in McpServer::new()
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            _ = interval.tick() => {}
        }
        // Reap sessions idle for >10 minutes
    }
});
```

## Error Handling

### MCP Error Responses

Errors are returned as `CallToolResult` with `is_error: Some(true)`:

```rust
pub(crate) fn error_response(msg: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![Content::text(msg.into())])
}
```

### Error Categories

| Category | Example | Response Format |
|----------|---------|-----------------|
| Invalid parameters | Missing required arg | `{"error": "Missing required argument: sql"}` |
| Permission denied | INSERT when disabled | `{"error": "INSERT operation denied..."}` |
| SQL errors | Syntax error | `{"error": "SQL parse error: ..."}` |
| Connection errors | DB unreachable | `{"error": "Connection failed: ..."}` |
| Resource limits | Max sessions reached | `{"error": "Maximum session limit..."}` |

### Error Flow

```
┌─────────────────────────┐
│   Tool Handler          │
│   (e.g., handle_query)  │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐     ┌─────────────────────────┐
│   Validation Error?     │──Yes──▶│ tool_error!() macro     │
└──────────┬──────────────┘     └──────────┬──────────────┘
           │ No                             │
           ▼                                ▼
┌─────────────────────────┐     ┌─────────────────────────┐
│   Execute Operation     │     │   CallToolResult::      │
└──────────┬──────────────┘     │   error(vec![...])      │
           │                    └──────────┬──────────────┘
           ▼                               │
┌─────────────────────────┐                │
│   Execution Error?      │──Yes───────────┘
└──────────┬──────────────┘
           │ No
           ▼
┌─────────────────────────┐
│   serialize_response()  │
│   (success)             │
└─────────────────────────┘
```

### Validation Macro

The `tool_error!` macro creates consistent error responses:

```rust
macro_rules! tool_error {
    ($($arg:tt)*) => {
        Ok(crate::server::error::error_response(format!($($arg)*)))
    };
}
```

This ensures all errors are wrapped in `Ok(CallToolResult::error(...))` for proper JSON-RPC error serialization.
