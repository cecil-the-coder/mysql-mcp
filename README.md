# mysql-mcp

A MySQL MCP (Model Context Protocol) server written in Rust. Exposes MySQL databases to AI assistants via the MCP standard, enabling natural language database queries.

## Features

- **Read queries**: SELECT, SHOW, EXPLAIN — always allowed
- **Write operations**: INSERT, UPDATE, DELETE — opt-in via environment variables
- **DDL operations**: CREATE, ALTER, DROP, TRUNCATE — opt-in
- **Multi-database mode**: Query across multiple databases when no default DB is set
- **Per-schema permissions**: Fine-grained write control per database
- **Schema introspection**: Browse tables and column metadata via MCP resources
- **HTTP remote mode**: Serve MCP over HTTP with Bearer token auth
- **Connection pooling**: Configurable pool size, timeouts, and keepalives
- **SSL support**: Encrypted connections to MySQL
- **Unix socket support**: Connect via socket path instead of host:port

## Installation

### From source

```bash
cargo install --git https://github.com/yourusername/mysql-mcp
```

### Pre-built binaries

Download from the [Releases](../../releases) page:
- `mysql-mcp-x86_64-unknown-linux-musl.tar.gz` — Linux x86_64 (static)
- `mysql-mcp-aarch64-unknown-linux-gnu.tar.gz` — Linux ARM64
- `mysql-mcp-x86_64-apple-darwin.tar.gz` — macOS Intel
- `mysql-mcp-aarch64-apple-darwin.tar.gz` — macOS Apple Silicon
- `mysql-mcp-x86_64-pc-windows-msvc.zip` — Windows

## Quick Start

```bash
# Set MySQL connection details
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASS=yourpassword
export MYSQL_DB=mydatabase

# Run the server (stdio transport, for use with Claude)
./mysql-mcp
```

## Claude Desktop / Claude Code Configuration

Add to your MCP configuration:

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "mysql": {
      "command": "/path/to/mysql-mcp",
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "root",
        "MYSQL_PASS": "yourpassword",
        "MYSQL_DB": "mydatabase"
      }
    }
  }
}
```

**Claude Code** (`.claude/settings.json` or global settings):
```json
{
  "mcpServers": {
    "mysql": {
      "command": "/path/to/mysql-mcp",
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_USER": "root",
        "MYSQL_DB": "mydatabase"
      }
    }
  }
}
```

## Configuration

Configuration is loaded from (in order of precedence):
1. Environment variables (highest priority)
2. `mysql-mcp.toml` config file (or `$MCP_CONFIG_FILE`)

See `mysql-mcp.example.toml` for a fully documented example.

### All Configuration Options

| Environment Variable | TOML Key | Default | Description |
|---------------------|----------|---------|-------------|
| `MYSQL_HOST` | `connection.host` | `localhost` | MySQL server hostname |
| `MYSQL_PORT` | `connection.port` | `3306` | MySQL server port |
| `MYSQL_SOCKET_PATH` | `connection.socket` | — | Unix socket path (overrides host:port) |
| `MYSQL_USER` | `connection.user` | `root` | MySQL username |
| `MYSQL_PASS` | `connection.password` | — | MySQL password |
| `MYSQL_DB` | `connection.database` | — | Target database (empty = multi-DB mode) |
| `MYSQL_CONNECTION_STRING` | `connection.connection_string` | — | Full `mysql://` URL or CLI-style string |
| `MYSQL_POOL_SIZE` | `pool.size` | `10` | Connection pool size |
| `MYSQL_QUERY_TIMEOUT` | `pool.query_timeout_ms` | `30000` | Query timeout (ms) |
| `MYSQL_CONNECT_TIMEOUT` | `pool.connect_timeout_ms` | `10000` | Connection timeout (ms) |
| `MYSQL_QUEUE_LIMIT` | `pool.queue_limit` | `100` | Max queued connection requests |
| `MYSQL_CACHE_TTL` | `pool.cache_ttl_secs` | `60` | Schema cache TTL (0 = disabled) |
| `ALLOW_INSERT_OPERATION` | `security.allow_insert` | `false` | Allow INSERT statements |
| `ALLOW_UPDATE_OPERATION` | `security.allow_update` | `false` | Allow UPDATE statements |
| `ALLOW_DELETE_OPERATION` | `security.allow_delete` | `false` | Allow DELETE statements |
| `ALLOW_DDL_OPERATION` | `security.allow_ddl` | `false` | Allow DDL (CREATE/ALTER/DROP/TRUNCATE) |
| `MYSQL_DISABLE_READ_ONLY_TRANSACTIONS` | `security.disable_read_only_transactions` | `false` | Disable read-only transaction wrapper |
| `MYSQL_SSL` | `security.ssl` | `false` | Require SSL |
| `MULTI_DB_WRITE_MODE` | `security.multi_db_write_mode` | `false` | Allow writes in multi-DB mode |
| `IS_REMOTE_MCP` | `remote.enabled` | `false` | Enable HTTP transport |
| `REMOTE_SECRET_KEY` | `remote.secret_key` | — | Bearer token for HTTP auth |
| `PORT` | `remote.port` | `3000` | HTTP server port |
| `MYSQL_ENABLE_LOGGING` | `monitoring.logging` | `true` | Enable request logging |
| `MYSQL_LOG_LEVEL` | `monitoring.log_level` | `info` | Log level (error/warn/info/debug/trace) |
| `MYSQL_METRICS_ENABLED` | `monitoring.metrics_enabled` | `false` | Enable metrics |
| `MYSQL_TIMEZONE` | `timezone` | — | Session timezone (e.g., `UTC`, `+08:00`) |
| `MYSQL_DATE_STRINGS` | `date_strings` | `false` | Return dates as strings |

### Schema-Specific Permissions

Override write permissions per database using `SCHEMA_<NAME>_PERMISSIONS` env vars:

```bash
# Allow writes on 'app_db', block on everything else
ALLOW_INSERT_OPERATION=false
SCHEMA_APP_DB_PERMISSIONS=insert,update
```

Or in TOML:
```toml
[security.schema_permissions.app_db]
allow_insert = true
allow_update = true
allow_delete = false
```

## Multi-Database Mode

When `MYSQL_DB` is empty (or `connection.database` is not set), the server operates in multi-database mode:
- Lists tables from all non-system databases
- Use fully-qualified names: `SELECT * FROM mydb.users`
- Writes require `MULTI_DB_WRITE_MODE=true`

## HTTP Remote Mode

To serve MCP over HTTP (for remote clients):

```bash
IS_REMOTE_MCP=true
REMOTE_SECRET_KEY=your-secret-key
PORT=3000
./mysql-mcp
```

Clients must include: `Authorization: Bearer your-secret-key`

## MCP Tools

### `mysql_query`

Execute SQL against the connected MySQL database.

**Input**: `{ "sql": "SELECT * FROM users LIMIT 10" }`

**Output** (read queries):
```json
{
  "rows": [{"id": 1, "name": "Alice"}, "..."],
  "row_count": 1,
  "execution_time_ms": 3
}
```

**Output** (write queries):
```json
{
  "rows_affected": 1,
  "last_insert_id": 42,
  "execution_time_ms": 5
}
```

## MCP Resources

- `mysql://tables` — List all accessible tables with metadata
- `mysql://tables/{schema}/{table}` — Get column schema for a specific table

## Development

```bash
# Run unit tests (no MySQL needed)
cargo test

# Run with MySQL available
MYSQL_HOST=localhost MYSQL_USER=root cargo test

# Build release binary
cargo build --release

# Install pre-commit hooks
make install-hooks
```

## License

MIT
