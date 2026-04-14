# sql-mcp

[![CI](https://github.com/cecil-the-coder/mysql-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/cecil-the-coder/mysql-mcp/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A multi-backend database MCP (Model Context Protocol) server written in Rust. It exposes MySQL, PostgreSQL, and SQLite databases to LLM assistants via the MCP standard, allowing them to run SQL queries, inspect table schemas, obtain query execution plans, and receive performance warnings — all through a simple tool interface.

## Features

- **Multi-backend**: MySQL, PostgreSQL, and SQLite support via feature flags
- **Read queries**: SELECT, SHOW, EXPLAIN — always allowed
- **Write operations**: INSERT, UPDATE, DELETE — opt-in via config
- **DDL operations**: CREATE, ALTER, DROP, TRUNCATE — opt-in
- **Performance hints**: automatic EXPLAIN + index suggestions on slow queries
- **Named sessions**: connect to additional databases at runtime via `connect`
- **SSH tunneling**: reach databases behind a bastion/jump host (MySQL, PostgreSQL)
- **Per-schema permissions**: fine-grained write control per database
- **Connection pooling**: configurable pool size and timeouts
- **SSL support**: encrypted connections with optional CA verification (MySQL, PostgreSQL)
- **Unix socket support**: connect via socket path instead of host:port (MySQL)

## Quick Start

### Installation

```bash
cargo install --git https://github.com/cecil-the-coder/mysql-mcp
```

Or build from source:

```bash
cargo build --release
```

### Run with environment variables (MySQL)

```bash
export DB_BACKEND=mysql
export DB_HOST=localhost
export DB_PORT=3306
export DB_USER=myuser
export DB_PASS=mypassword
export DB_DATABASE=mydatabase

./sql-mcp
```

### Run with environment variables (PostgreSQL)

```bash
export DB_BACKEND=postgres
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=myuser
export DB_PASS=mypassword
export DB_DATABASE=mydatabase

./sql-mcp
```

### Run with environment variables (SQLite)

```bash
export DB_BACKEND=sqlite
export DB_PATH=/path/to/mydb.sqlite3

./sql-mcp
```

### Claude Desktop configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "mysql": {
      "command": "/path/to/sql-mcp",
      "env": {
        "DB_BACKEND": "mysql",
        "DB_HOST": "localhost",
        "DB_PORT": "3306",
        "DB_USER": "myuser",
        "DB_PASS": "mypassword",
        "DB_DATABASE": "mydatabase"
      }
    }
  }
}
```

### Claude Code configuration

Add to `.claude/settings.json` (project) or your global Claude Code settings:

```json
{
  "mcpServers": {
    "mysql": {
      "command": "/path/to/sql-mcp",
      "env": {
        "DB_HOST": "localhost",
        "DB_USER": "myuser",
        "DB_DATABASE": "mydatabase"
      }
    }
  }
}
```

### TOML config file

Create `sql-mcp.toml` in the working directory (or point `MCP_CONFIG_FILE` at a custom path):

**MySQL:**

```toml
[connection]
host = "localhost"
port = 3306
user = "myuser"
password = "mypassword"
database = "mydatabase"

[pool]
size = 20
max_rows = 1000

[security]
allow_insert = false
allow_update = false
```

**PostgreSQL:**

```toml
backend = "postgres"

[connection]
host = "localhost"
port = 5432
user = "myuser"
password = "mypassword"
database = "mydatabase"
```

**SQLite:**

```toml
backend = "sqlite"

[connection]
path = "/path/to/mydb.sqlite3"
```

Configuration is loaded in this order (highest priority wins):
1. Environment variables
2. TOML config file (`sql-mcp.toml` or `$MCP_CONFIG_FILE`)
3. Built-in defaults

A `.env` file in the working directory is loaded automatically if present.

---

## Configuration Reference

### Backend

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `backend` | `DB_BACKEND` | string | `mysql` | Database backend: `mysql`, `postgres`, or `sqlite` |

### Connection

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `connection.host` | `DB_HOST` | string | `localhost` | Server hostname (MySQL, PostgreSQL) |
| `connection.port` | `DB_PORT` | u16 | — | Server port (defaults: MySQL 3306, PostgreSQL 5432) |
| `connection.path` | `DB_PATH` | string | — | SQLite database file path |
| `connection.user` | `DB_USER` | string | — | Username (MySQL, PostgreSQL) |
| `connection.password` | `DB_PASS` | string | `""` | Password (MySQL, PostgreSQL) |
| `connection.database` | `DB_DATABASE` | string | — | Default database name |
| `connection.connection_string` | `DB_CONNECTION_STRING` | string | — | Full connection URL; overrides other connection fields |

### Pool

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `pool.size` | `DB_POOL_SIZE` | u32 | `20` | Maximum number of pooled connections |
| `pool.query_timeout_ms` | `DB_QUERY_TIMEOUT` | u64 | `30000` | Per-query timeout in milliseconds |
| `pool.connect_timeout_ms` | `DB_CONNECT_TIMEOUT` | u64 | `10000` | Connection establishment timeout in milliseconds |
| `pool.performance_hints` | `DB_PERFORMANCE_HINTS` | string | `none` | When to run EXPLAIN: `none`, `auto`, or `always` |
| `pool.slow_query_threshold_ms` | `DB_SLOW_QUERY_THRESHOLD_MS` | u64 | `500` | Threshold used by `performance_hints=auto` |
| `pool.max_rows` | `DB_MAX_ROWS` | u32 | `1000` | Cap on rows returned per query |
| `pool.cache_ttl_secs` | `DB_CACHE_TTL` | u64 | `60` | Schema introspection cache TTL in seconds |
| `pool.retry_attempts` | `DB_RETRY_ATTEMPTS` | u32 | `2` | Retry attempts on transient network errors |

### Security

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `security.allow_insert` | `DB_ALLOW_INSERT` | bool | `false` | Permit INSERT statements |
| `security.allow_update` | `DB_ALLOW_UPDATE` | bool | `false` | Permit UPDATE statements |
| `security.allow_delete` | `DB_ALLOW_DELETE` | bool | `false` | Permit DELETE statements |
| `security.allow_ddl` | `DB_ALLOW_DDL` | bool | `false` | Permit DDL statements (CREATE, ALTER, DROP, TRUNCATE) |
| `security.ssl` | `DB_SSL` | bool | `false` | Require SSL/TLS for the connection |
| `security.ssl_accept_invalid_certs` | `DB_SSL_ACCEPT_INVALID_CERTS` | bool | `false` | Skip certificate validation |
| `security.ssl_ca` | `DB_SSL_CA` | string | — | Path to PEM CA certificate file |
| `security.schema_permissions` | `DB_SCHEMA_<NAME>_PERMISSIONS` | map | `{}` | Per-schema write permission overrides |
| `security.allow_runtime_connections` | `DB_ALLOW_RUNTIME_CONNECTIONS` | bool | `false` | Allow `connect` to accept raw credentials at runtime |
| `security.max_sessions` | `DB_MAX_SESSIONS` | u32 | `50` | Maximum number of concurrent named sessions |

### SSH Tunnel (MySQL, PostgreSQL only)

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `ssh.host` | `DB_SSH_HOST` | string | — | SSH bastion hostname |
| `ssh.port` | `DB_SSH_PORT` | u16 | `22` | SSH server port |
| `ssh.user` | `DB_SSH_USER` | string | — | SSH username |
| `ssh.private_key` | `DB_SSH_PRIVATE_KEY` | string | — | Path to PEM private key file |
| `ssh.known_hosts_check` | `DB_SSH_KNOWN_HOSTS_CHECK` | string | `strict` | Host key verification: `strict`, `accept-new`, or `insecure` |
| `ssh.known_hosts_file` | `DB_SSH_KNOWN_HOSTS_FILE` | string | — | Override path to known_hosts file |

### Feature Flags

```bash
# Build all backends (default)
cargo build

# MySQL only
cargo build --no-default-features --features mysql

# PostgreSQL only
cargo build --no-default-features --features postgres

# SQLite only
cargo build --no-default-features --features sqlite

# MySQL + PostgreSQL (no SQLite)
cargo build --no-default-features --features "mysql,postgres"
```

---

## MCP Tools

### `query`

Execute a SQL statement.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `sql` | string | yes | The SQL statement to execute |
| `explain` | boolean | no | Force an EXPLAIN run for this query |
| `session` | string | no | Named session to route this query to |

### `schema_info`

Get schema metadata for a table.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `table` | string | yes | Table name |
| `database` | string | no | Database/schema name |
| `include` | array of strings | no | Additional metadata: `"indexes"`, `"foreign_keys"`, `"size"` |
| `session` | string | no | Named session to use |

### `server_info`

Get server metadata: version, current database, current user, and backend-specific settings.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `session` | string | no | Named session to use |

### `explain_plan`

Get the execution plan for a SELECT query without running it.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `sql` | string | yes | The SELECT statement to explain |
| `session` | string | no | Named session to use |

### `connect`

Create a named session to a different database server. Requires `DB_ALLOW_RUNTIME_CONNECTIONS=true`.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Session identifier |
| `host` | string | yes | Server hostname |
| `user` | string | yes | Username |
| `port` | integer | no | Server port |
| `password` | string | no | Password |
| `database` | string | no | Default database |
| `ssl` | boolean | no | Enable SSL/TLS |
| `ssl_ca` | string | no | Path to CA certificate file |
| `ssh_host` | string | no | SSH bastion hostname |
| `ssh_port` | integer | no | SSH port |
| `ssh_user` | string | no | SSH username |
| `ssh_private_key` | string | no | Path to SSH private key |

### `disconnect`

Explicitly close a named session.

### `list_sessions`

List all active named sessions.

### `list_tables`

List all tables in the current or specified database.

---

## Development

```bash
# Run tests (MySQL via testcontainers, PG/SQLite unit tests)
cargo test

# Run tests against a real MySQL instance
DB_HOST=localhost DB_USER=root DB_DATABASE=mydb cargo test

# Build release binary
cargo build --release
```

## License

MIT
