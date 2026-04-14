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

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Session identifier (alphanumeric, underscore, hyphen; max 64 chars). `"default"` is reserved. |
| `host` | string | yes | Server hostname |
| `user` | string | yes | Username |
| `port` | integer | no | Server port |
| `password` | string | no | Password |
| `database` | string | no | Default database |
| `ssl` | boolean | no | Enable SSL/TLS (default: false). When true without `ssl_ca`, uses VerifyIdentity mode (validates server certificate and hostname). |
| `ssl_ca` | string | no | Path to PEM CA certificate file. When set (with `ssl=true`), uses VerifyCa mode (validates cert chain without hostname check). Requires `ssl=true`. |
| `ssl_accept_invalid_certs` | boolean | no | Skip certificate validation (not for production). When true with `ssl=true`, uses Required mode (encrypted but unverified). |
| `ssh_host` | string | no | SSH bastion hostname (enables SSH tunneling) |
| `ssh_port` | integer | no | SSH port (default: 22) |
| `ssh_user` | string | no | SSH username (required when `ssh_host` is set) |
| `ssh_private_key` | string | no | Path to SSH private key file |
| `ssh_known_hosts_check` | string | no | Host key verification: `strict` (default), `accept-new`, or `insecure` |

Sessions idle for more than 10 minutes are closed automatically. Pass the session `name` to other tools via the `session` parameter.

---

### `mysql_disconnect`

Explicitly close a named session. The default session cannot be closed.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Session name to close |

---

### `mysql_list_sessions`

List all active named sessions with host, database, and idle time. The default session is always shown first.

No parameters.

---

## Performance Guide

### `performance_hints` modes

| Value | Behaviour |
|---|---|
| `none` (default) | No EXPLAIN is run. Fastest for queries known to be efficient. |
| `auto` | EXPLAIN runs only when the query exceeds `slow_query_threshold_ms` (default 500 ms). Good for production: zero overhead on fast queries. |
| `always` | EXPLAIN runs after every SELECT. Useful during development or debugging. |

You can also override per-call by passing `"explain": true` to `mysql_query` or by using `mysql_explain_plan` before executing.

### `max_rows` protects against runaway results

By default, `max_rows = 1000`. When a SELECT has no LIMIT clause, `LIMIT 1000` is appended automatically and `capped: true` is set in the response. This prevents accidentally pulling back millions of rows. `max_rows` must be >= 1 (use a large value like 1000000 for effectively unlimited rows).

---

## Security

### Write permissions

By default, only read-only statements are permitted. Enable writes explicitly:

```bash
MYSQL_ALLOW_INSERT=true
MYSQL_ALLOW_UPDATE=true
MYSQL_ALLOW_DELETE=true
MYSQL_ALLOW_DDL=true
```

Or in TOML:

```toml
[security]
allow_insert = true
allow_update = true
allow_delete = false
allow_ddl = false
```

### Per-schema permission overrides

Override write permissions for individual databases without affecting the global defaults.

**Environment variable** (comma-separated list of allowed operations):

```bash
MYSQL_ALLOW_INSERT=false
MYSQL_SCHEMA_APP_DB_PERMISSIONS=insert,update
MYSQL_SCHEMA_ARCHIVE_DB_PERMISSIONS=
```

This allows INSERT and UPDATE only on `app_db`; no writes on `archive_db`; global default (false) applies to everything else.

**TOML**:

```toml
[security.schema_permissions.app_db]
allow_insert = true
allow_update = true
allow_delete = false
allow_ddl = false
```

Schema names in TOML are lowercase. The `MYSQL_SCHEMA_<NAME>_PERMISSIONS` env var name is case-insensitive in the `<NAME>` portion.


**Warning: SSH tunneling should never be combined with `allow_runtime_connections=true`.**
When using SSH tunnels, database credentials are protected by SSH authentication. Enabling
`allow_runtime_connections` would expose these credentials to the LLM over the MCP protocol,
defeating the security provided by SSH. If you need to use `mysql_connect` with SSH, ensure
`allow_runtime_connections=false` (the default) and manage connections through your SSH
configuration instead.

### SSL

**Warning: SSL with `allow_runtime_connections=true` exposes your database credentials to the LLM.**
When using SSL, database connections are encrypted and (optionally) certificate-verified. Enabling
`allow_runtime_connections` would allow the LLM to submit arbitrary SQL over these encrypted connections,
defeating the purpose of encryption if the LLM is not trusted.

### SSL

```bash
MYSQL_SSL=true
MYSQL_SSL_CA=/path/to/ca.pem         # VerifyCa mode (validates cert chain)
# For self-signed certs only — do not use in production:
MYSQL_SSL_ACCEPT_INVALID_CERTS=true
```

### SSL/TLS Troubleshooting

See the full [TROUBLESHOOTING.md](TROUBLESHOOTING.md) guide for comprehensive SSL/TLS debugging steps.

| Error | Fix |
|-------|-----|
| `unable to get local issuer certificate` / `certificate verify failed` | Verify `MYSQL_SSL_CA` points to the correct CA bundle; use `MYSQL_SSL_ACCEPT_INVALID_CERTS=true` for self-signed certs (development only) |
| `SSL connection error: unknown error number` | Check the MySQL server supports TLS 1.2+; verify `MYSQL_SSL_CA` file exists and is readable |
| `SSL CA file not found` | Ensure the path in `MYSQL_SSL_CA` is absolute and the file exists; check file permissions |
| Self-signed certificate rejected | Either set `MYSQL_SSL_ACCEPT_INVALID_CERTS=true` (insecure, development only) or add the self-signed cert to your system's trust store |
| `Bad handshake` / TLS version mismatch | Ensure client and server support compatible TLS versions; MySQL 8.0 defaults to TLS 1.2+ which may reject older clients |

---

## SSH Tunneling

mysql-mcp can route database connections through an SSH tunnel (jump host / bastion),
enabling access to databases that are not directly reachable from the machine running
the server.

```
[Local machine] → [SSH tunnel → bastion:22] → [DB host:3306]
```

### Prerequisites

| Platform | Requirement |
|----------|------------|
| macOS | Built-in OpenSSH — no installation needed |
| Linux | OpenSSH almost universally pre-installed |
| Windows | Install OpenSSH: enable via **Windows Features → Optional Features → OpenSSH Client**, or run `winget install Microsoft.OpenSSH.Beta`. Verify with `ssh -V`. |

### Configuration

**TOML (`mysql-mcp.toml`):**

```toml
[connection]
host = "db.internal"      # DB host as seen from the bastion
port = 3306
user = "dbuser"
password = "secret"

[ssh]
host = "bastion.example.com"
user = "ubuntu"
private_key = "/home/user/.ssh/id_rsa"   # or use SSH agent
known_hosts_check = "strict"             # strict | accept-new | insecure
```

**Environment variables:**

```bash
MYSQL_SSH_HOST=bastion.example.com
MYSQL_SSH_USER=ubuntu
MYSQL_SSH_PRIVATE_KEY=/home/user/.ssh/id_rsa
MYSQL_SSH_KNOWN_HOSTS_CHECK=strict
```

### Authentication

- **Key file (recommended for automation):** Set `private_key` to the key path. For encrypted keys, use SSH agent instead.
- **SSH agent:** The agent must be running and have the key loaded (`ssh-add /path/to/key`). This is required for encrypted private keys.

### Known Hosts

The default `known_hosts_check = "strict"` refuses connections to unknown hosts. For first-time setup, pre-populate your known_hosts file:

```bash
ssh-keyscan -H bastion.example.com >> ~/.ssh/known_hosts
```

Or temporarily use `accept-new` to add the key automatically on first connect:

```toml
[ssh]
known_hosts_check = "accept-new"
```

### Dynamic tunnels via `mysql_connect`

The `mysql_connect` tool also accepts SSH parameters for on-demand tunneled sessions:

```json
{
  "name": "mysql_connect",
  "arguments": {
    "name": "prod-tunneled",
    "host": "db.internal",
    "port": 3306,
    "user": "dbuser",
    "password": "secret",
    "ssh_host": "bastion.example.com",
    "ssh_user": "ubuntu",
    "ssh_known_hosts_check": "accept-new"
  }
}
```

Requires `MYSQL_ALLOW_RUNTIME_CONNECTIONS=true`.

### Troubleshooting

See the full [TROUBLESHOOTING.md](TROUBLESHOOTING.md) guide for comprehensive SSH tunnel debugging steps.

| Error | Fix |
|-------|-----|
| `'ssh' binary not found` | Install OpenSSH (see Prerequisites above) |
| `Host key verification failed` | Pre-add host with `ssh-keyscan`, or set `known_hosts_check = "accept-new"` |
| `Permission denied (publickey)` | Verify key path, check `ssh-add -l` if using agent |
| `Timed out waiting for local port` | Check SSH connectivity: `ssh user@bastion` interactively |
| Connection resets after idle | Expected — the 10-minute session reaper closes idle sessions |
=======

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