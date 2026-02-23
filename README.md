# mysql-mcp

A MySQL MCP (Model Context Protocol) server written in Rust. It exposes a MySQL database to LLM assistants via the MCP standard, allowing them to run SQL queries, inspect table schemas, obtain query execution plans, and receive performance warnings — all through a simple tool interface.

## Features

- **Read queries**: SELECT, SHOW, EXPLAIN — always allowed
- **Write operations**: INSERT, UPDATE, DELETE — opt-in via config
- **DDL operations**: CREATE, ALTER, DROP, TRUNCATE — opt-in
- **Parallel queries**: `mysql_multi_query` runs independent SELECTs concurrently
- **Performance hints**: automatic EXPLAIN + index suggestions on slow queries
- **Per-schema permissions**: fine-grained write control per database
- **Schema introspection**: browse tables and column metadata via MCP resources
- **HTTP remote mode**: serve MCP over HTTP with Bearer token auth
- **Connection pooling**: configurable pool size, timeouts, and warm-up
- **SSL support**: encrypted connections with optional cert validation bypass
- **Unix socket support**: connect via socket path instead of host:port

## Quick Start

### Installation

```bash
cargo install --git https://github.com/yourusername/mysql-mcp
```

Or download a pre-built binary from the [Releases](../../releases) page.

### Run with environment variables

```bash
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=myuser
export MYSQL_PASS=mypassword
export MYSQL_DB=mydatabase

./mysql-mcp
```

### Claude Desktop configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "mysql": {
      "command": "/path/to/mysql-mcp",
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "myuser",
        "MYSQL_PASS": "mypassword",
        "MYSQL_DB": "mydatabase"
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
      "command": "/path/to/mysql-mcp",
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_USER": "myuser",
        "MYSQL_DB": "mydatabase"
      }
    }
  }
}
```

### TOML config file

Alternatively, create `mysql-mcp.toml` in the working directory (or point `MCP_CONFIG_FILE` at a custom path):

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

Configuration is loaded in this order (highest priority wins):
1. Environment variables
2. TOML config file (`mysql-mcp.toml` or `$MCP_CONFIG_FILE`)
3. Built-in defaults

A `.env` file in the working directory is loaded automatically if present.

---

## Configuration Reference

### Connection

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `connection.host` | `MYSQL_HOST` | string | `localhost` | MySQL server hostname |
| `connection.port` | `MYSQL_PORT` | u16 | `3306` | MySQL server port |
| `connection.socket` | `MYSQL_SOCKET_PATH` | string | — | Unix socket path; overrides host/port when set |
| `connection.user` | `MYSQL_USER` | string | `root` | MySQL username |
| `connection.password` | `MYSQL_PASS` | string | `""` | MySQL password |
| `connection.database` | `MYSQL_DB` | string | — | Default database; omit for multi-database mode |
| `connection.connection_string` | `MYSQL_CONNECTION_STRING` | string | — | Full `mysql://user:pass@host/db` URL, or mysql CLI-style flags (`-h host -P 3307 -u user -p pass dbname`) |

### Pool

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `pool.size` | `MYSQL_POOL_SIZE` | u32 | `20` | Maximum number of pooled connections |
| `pool.query_timeout_ms` | `MYSQL_QUERY_TIMEOUT` | u64 | `30000` | Per-query timeout in milliseconds |
| `pool.connect_timeout_ms` | `MYSQL_CONNECT_TIMEOUT` | u64 | `10000` | Connection establishment timeout in milliseconds; also serves as the acquire timeout — callers that cannot obtain a connection within this window receive an error |
| `pool.readonly_transaction` | `MYSQL_READONLY_TRANSACTION` | bool | `false` | Wrap every SELECT in a `SET TRANSACTION READ ONLY` + `BEGIN` + `COMMIT` (4-RTT). Leave `false` for bare 1-RTT fetches |
| `pool.performance_hints` | `MYSQL_PERFORMANCE_HINTS` | string | `none` | When to run EXPLAIN: `none`, `auto` (only when query exceeds `slow_query_threshold_ms`), or `always` |
| `pool.slow_query_threshold_ms` | `MYSQL_SLOW_QUERY_THRESHOLD_MS` | u64 | `500` | Threshold used by `performance_hints=auto` |
| `pool.max_rows` | `MYSQL_MAX_ROWS` | u32 | `1000` | Cap on rows returned per query; `LIMIT {max_rows}` is appended when the query has no LIMIT. `0` disables the cap |
| `pool.warmup_connections` | `MYSQL_POOL_WARMUP` | u32 | `1` | Number of connections to pre-open at startup |
| `pool.cache_ttl_secs` | `MYSQL_CACHE_TTL` | u64 | `60` | Schema introspection cache TTL in seconds (`0` disables caching) |

### Security

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `security.allow_insert` | `MYSQL_ALLOW_INSERT` | bool | `false` | Permit INSERT statements |
| `security.allow_update` | `MYSQL_ALLOW_UPDATE` | bool | `false` | Permit UPDATE statements |
| `security.allow_delete` | `MYSQL_ALLOW_DELETE` | bool | `false` | Permit DELETE statements |
| `security.allow_ddl` | `MYSQL_ALLOW_DDL` | bool | `false` | Permit DDL statements (CREATE, ALTER, DROP, TRUNCATE) |
| `security.ssl` | `MYSQL_SSL` | bool | `false` | Require SSL/TLS for the connection |
| `security.ssl_accept_invalid_certs` | `MYSQL_SSL_ACCEPT_INVALID_CERTS` | bool | `false` | Skip certificate validation (useful for self-signed certs; not for production) |
| `security.schema_permissions` | `MYSQL_SCHEMA_<NAME>_PERMISSIONS` | map | `{}` | Per-schema write permission overrides (see below) |
| `security.multi_db_write_mode` | `MYSQL_MULTI_DB_WRITE_MODE` | bool | `false` | Allow writes when no default database is set (multi-DB mode) |
| `security.allow_runtime_connections` | `MYSQL_ALLOW_RUNTIME_CONNECTIONS` | bool | `false` | Allow `mysql_connect` to accept raw credentials at runtime; when `false` only preset-based connections are permitted |
| `security.max_sessions` | `MYSQL_MAX_SESSIONS` | u32 | `50` | Maximum number of concurrent named sessions (not counting the default session); prevents unbounded session creation when `allow_runtime_connections` is `true` |

### Monitoring

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `monitoring.logging` | `MYSQL_ENABLE_LOGGING` | bool | `true` | Enable request/response logging |
| `monitoring.log_level` | `MYSQL_LOG_LEVEL` | string | `info` | Log verbosity: `error`, `warn`, `info`, `debug`, `trace` |
| `monitoring.metrics_enabled` | `MYSQL_METRICS_ENABLED` | bool | `false` | Enable metrics collection |

### Remote / HTTP mode

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `remote.enabled` | `IS_REMOTE_MCP` | bool | `false` | Serve MCP over HTTP instead of stdio |
| `remote.secret_key` | `REMOTE_SECRET_KEY` | string | — | Bearer token required for HTTP clients |
| `remote.port` | `PORT` | u16 | `3000` | HTTP listen port |

### Other

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `timezone` | `MYSQL_TIMEZONE` | string | — | MySQL session timezone, e.g. `UTC` or `+08:00` |
| `date_strings` | `MYSQL_DATE_STRINGS` | bool | `false` | Return DATE/DATETIME values as strings instead of native types |

---

## MCP Tools

### `mysql_query`

Execute a single SQL statement.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `sql` | string | yes | The SQL statement to execute |
| `explain` | boolean | no | Force an EXPLAIN run for this query, regardless of the `performance_hints` setting |

**Supported statement types**

- Always allowed: SELECT, SHOW, EXPLAIN, DESCRIBE
- Allowed when configured: INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, TRUNCATE

**Response fields — read queries (SELECT / SHOW / EXPLAIN)**

| Field | Type | Always present | Description |
|---|---|---|---|
| `rows` | array of objects | yes | Result rows; each object maps column name to value |
| `row_count` | number | yes | Number of rows in `rows` |
| `execution_time_ms` | number | yes | Time spent waiting for MySQL to return results |
| `serialization_time_ms` | number | yes | Time spent converting MySQL rows to JSON |
| `capped` | boolean | only when `true` | Present and `true` when `max_rows` was applied and the result was truncated |
| `parse_warnings` | array of strings | only when non-empty | Static analysis warnings (e.g. `SELECT *`, missing WHERE on UPDATE) |
| `plan` | object | only when EXPLAIN ran | Query execution plan (see below) |
| `suggestions` | array of strings | only when applicable | Index suggestions when a full table scan is detected with no index on WHERE columns |

**`plan` object fields**

| Field | Type | Description |
|---|---|---|
| `full_table_scan` | boolean | Whether MySQL performed a full table scan |
| `index_used` | string or null | Name of the index used, or null if none |
| `rows_examined_estimate` | number | MySQL's estimate of rows examined |
| `filtered_pct` | number | Estimated percentage of rows remaining after filtering |
| `efficiency` | number | rows_returned / rows_examined_estimate |
| `extra_flags` | array of strings | MySQL EXPLAIN Extra field tokens (e.g. `Using filesort`) |
| `tier` | string | `fast` (index used, ≤1k rows examined), `slow` (full scan or >1k rows), `very_slow` (full scan + >10k rows) |

**Response fields — write queries (INSERT / UPDATE / DELETE)**

| Field | Type | Always present | Description |
|---|---|---|---|
| `rows_affected` | number | yes | Number of rows modified |
| `execution_time_ms` | number | yes | Query execution time |
| `last_insert_id` | number | only for INSERT | Auto-increment ID of the last inserted row |
| `parse_warnings` | array of strings | only when non-empty | Static analysis warnings |

**Response fields — DDL queries (CREATE / ALTER / DROP / TRUNCATE)**

Same as write queries: `rows_affected`, `execution_time_ms`, optionally `last_insert_id` and `parse_warnings`.

**Example — read query**

```json
{
  "sql": "SELECT id, name FROM users WHERE active = 1",
  "explain": false
}
```

```json
{
  "rows": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ],
  "row_count": 2,
  "execution_time_ms": 4,
  "serialization_time_ms": 0
}
```

**Example — with performance hints**

With `performance_hints=always` (or `explain: true`), a slow query also returns a `plan`:

```json
{
  "rows": [...],
  "row_count": 950,
  "execution_time_ms": 820,
  "serialization_time_ms": 12,
  "capped": true,
  "plan": {
    "full_table_scan": true,
    "index_used": null,
    "rows_examined_estimate": 98000,
    "filtered_pct": 10.0,
    "efficiency": 0.0097,
    "extra_flags": ["Using where"],
    "tier": "slow"
  },
  "suggestions": [
    "Column `active` in WHERE clause on table `users` has no index. Consider: CREATE INDEX idx_users_active ON users(active);"
  ]
}
```

---

### `mysql_multi_query`

Execute multiple read-only SQL statements in parallel and return all results together. All queries run concurrently and the response arrives in approximately one network round-trip, regardless of how many queries are submitted.

Only read-only statements are accepted (SELECT, SHOW, EXPLAIN).

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `queries` | array of strings | yes | SQL statements to execute in parallel |

**Response fields**

| Field | Type | Description |
|---|---|---|
| `results` | array of objects | One entry per query, in the same order as the input `queries` array |
| `wall_time_ms` | number | Total elapsed wall-clock time for all queries to complete |

Each entry in `results` contains the same fields as a `mysql_query` read response (`sql`, `rows`, `row_count`, `execution_time_ms`, `serialization_time_ms`, `capped`, and optionally `parse_warnings` and `plan`). On error, the entry contains `sql` and `error` instead.

**Example**

```json
{
  "queries": [
    "SELECT COUNT(*) AS total FROM orders",
    "SELECT COUNT(*) AS total FROM users WHERE active = 1"
  ]
}
```

```json
{
  "results": [
    {
      "sql": "SELECT COUNT(*) AS total FROM orders",
      "rows": [{"total": 1042}],
      "row_count": 1,
      "execution_time_ms": 11,
      "serialization_time_ms": 0,
      "capped": false
    },
    {
      "sql": "SELECT COUNT(*) AS total FROM users WHERE active = 1",
      "rows": [{"total": 387}],
      "row_count": 1,
      "execution_time_ms": 8,
      "serialization_time_ms": 0,
      "capped": false
    }
  ],
  "wall_time_ms": 13
}
```

---

## MCP Resources

The server also exposes table schemas as MCP resources:

- `mysql://tables/{schema}/{table}` — column metadata for a specific table
- Listing resources returns all accessible tables with approximate row counts

---

## Performance Guide

### Use `mysql_multi_query` for independent reads

When you need data from multiple unrelated tables, `mysql_multi_query` runs all queries concurrently. Two queries that each take 50 ms will finish in ~50 ms total instead of ~100 ms. The savings grow with the number of queries and network latency to the database.

### `performance_hints` modes

| Value | Behaviour |
|---|---|
| `none` (default) | No EXPLAIN is run. Fastest for queries known to be efficient. |
| `auto` | EXPLAIN runs only when the query exceeds `slow_query_threshold_ms` (default 500 ms). Good for production: zero overhead on fast queries. |
| `always` | EXPLAIN runs after every SELECT. Useful during development or debugging. |

You can also override per-call by passing `"explain": true` to `mysql_query`.

### `max_rows` protects against runaway results

By default, `max_rows = 1000`. When a SELECT has no LIMIT clause, `LIMIT 1000` is appended automatically and `capped: true` is set in the response. This prevents accidentally pulling back millions of rows. Set `max_rows = 0` to disable the cap entirely, or raise it for large exports.

### `readonly_transaction` overhead

By default (`readonly_transaction = false`), SELECT/SHOW/EXPLAIN run as bare `fetch_all` calls — a single round-trip to MySQL. Setting `readonly_transaction = true` wraps every read in `SET TRANSACTION READ ONLY` + `BEGIN` + query + `COMMIT`, adding three extra round-trips. Only enable this if your MySQL user has write privileges and you want extra safety guarantees.

### Connection pool warm-up

`warmup_connections = 1` (default) pre-opens one connection at startup so the first query is not delayed by connection establishment. Increase this if you expect many concurrent queries at startup.

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

### Multi-database mode

When `connection.database` is not set, the server operates in multi-database mode: it lists tables from all non-system databases and accepts fully-qualified names (`SELECT * FROM mydb.users`). Writes in this mode require `multi_db_write_mode = true` (or `MYSQL_MULTI_DB_WRITE_MODE=true`) in addition to the relevant `allow_*` flags.

### SSL

```bash
MYSQL_SSL=true
# For self-signed certs only — do not use in production:
MYSQL_SSL_ACCEPT_INVALID_CERTS=true
```

### HTTP remote mode

```bash
IS_REMOTE_MCP=true
REMOTE_SECRET_KEY=your-secret-token
PORT=3000
./mysql-mcp
```

Remote clients must send `Authorization: Bearer your-secret-token` with every request.

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

| Variable | Description |
|----------|-------------|
| `MYSQL_SSH_HOST` | SSH bastion hostname |
| `MYSQL_SSH_PORT` | SSH port (default: 22) |
| `MYSQL_SSH_USER` | SSH username |
| `MYSQL_SSH_PRIVATE_KEY` | Path to PEM private key |
| `MYSQL_SSH_USE_AGENT` | `true` to use SSH agent |
| `MYSQL_SSH_KNOWN_HOSTS_CHECK` | `strict` / `accept-new` / `insecure` |
| `MYSQL_SSH_KNOWN_HOSTS_FILE` | Override known_hosts path |

### Authentication

- **Key file (recommended for automation):** Set `private_key` to a passphrase-free key path.
- **SSH agent:** Set `use_agent = true` (or `MYSQL_SSH_USE_AGENT=true`). The agent must be running and have the key loaded (`ssh-add /path/to/key`).

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

| Error | Fix |
|-------|-----|
| `'ssh' binary not found` | Install OpenSSH (see Prerequisites above) |
| `Host key verification failed` | Pre-add host with `ssh-keyscan`, or set `known_hosts_check = "accept-new"` |
| `Permission denied (publickey)` | Verify key path, check `ssh-add -l` if using agent |
| `Timed out waiting for local port` | Check SSH connectivity: `ssh user@bastion` interactively |
| Connection resets after idle | Expected — the 10-minute session reaper closes idle sessions |

---

## Development

```bash
# Run unit tests (no MySQL needed)
cargo test

# Build release binary
cargo build --release

# Run with a real MySQL instance
MYSQL_HOST=localhost MYSQL_USER=root cargo test
```

Integration tests use [testcontainers](https://github.com/testcontainers/testcontainers-rs) to spin up a real MySQL instance automatically when Docker is available.

---

## License

MIT
