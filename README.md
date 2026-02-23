# mysql-mcp

A MySQL MCP (Model Context Protocol) server written in Rust. It exposes a MySQL database to LLM assistants via the MCP standard, allowing them to run SQL queries, inspect table schemas, obtain query execution plans, and receive performance warnings — all through a simple tool interface.

## Features

- **Read queries**: SELECT, SHOW, EXPLAIN — always allowed
- **Write operations**: INSERT, UPDATE, DELETE — opt-in via config
- **DDL operations**: CREATE, ALTER, DROP, TRUNCATE — opt-in
- **Performance hints**: automatic EXPLAIN + index suggestions on slow queries
- **Named sessions**: connect to additional databases at runtime via `mysql_connect`
- **SSH tunneling**: reach databases behind a bastion/jump host
- **Per-schema permissions**: fine-grained write control per database
- **Connection pooling**: configurable pool size, timeouts, and warm-up
- **SSL support**: encrypted connections with optional CA verification
- **Unix socket support**: connect via socket path instead of host:port

## Quick Start

### Installation

```bash
cargo install --git https://github.com/cecil-the-coder/mysql-mcp
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
| `connection.connection_string` | `MYSQL_CONNECTION_STRING` | string | — | Full `mysql://user:pass@host/db` URL; overrides all other connection fields |

### Pool

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `pool.size` | `MYSQL_POOL_SIZE` | u32 | `20` | Maximum number of pooled connections |
| `pool.query_timeout_ms` | `MYSQL_QUERY_TIMEOUT` | u64 | `30000` | Per-query timeout in milliseconds |
| `pool.connect_timeout_ms` | `MYSQL_CONNECT_TIMEOUT` | u64 | `10000` | Connection establishment timeout in milliseconds; also serves as the acquire timeout |
| `pool.readonly_transaction` | `MYSQL_READONLY_TRANSACTION` | bool | `false` | Wrap every SELECT in `SET TRANSACTION READ ONLY` + `BEGIN` + `COMMIT` (4-RTT). Leave `false` for bare 1-RTT fetches |
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
| `security.ssl_accept_invalid_certs` | `MYSQL_SSL_ACCEPT_INVALID_CERTS` | bool | `false` | Skip certificate validation (not for production) |
| `security.ssl_ca` | `MYSQL_SSL_CA` | string | — | Path to PEM CA certificate file |
| `security.schema_permissions` | `MYSQL_SCHEMA_<NAME>_PERMISSIONS` | map | `{}` | Per-schema write permission overrides (see below) |
| `security.multi_db_write_mode` | `MYSQL_MULTI_DB_WRITE_MODE` | bool | `false` | Allow writes when no default database is set (multi-DB mode) |
| `security.allow_runtime_connections` | `MYSQL_ALLOW_RUNTIME_CONNECTIONS` | bool | `false` | Allow `mysql_connect` to accept raw credentials at runtime |
| `security.max_sessions` | `MYSQL_MAX_SESSIONS` | u32 | `50` | Maximum number of concurrent named sessions |

### SSH Tunnel

| TOML key | Environment variable | Type | Default | Description |
|---|---|---|---|---|
| `ssh.host` | `MYSQL_SSH_HOST` | string | — | SSH bastion hostname |
| `ssh.port` | `MYSQL_SSH_PORT` | u16 | `22` | SSH server port |
| `ssh.user` | `MYSQL_SSH_USER` | string | — | SSH username |
| `ssh.private_key` | `MYSQL_SSH_PRIVATE_KEY` | string | — | Path to PEM private key file |
| `ssh.private_key_passphrase` | `MYSQL_SSH_PRIVATE_KEY_PASSPHRASE` | string | — | Passphrase for the private key (if encrypted) |
| `ssh.known_hosts_check` | `MYSQL_SSH_KNOWN_HOSTS_CHECK` | string | `strict` | Host key verification: `strict`, `accept-new`, or `insecure` |
| `ssh.known_hosts_file` | `MYSQL_SSH_KNOWN_HOSTS_FILE` | string | — | Override path to known_hosts file |

---

## MCP Tools

### `mysql_query`

Execute a SQL statement.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `sql` | string | yes | The SQL statement to execute |
| `explain` | boolean | no | Force an EXPLAIN run for this query, overriding the `performance_hints` setting |
| `session` | string | no | Named session to route this query to (omit for default connection) |

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
| `parse_warnings` | array of strings | only when non-empty | Static analysis warnings (e.g. `SELECT *`, missing WHERE on UPDATE) |
| `capped` | boolean | only when `true` | Present when `max_rows` was applied and the result was truncated |
| `next_offset` | number | only when capped | Offset to pass for the next page |
| `capped_hint` | string | only when capped | Suggested query with LIMIT/OFFSET to fetch the next page |
| `plan` | object | only when EXPLAIN ran | Query execution plan (see below) |
| `suggestions` | array of strings | only when applicable | Index suggestions when a full table scan is detected |
| `explain_error` | string | only when applicable | Error message when the automatic EXPLAIN run failed |

**`plan` object fields**

| Field | Type | Description |
|---|---|---|
| `full_table_scan` | boolean | Whether MySQL performed a full table scan |
| `index_used` | string or null | Name of the index used, or null if none |
| `rows_examined_estimate` | number | MySQL's estimate of rows examined |
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

With `performance_hints=always` (or `explain: true`):

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
    "extra_flags": ["Using where"],
    "tier": "slow"
  },
  "suggestions": [
    "Column `active` in WHERE clause on table `users` has no index. Consider: CREATE INDEX idx_users_active ON users(active);"
  ]
}
```

---

### `mysql_schema_info`

Get schema metadata for a table.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `table` | string | yes | Table name |
| `database` | string | no | Database/schema name (uses connected database if omitted) |
| `include` | array of strings | no | Additional metadata: `"indexes"`, `"foreign_keys"`, `"size"` (any combination) |
| `session` | string | no | Named session to use |

**Response**

Always includes `columns` (name, type, nullable). Optional sections:
- `include: ["indexes"]` — all indexes with column lists, uniqueness, and primary key flag
- `include: ["foreign_keys"]` — FK constraints with referenced table and columns
- `include: ["size"]` — estimated row count, data size, and index size in bytes

---

### `mysql_server_info`

Get MySQL server metadata: version, current database, current user, sql_mode, character set, collation, time zone, read-only flag, and which write operations are enabled.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `session` | string | no | Named session to use |


**Response**

Returns a JSON object with:
- `mysql_version`, `current_user`, `current_database`, `sql_mode`, `character_set`, `collation`, `time_zone`, `read_only`
- `accessible_features` — array of statement types enabled for this connection (always includes `SELECT`, `SHOW`, `EXPLAIN`; includes `INSERT`, `UPDATE`, `DELETE`, `DDL (CREATE/ALTER/DROP)` when the corresponding permission is enabled)

---

### `mysql_explain_plan`

Get the execution plan for a SELECT query without running it. Returns index usage, rows examined estimate, and optimization tier.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `sql` | string | yes | The SELECT statement to explain (must be a single SELECT) |
| `session` | string | no | Named session to use |

**Response**

Returns the same `plan` object as described for `mysql_query` above.

---

### `mysql_connect`

Create a named session to a different MySQL server or database. Requires `MYSQL_ALLOW_RUNTIME_CONNECTIONS=true`.

**Parameters**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Session identifier (alphanumeric, underscore, hyphen; max 64 chars). `"default"` is reserved. |
| `host` | string | yes | MySQL host |
| `user` | string | yes | MySQL username |
| `port` | integer | no | MySQL port (default: 3306) |
| `password` | string | no | MySQL password |
| `database` | string | no | Default database for the session |
| `ssl` | boolean | no | Enable SSL/TLS |
| `ssl_ca` | string | no | Path to PEM CA certificate file |
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

List all active named sessions with host, database, and idle time. The default session is omitted when it is the only active session.

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
MYSQL_SSL_CA=/path/to/ca.pem         # VerifyCa mode (validates cert chain)
# For self-signed certs only — do not use in production:
MYSQL_SSL_ACCEPT_INVALID_CERTS=true
```

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

- **Key file (recommended for automation):** Set `private_key` to the key path. Use `private_key_passphrase` (or `MYSQL_SSH_PRIVATE_KEY_PASSPHRASE`) if the key is encrypted.
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
# Run unit tests (no MySQL needed — uses testcontainers/Docker automatically)
cargo test

# Build release binary
cargo build --release

# Run against a real MySQL instance
MYSQL_HOST=localhost MYSQL_USER=root MYSQL_DB=mydb cargo test
```

Integration tests use [testcontainers](https://github.com/testcontainers/testcontainers-rs) to spin up a real MySQL instance automatically when Docker is available and `MYSQL_HOST` is not set.

---

## License

MIT
