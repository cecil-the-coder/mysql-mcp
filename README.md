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

### SSH Tunnel Example

For databases behind a bastion host, you can configure SSH tunneling directly in the TOML file:

```toml
[connection]
host = "db.internal"
port = 3306
user = "dbuser"
password = "secret"
database = "mydatabase"

[ssh]
host = "bastion.example.com"
user = "ubuntu"
private_key = "/home/user/.ssh/id_rsa"
known_hosts_check = "strict"

[pool]
size = 20
max_rows = 1000

[security]
allow_insert = false
allow_update = false
```

All SSH-related options are:
- `ssh.host` — SSH bastion hostname (required)
- `ssh.port` — SSH server port (default: 22)
- `ssh.user` — SSH username (required when host is set)
- `ssh.private_key` — Path to PEM private key file
- `ssh.known_hosts_check` — Host key verification: `strict`, `accept-new`, or `insecure` (default: `strict`)
- `ssh.known_hosts_file` — Override path to known_hosts file

Configuration is loaded in this order (highest priority wins):
1. Environment variables (highest)
2. TOML config file (`mysql-mcp.toml` or `$MCP_CONFIG_FILE`)
3. `.env` file
4. Built-in defaults (lowest)

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