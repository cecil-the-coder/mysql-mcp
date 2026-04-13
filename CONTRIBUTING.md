# Contributing to mysql-mcp

Thank you for your interest in contributing! This guide covers everything you need to get started.

## Prerequisites

- **Rust** — stable toolchain (install via [rustup](https://rustup.rs))
- **Docker** — required for integration/e2e tests that use [testcontainers](https://github.com/testcontainers/testcontainers-rs) to spin up ephemeral MySQL instances
- **OpenSSH** — needed if you want to run SSH tunnel tests locally (pre-installed on macOS and most Linux distros)
- **Git** — version control

## Getting Started

```bash
# Clone the repository
git clone https://github.com/cecil-the-coder/mysql-mcp.git
cd mysql-mcp

# Verify everything compiles
cargo check

# Install pre-commit hooks (runs fmt, clippy, and tests before each commit)
make install-hooks
```

## Pre-commit Hooks

The project uses [pre-commit](https://pre-commit.com) to enforce code quality before changes land in CI. The hooks (defined in `.pre-commit-config.yaml`) run:

- **cargo fmt** — ensures consistent formatting
- **cargo clippy — -D warnings** — catches common mistakes and enforces lints (warnings are treated as errors)
- **cargo test** — runs the full test suite

### Setup

```bash
# Install pre-commit (if not already installed)
pip install pre-commit
# Or via Homebrew:
# brew install pre-commit

# Install the hooks
make install-hooks
```

Hooks run automatically on `git commit`. If a hook fails, the commit is aborted — fix the issue and try again. You can skip hooks temporarily with `git commit --no-verify`, but CI will still enforce all checks.

## Building

```bash
# Debug build (fast compilation, useful during development)
cargo build

# Release build (optimised binary)
cargo build --release

# Quick type-check without full compilation
cargo check
```

The release binary is output to `target/release/mysql-mcp`.

## Running Tests

### Unit Tests

Unit tests do not require a running MySQL instance. They run purely in-process:

```bash
cargo test --lib
```

### Integration Tests (testcontainers)

Integration tests use [testcontainers](https://github.com/testcontainers/testcontainers-rs) to automatically spin up a MySQL 8.1 Docker container. **Docker must be running.**

```bash
# Run all tests (unit + integration via testcontainers)
cargo test
```

When `MYSQL_HOST` is **not** set, tests that need a database will automatically start a throwaway MySQL container. The container is torn down when the test completes.

### Integration Tests (external MySQL)

You can also run integration tests against an existing MySQL instance:

```bash
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASS=mypassword
export MYSQL_DB=testdb
cargo test
```

When `MYSQL_HOST` is set, tests connect to that instance instead of starting a container.

### End-to-End Tests

E2E tests spawn the compiled `mysql-mcp` binary as a subprocess and exercise the full MCP protocol (JSON-RPC handshake, tool calls, etc.). They require the binary to be built first:

```bash
cargo build
cargo test
```

E2E tests are automatically included in `cargo test` when the binary exists. They will be skipped gracefully if the binary is not found or if no MySQL instance (or Docker) is available.

### Benchmarks

The project includes criterion benchmarks for SQL parsing, EXPLAIN parsing, permission checks, and query execution:

```bash
cargo bench
```

Benchmark results are written to `target/criterion/`.

### Useful Test Flags

```bash
# Show test output (println, eprintln) even on success
cargo test -- --nocapture

# Run a specific test by name (substring match)
cargo test test_mcp_initialize_handshake

# Run only unit tests (skip integration/e2e)
cargo test --lib

# Run only benchmarks (without measuring)
cargo bench -- --quick
```

## Code Quality

CI enforces three checks that must pass before a PR can be merged:

| Check | Command | What it does |
|-------|---------|--------------|
| Format | `cargo fmt --check` | Ensures code follows Rust style conventions |
| Lint | `cargo clippy -- -D warnings` | Catches bugs, idioms, and style issues (warnings = errors) |
| Tests | `cargo test` | Runs the full test suite including integration tests |

Run these locally before pushing:

```bash
make fmt      # auto-format code
make clippy   # lint
make test     # run tests
make check    # quick type-check
```

Or use the `Makefile` shortcut to run everything:

```bash
make fmt && make clippy && make test
```

## Project Structure

```
src/
├── main.rs              # Binary entry point (Tokio runtime + MCP server setup)
├── lib.rs               # Library surface for tests and benchmarks
├── config/              # Configuration loading (TOML, env vars, defaults)
│   ├── mod.rs
│   ├── env_config.rs
│   └── tests.rs
├── db.rs                # MySQL connection pooling (sqlx)
├── server/              # MCP tool handlers and session management
│   ├── mod.rs
│   ├── handlers.rs      # Tool implementations (mysql_query, mysql_schema_info, etc.)
│   ├── sessions.rs      # Named session lifecycle
│   ├── tool_schemas.rs  # JSON Schema definitions for MCP tools
│   └── error.rs         # Error types for MCP responses
├── query/               # SQL execution engine
│   ├── mod.rs
│   ├── read.rs          # SELECT/SHOW execution
│   ├── write.rs         # INSERT/UPDATE/DELETE/DDL execution
│   ├── explain.rs       # EXPLAIN plan generation
│   ├── explain_parse.rs # EXPLAIN output parser
│   └── retry.rs         # Transient error retry logic
├── schema/              # Schema introspection and caching
│   ├── mod.rs
│   ├── fetch.rs         # information_schema queries
│   ├── introspect.rs    # Table metadata assembly
│   └── tests.rs
├── sql_parser/          # SQL statement classification
│   ├── mod.rs
│   ├── classify.rs      # Statement type detection (SELECT, INSERT, DDL, etc.)
│   └── tests.rs
├── permissions.rs       # Permission enforcement (global + per-schema)
├── tunnel.rs            # SSH tunnel management
├── test_helpers.rs      # Shared test utilities (testcontainers setup, pool helpers)
├── e2e_tests.rs         # End-to-end MCP protocol tests
├── e2e_session_tests.rs # E2E tests for named sessions
├── e2e_ssh_tests.rs     # E2E tests for SSH tunneling
├── e2e_test_utils.rs    # Shared E2E helpers (spawn server, JSON-RPC I/O)
├── perf_tests.rs        # Performance integration tests
└── perf_write_tests.rs  # Write-operation performance tests
```

## Security Model

Understanding the security model is important when contributing changes that affect query handling or permissions.

### Read-by-default

Only read-only statements (SELECT, SHOW, EXPLAIN, DESCRIBE) are allowed by default. All write operations (INSERT, UPDATE, DELETE) and DDL (CREATE, ALTER, DROP, TRUNCATE) require explicit opt-in via configuration flags (`allow_insert`, `allow_update`, `allow_delete`, `allow_ddl`).

This design ensures that a misconfigured or overly-permissive LLM cannot accidentally destroy data. When contributing new SQL statement handling, always route through the permission check in `permissions.rs`.

### Per-schema overrides

Write permissions can be overridden per-database via `schema_permissions`. This allows, for example, writes to a staging database while keeping production read-only. When modifying permission logic, ensure both global and per-schema paths are covered.

### Row limits

`max_rows` (default: 1000) caps result sets to prevent runaway queries from consuming excessive memory. This is enforced at the query execution layer, not the parser. Any new query execution paths must respect this limit.

### Connection security

- SSL is opt-in (`security.ssl`)
- SSH tunneling uses subprocess-based OpenSSH (not a Rust SSH library) for maximum compatibility
- Runtime connections (`mysql_connect`) are disabled by default to prevent credential injection

## Pull Request Guidelines

1. **Fork and branch** — Create a feature branch from `main`. Use a descriptive name (e.g., `feat/ssh-reconnect`, `fix/pool-timeout`).

2. **One concern per PR** — Keep changes focused. Mixed refactor + feature PRs are harder to review.

3. **Ensure CI passes** — All three CI jobs (fmt, clippy, test) must pass. Run them locally first:
   ```bash
   cargo fmt --check
   cargo clippy -- -D warnings
   cargo test
   ```

4. **Add tests** — New features and bug fixes should include tests. Integration tests go in the appropriate `src/*.rs` file (tests are co-located with code using `#[cfg(test)]` modules or in the `e2e_*.rs` / `perf_*.rs` files).

5. **Update documentation** — If your change affects user-facing behavior (new config options, changed tool parameters, new features), update `README.md` and the example config `mysql-mcp.example.toml` accordingly.

6. **Write clear commit messages** — Summarise what and why, not just how.

7. **Keep the PR description current** — Describe the motivation, approach, and any trade-offs.

## Adding a New MCP Tool

If you're adding a new tool exposed over MCP:

1. Define the tool schema in `src/server/tool_schemas.rs`
2. Implement the handler in `src/server/handlers.rs`
3. Register the tool in the server setup (`src/server/mod.rs`)
4. Add permission checks in `src/permissions.rs` if the tool modifies data
5. Add E2E tests in a new `src/e2e_*.rs` file or extend an existing one
6. Document the tool in `README.md` under "MCP Tools"

## Adding a New Configuration Option

1. Add the field to the config struct in `src/config/mod.rs`
2. Load it from environment variables in `src/config/env_config.rs`
3. Add a TOML example and comment in `mysql-mcp.example.toml`
4. Document it in the Configuration Reference table in `README.md`
5. Add a test in `src/config/tests.rs`

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
