# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **SSH Tunneling**: Support for connecting to MySQL databases through bastion hosts or jump servers. SSH tunnels are automatically managed with configurable host key verification (`strict`, `accept-new`, `insecure`), private key authentication, and automatic zombie process prevention.
- **SSL/TLS Support**: Full SSL/TLS encryption for database connections with configurable modes:
  - `VerifyIdentity` (SSL enabled with full certificate and hostname validation)
  - `VerifyCa` (SSL enabled with CA certificate chain validation)
  - `Required` (SSL enabled without certificate validation - not recommended for production)
- **Performance Hints**: Automatic query analysis via `EXPLAIN FORMAT=JSON` with three modes:
  - `none`: No automatic analysis
  - `auto`: EXPLAIN runs only when queries exceed the slow query threshold
  - `always`: EXPLAIN runs after every SELECT query
- **Index Suggestions**: When a full table scan is detected, the system provides actionable `CREATE INDEX` suggestions for columns used in WHERE clauses.
- **Named Sessions**: Dynamic runtime database connections via `mysql_connect` tool. Named sessions allow connecting to different MySQL servers or databases during a single session, with automatic cleanup of idle connections after 10 minutes.
- **Per-Schema Permissions**: Fine-grained permission overrides for individual databases. Allows allowing INSERT/UPDATE/DELETE on specific schemas while keeping global defaults restrictive.
- **Connection Pool Management**: Configurable connection pooling with separate `max_sessions` and `max_total_connections` limits to prevent resource exhaustion.
- **Query Result Capping**: Automatic `LIMIT` application with `max_rows` setting (default 1000) to prevent runaway queries from returning excessive data.
- **Memory Limits**: `max_result_memory_mb` configuration to cap result set memory usage, with graceful truncation and warnings.
- **Retry Logic**: Automatic retry with exponential backoff for transient network errors.
- **Schema Introspection Cache**: Configurable TTL-based caching for table metadata, indexes, and foreign keys.
- **Query Timeout Protection**: Per-query timeout configuration to prevent runaway queries from blocking the server.

### Changed

- Enhanced `mysql_query` response to include execution timing, row counts, and automatic pagination hints when results are capped.
- Improved error messages with SSL/TLS troubleshooting guidance.
- Expanded documentation with comprehensive configuration examples and security guidance.

### Security

- Added validation to prevent combining `allow_runtime_connections=true` with `ssh.known_hosts_check=insecure`, which could enable man-in-the-middle attacks on SSH tunnels.
- Added SSH private key file permission validation (requires mode 0600 or 0400 on Unix systems).
- Added SSH known_hosts file permission validation for strict mode.
- SSH hostnames are validated to prevent command injection and null byte attacks.

### Documentation

- Added comprehensive SSL/TLS troubleshooting section to README.
- Added CONTRIBUTING.md with development guidelines.
- Documented all `mysql_connect` SSL parameters and behavior.
- Added module-level documentation to all major source files.

## [0.1.1] - 2025-03-25

### Fixed

- Edge cases and overengineering simplifications (code cleanup and simplification).

## [0.1.0] - 2025-03-XX

### Added

- Initial release of mysql-mcp.
- MCP protocol support via `rmcp` library.
- Core MySQL connection pooling via `sqlx`.
- Basic MCP tools:
  - `mysql_query`: Execute SQL statements with configurable permissions
  - `mysql_schema_info`: Get table metadata, indexes, and foreign keys
  - `mysql_server_info`: Get MySQL server metadata
  - `mysql_explain_plan`: Get query execution plans
  - `mysql_list_tables`: List tables in a database
- SQL parser for statement classification (SELECT, INSERT, UPDATE, DELETE, DDL).
- Permission system for write operations (INSERT, UPDATE, DELETE, DDL opt-in).
- Environment variable and TOML configuration support.
- Connection string support (`mysql://` URLs).
- Unix socket support.
- Basic CI/CD with GitHub Actions.

[unreleased]: https://github.com/cecil-the-coder/mysql-mcp/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/cecil-the-coder/mysql-mcp/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/cecil-the-coder/mysql-mcp/releases/tag/v0.1.0
