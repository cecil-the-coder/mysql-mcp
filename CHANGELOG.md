# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Added CONTRIBUTING.md with development guidelines
- Added SSL/TLS troubleshooting section to README
- Added module-level documentation to multiple source files (query, server, sql_parser modules)
- Added environment variable precedence clarification to README
- Added missing `explain` parameter to `mysql_query` tool schema documentation
- Added SSL mode documentation to `mysql_connect` tool schema
- Added MCP server instructions for better LLM guidance
- Added missing configuration options to example TOML file (`allow_runtime_connections`, `max_total_connections`, pool config options)

### Security
- Block dangerous SSH insecure mode when used with runtime connections (`mysql_connect`)
- Add SSH hostname length validation to prevent truncation
- Improve SSL/TLS certificate validation documentation

### Fixed
- Remove non-existent `[monitoring]` section from example TOML file

## [0.1.1] - 2024-03-28

### Added
- Add `mysql_list_tables` tool for easier table discovery
- Add `mysql_explain_plan` tool for standalone query analysis
- Add `mysql_server_info` tool for server metadata
- Add `mysql_list_sessions` tool for session management visibility
- Add `mysql_disconnect` tool for explicit session cleanup
- Add SSH tunneling support via bastion/jump hosts (subprocess-based OpenSSH with key file authentication)
- Add SSL/TLS support with certificate verification options
- Add Unix socket connection support
- Add per-schema permission overrides for fine-grained write control
- Add connection pooling with configurable size and timeouts
- Add query retry logic for transient network errors
- Add automatic EXPLAIN with index suggestions on slow queries
- Add row capping with automatic LIMIT injection to prevent runaway results
- Add session reaper for idle connection cleanup
- Add comprehensive test suite with testcontainers integration
- Add benchmarking suite for performance monitoring

### Security
- Implement permission system for write operations (INSERT, UPDATE, DELETE, DDL)
- Add input validation for SQL statements (reject NUL bytes, validate table names)
- Add DNS resolution validation to prevent SSRF attacks
- Add SSH known_hosts verification with multiple modes (strict, accept-new, insecure)
- Redact passwords in Debug output
- Block dangerous SQL patterns (LOAD DATA INFILE, SELECT INTO OUTFILE)
- Implement schema-level permission overrides via environment variables and TOML config

### Fixed
- Fix potential deadlock in session management
- Fix connection leak in session cleanup
- Fix edge cases in retry logic for transient errors
- Fix EXPLAIN SQL mismatch issues
- Fix UTF-8 safe slicing in SQL truncation
- Fix BIGINT precision handling
- Fix NULL decoding for all MySQL type arms
- Fix reaper TOCTOU race condition
- Fix integer overflow in LIMIT injection
- Fix tunnel process cleanup and resource leaks
- Fix cache invalidation for schema metadata
- Fix DNS validation to allow localhost resolution
- Fix session lock scope issues
- Fix composite index detection in suggestions

### Changed
- Simplified overengineered configuration system
- Simplified schema cache implementation
- Simplified permissions system
- Improved error messages for permission denials
- Improved logging throughout the codebase
- Improved SSH tunnel diagnostics and error handling
- Improved EXPLAIN parsing and tier classification
- Improved test coverage for security-critical functions

## [0.1.0] - 2024-03-20

### Added
- Initial release of mysql-mcp
- MySQL MCP server exposing databases via Model Context Protocol
- Support for read queries (SELECT, SHOW, EXPLAIN, DESCRIBE)
- Basic connection management with sqlx
- TOML configuration file support
- Environment variable configuration support
- Schema introspection with table metadata and indexes
- Query performance hints based on EXPLAIN output
- Named sessions for connecting to multiple databases
- `mysql_query` tool for SQL execution
- `mysql_schema_info` tool for table metadata
- `mysql_connect` tool for runtime connection creation

[Unreleased]: https://github.com/cecil-the-coder/mysql-mcp/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/cecil-the-coder/mysql-mcp/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/cecil-the-coder/mysql-mcp/releases/tag/v0.1.0
