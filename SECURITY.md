# Security Guide

This document provides comprehensive security guidance for deploying and operating mysql-mcp in production environments.

## Overview

mysql-mcp provides several security mechanisms to protect your database connections and control access. This guide covers best practices for SSL configuration, SSH tunneling security, and detailed explanations of security settings.

## Security Best Practices

### Principle of Least Privilege

By default, mysql-mcp only allows read-only operations (SELECT, SHOW, EXPLAIN). Write operations (INSERT, UPDATE, DELETE, DDL) must be explicitly enabled:

```toml
[security]
allow_insert = false  # Disable by default
allow_update = false  # Disable by default
allow_delete = false  # Disable by default
allow_ddl = false     # Disable by default
```

**Never enable write permissions unless absolutely necessary.** When enabled, ensure they are scoped to specific schemas using per-schema permission overrides.

### Per-Schema Permission Overrides

Instead of globally enabling write operations, use per-schema permissions to limit write access to specific databases:

```toml
[security.schema_permissions.app_db]
allow_insert = true
allow_update = true
# allow_delete = false  # Explicitly disabled
# allow_ddl = false     # Explicitly disabled

[security.schema_permissions.production_db]
# No write permissions for production
# Inherits global defaults (false)
```

Environment variable equivalents:
```bash
MYSQL_SCHEMA_APP_DB_PERMISSIONS=insert,update
MYSQL_SCHEMA_PRODUCTION_DB_PERMISSIONS=  # No permissions
```

### Connection Pool Limits

Limit the maximum number of connections and rows returned to prevent resource exhaustion:

```toml
[pool]
size = 20              # Limit concurrent connections
max_rows = 1000        # Prevent runaway result sets
max_total_connections = 100  # Total connections across all sessions
```

**Note:** The `max_rows` setting automatically appends `LIMIT 1000` to queries without a LIMIT clause, preventing accidental full table scans.

### Runtime Connections

Disable runtime session creation unless absolutely required:

```toml
[security]
allow_runtime_connections = false  # Default: false
```

Runtime connections (`mysql_connect` tool) require this to be enabled. When enabled, they bypass many security checks and should only be used in controlled environments.

## SSL/TLS Configuration

### Basic SSL Setup

Enable SSL/TLS encryption for all connections:

```toml
[security]
ssl = true
```

**Important:** SSL should be enabled in production environments to protect data in transit.

### Certificate Verification

#### VerifyCa Mode (Recommended)

Verify the server's certificate chain against a trusted CA:

```toml
[security]
ssl = true
ssl_ca = "/path/to/ca.pem"
```

This mode validates the certificate chain without hostname verification. Use when you have a proper CA infrastructure.

#### Accept Invalid Certificates (Development Only)

**Never use this in production:**

```toml
[security]
ssl = true
ssl_accept_invalid_certs = true  # INSECURE - development only
```

This bypasses all certificate validation and makes connections vulnerable to MITM attacks.

### SSL/TLS Troubleshooting

| Error | Diagnosis | Solution |
|-------|-----------|----------|
| `unable to get local issuer certificate` | CA certificate not found or invalid | Set `MYSQL_SSL_CA` to correct CA bundle path |
| `certificate verify failed` | Server certificate doesn't match CA | Verify CA is correct; use `accept_invalid_certs` only for testing |
| `SSL connection error: unknown error number` | TLS version mismatch | Ensure server supports TLS 1.2+; check `ssl_ca` file |
| `SSL CA file not found` | Path incorrect or file unreadable | Use absolute path; verify file permissions (`chmod 600`) |
| `Bad handshake` | Protocol incompatibility | Check client/server TLS compatibility |

### Self-Signed Certificates (Development Only)

For development with self-signed certificates:

```toml
[security]
ssl = true
ssl_accept_invalid_certs = true  # Only for trusted dev environments
```

**Security Warning:** This exposes connections to MITM attacks. Use only in isolated development environments.

### Certificate File Permissions

Ensure CA certificate files have strict permissions:

```bash
chmod 600 /path/to/ca.pem
chown $(whoami) /path/to/ca.pem
```

Files should **not** be group or world-readable.

## SSH Tunneling Security

SSH tunneling provides an additional layer of security by routing connections through a bastion host. However, it introduces its own security considerations.

### Configuration

```toml
[connection]
host = "db.internal"      # Database host (as seen from bastion)
port = 3306
user = "dbuser"
(password omitted for brevity)

[ssh]
host = "bastion.example.com"
user = "ubuntu"
private_key = "/home/user/.ssh/id_rsa"
known_hosts_check = "strict"
```

### Host Key Verification

The `known_hosts_check` setting controls SSH host key verification:

| Setting | Security Level | Description |
|---------|---------------|-------------|
| `strict` (default) | Highest | Only connects to hosts with verified keys. Most secure. |
| `accept-new` | Medium | Accepts new keys automatically on first connection. Vulnerable to first-connection MITM. |
| `insecure` | Lowest | Skips all verification. **Vulnerable to MITM attacks.** Only for isolated test environments. |

### Pre-Populating Known Hosts

For strict mode, pre-populate your known_hosts file:

```bash
ssh-keyscan -H bastion.example.com >> ~/.ssh/known_hosts
```

Verify permissions:
```bash
chmod 600 ~/.ssh/known_hosts
```

### Private Key Security

**Private key files must have strict permissions:**

```bash
chmod 600 /path/to/private_key
```

**Never use world-readable or group-readable private keys.**

### SSH Agent (Recommended for Encrypted Keys)

For encrypted private keys, use SSH agent instead of storing keys on disk:

```bash
ssh-agent bash
ssh-add /path/to/encrypted_key
mysql-mcp  # Agent will provide the key
```

### Dynamic Tunnels via Runtime Connections

SSH parameters can also be passed to `mysql_connect` for on-demand tunnels:

```json
{
  "name": "mysql_connect",
  "arguments": {
    "name": "tunneled-session",
    "host": "db.internal",
    "ssh_host": "bastion.example.com",
    "ssh_user": "ubuntu",
    "ssh_private_key": "/home/user/.ssh/id_rsa",
    "ssh_known_hosts_check": "strict"
  }
}
```

**Requires:** `MYSQL_ALLOW_RUNTIME_CONNECTIONS=true`

### SSH Tunnel Security Checklist

Before using SSH tunneling:

1. ✅ Private key file permissions are `600`
2. ✅ `known_hosts` contains the bastion's verified key (for strict mode)
3. ✅ SSH agent is running and has the key loaded (for encrypted keys)
4. ✅ Bastion host is reachable via standard SSH
5. ✅ Database is accessible from bastion host only (not directly exposed)

## Authentication and Access Control

### Session Management

mysql-mcp supports named sessions for connecting to multiple databases:

```toml
[security]
max_sessions = 50              # Maximum named sessions
max_total_connections = 100    # Total connections across all sessions
```

Named sessions idle for more than 10 minutes are automatically closed.

### Session Isolation

Each named session has its own connection pool, providing isolation between different database connections. This prevents cross-session interference and allows different permission sets.

## Development vs Production Security

### Development Configuration

The examples directory contains development configurations with relaxed security:

```toml
# examples/development.toml
[security]
allow_insert = true
allow_update = true
ssl = false
```

**Never deploy development configurations to production.**

### Production Hardening Checklist

- [ ] SSL enabled with proper CA certificates
- [ ] Write permissions disabled (or strictly scoped)
- [ ] `allow_runtime_connections = false`
- [ ] Connection pool limits set appropriately
- [ ] Per-schema permissions configured
- [ ] SSH tunneling with strict host key verification
- [ ] Private key file permissions set to `600`
- [ ] Regular certificate rotation
- [ ] Monitoring and logging enabled

## Monitoring and Auditing

### Enable Query Logging

Track all queries for audit purposes:

```toml
[security]
# Log all queries with user context
# (Implementation depends on your logging infrastructure)
```

### Connection Monitoring

Monitor active sessions and connection counts:

```bash
# List active sessions
mysql-mcp mysql_list_sessions

# Monitor connection pool metrics
# (via your monitoring system's MySQL metrics)
```

## Emergency Procedures

### Revoking Compromised Credentials

1. Immediately revoke the compromised MySQL user's privileges
2. Rotate all passwords and SSH keys
3. Review connection logs for unauthorized access
4. Reconfigure mysql-mcp with new credentials

### Locking Down Write Access

If write access was accidentally enabled:

```toml
[security]
allow_insert = false
allow_update = false
allow_delete = false
allow_ddl = false
```

## References

- [MySQL SSL/TLS Configuration](https://dev.mysql.com/doc/refman/8.0/en/ssl-connections.html)
- [OpenSSH Security Best Practices](https://www.openssh.com/manual.html)
- [Connection Pooling Best Practices](https://github.com/cecil-the-coder/mysql-mcp#pool)

## Support

For security-related issues or questions, please review the [TROUBLESHOOTING.md](TROUBLESHOOTING.md) guide or consult your security team.