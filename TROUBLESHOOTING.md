# Troubleshooting Guide

This guide covers common issues and their solutions when running mysql-mcp.

## Table of Contents

- [Connection Pool Exhaustion](#connection-pool-exhaustion)
- [Query Timeouts](#query-timeouts)
- [Permission Denied Errors](#permission-denied-errors)
- [Performance Issues](#performance-issues)
- [SSL/TLS Issues](#ssltls-issues)
- [SSH Tunneling Issues](#ssh-tunneling-issues)
- [Container/Docker Networking](#containerdocker-networking)
- [Windows-Specific Issues](#windows-specific-issues)

---

## Connection Pool Exhaustion

### Symptoms

```
error: pool timed out while waiting for an open connection
timed out waiting for a connection from the pool
```

Queries fail intermittently under load, succeed when retrying immediately.

### Causes

1. **Pool size too small** for concurrent query load
2. **Long-running queries** holding connections
3. **Connection leaks** (rare in mysql-mcp, but possible with long-running sessions)
4. **Too many named sessions** competing for the global connection limit

### Diagnosis

Check your current pool and connection settings:

```toml
[pool]
size = 20                      # Default pool size

[security]
max_sessions = 50              # Named sessions allowed
max_total_connections = 100    # Global limit across all pools
```

### Fixes

**1. Increase pool size** (if you have headroom on MySQL `max_connections`):

```toml
[pool]
size = 50
```

Or via environment:
```bash
MYSQL_POOL_SIZE=50
```

**2. Reduce query timeout** to free connections faster from slow queries:

```toml
[pool]
query_timeout_ms = 10000       # 10 seconds instead of 30
```

**3. Limit concurrent named sessions**:

```toml
[security]
max_sessions = 10              # Reduce if you don't need many
max_total_connections = 50     # Cap total connections
```

**4. Close idle sessions explicitly**:

Use `mysql_disconnect` to close sessions you no longer need, rather than waiting for the 10-minute idle timeout.

**5. Enable connection retry** (already on by default):

```toml
[pool]
retry_attempts = 3             # Already defaults to 2
```

---

## Query Timeouts

### Symptoms

```
Query error: operation timed out
Query canceled after 30000ms
timed out while waiting for query results
```

### Diagnosis Steps

**1. Check which timeout is being hit:**

| Timeout Type | Config Key | Default | Error Pattern |
|-------------|-----------|---------|---------------|
| Query execution | `pool.query_timeout_ms` | 30000ms | "operation timed out" |
| Connection acquire | `pool.connect_timeout_ms` | 10000ms | "pool timed out" |

**2. Test query manually:**

```sql
-- Connect directly to MySQL and run:
SELECT SLEEP(31);  -- Will timeout if query_timeout_ms=30000
```

**3. Check for locked tables:**

```sql
-- Check for long-running transactions
SHOW PROCESSLIST;
SELECT * FROM information_schema.INNODB_TRX;
```

**4. Review slow query log** on MySQL server:

```sql
-- On MySQL server
SHOW VARIABLES LIKE 'slow_query%';
SHOW VARIABLES LIKE 'long_query_time';
```

### Fixes

**1. Increase query timeout for legitimate long queries:**

```toml
[pool]
query_timeout_ms = 120000      # 2 minutes for heavy analytics
```

**2. Add indexes** for slow queries (see [Performance Issues](#performance-issues))

**3. Break large queries into chunks** using `LIMIT` and `OFFSET`:

```sql
-- Instead of:
SELECT * FROM large_table WHERE created_at < '2023-01-01';

-- Use:
SELECT * FROM large_table 
WHERE created_at < '2023-01-01' 
LIMIT 1000 OFFSET 0;  -- Then 1000 OFFSET 1000, etc.
```

**4. Set appropriate `max_rows`** to prevent accidental full table scans:

```toml
[pool]
max_rows = 1000                # Truncate huge results
```

---

## Permission Denied Errors

### Symptoms

```
INSERT operation denied on 'mydb': INSERT operations are not allowed. Set MYSQL_ALLOW_INSERT=true
UPDATE operation denied on 'mydb': UPDATE operations are not allowed. Set MYSQL_ALLOW_UPDATE=true
DDL operation denied: DDL (CREATE) operations are not allowed. Set MYSQL_ALLOW_DDL=true
```

Or MySQL-level errors:

```
Access denied for user 'myuser'@'%' to database 'mydb'
SELECT command denied to user 'myuser'@'%' for table 'sensitive_table'
```

### Diagnosis Steps

**1. Check mysql-mcp security settings:**

```toml
[security]
allow_insert = false
allow_update = false
allow_delete = false
allow_ddl = false
```

Or environment variables:
```bash
echo $MYSQL_ALLOW_INSERT $MYSQL_ALLOW_UPDATE $MYSQL_ALLOW_DELETE $MYSQL_ALLOW_DDL
```

**2. Check per-schema permissions** (if using overrides):

```toml
[security.schema_permissions.mydb]
allow_insert = true
allow_update = false
```

**3. Verify MySQL user permissions** directly:

```sql
-- Connect to MySQL with the same credentials mysql-mcp uses
SHOW GRANTS FOR CURRENT_USER();

-- Check specific table permissions
SELECT * FROM information_schema.TABLE_PRIVILEGES 
WHERE GRANTEE LIKE '%myuser%';
```

**4. Check for schema-specific overrides interfering:**

If you set `MYSQL_SCHEMA_MYDB_PERMISSIONS=insert,update` but the error persists on a different database, the schema-specific setting doesn't apply globally.

### Fixes

**1. Enable write operations globally:**

```toml
[security]
allow_insert = true
allow_update = true
allow_delete = true  # Only if needed
```

Or via environment:
```bash
export MYSQL_ALLOW_INSERT=true
export MYSQL_ALLOW_UPDATE=true
```

**2. Enable writes for specific schema only:**

```toml
[security]
allow_insert = false          # Default: deny

[security.schema_permissions.production_db]
allow_insert = false          # Explicitly deny production

[security.schema_permissions.staging_db]
allow_insert = true           # Allow staging
allow_update = true
```

Or via environment:
```bash
MYSQL_ALLOW_INSERT=false
MYSQL_SCHEMA_STAGING_DB_PERMISSIONS=insert,update
MYSQL_SCHEMA_PRODUCTION_DB_PERMISSIONS=
```

**3. Grant MySQL permissions** (if MySQL-level denial):

```sql
-- On MySQL server, as admin
GRANT SELECT, INSERT, UPDATE, DELETE ON mydb.* TO 'myuser'@'%';
FLUSH PRIVILEGES;
```

**4. Check for read-only MySQL server:**

```sql
-- Check if MySQL is in read-only mode
SHOW VARIABLES LIKE 'read_only';
```

---

## Performance Issues

### Symptoms

- Queries taking longer than `slow_query_threshold_ms` (default 500ms)
- Full table scan warnings in results
- `tier: "slow"` or `tier: "very_slow"` in EXPLAIN plans
- Memory limit warnings: "Result truncated at N rows due to memory limit"

### Diagnosis Steps

**1. Enable automatic EXPLAIN for slow queries:**

```toml
[pool]
performance_hints = "auto"
slow_query_threshold_ms = 500
```

**2. Check query execution plans:**

```sql
-- Use mysql_explain_plan tool or add explain: true to queries
{
  "sql": "SELECT * FROM users WHERE email = 'test@example.com'",
  "explain": true
}
```

Look for:
- `"full_table_scan": true` - No index used
- `"rows_examined_estimate": 50000` - High row count
- `"tier": "slow"` or `"very_slow"` - Performance classification

**3. Check existing indexes:**

Use `mysql_schema_info` with `include: ["indexes"]` to see existing indexes on tables.

**4. Review index suggestions** in query responses:

```json
{
  "suggestions": [
    "Column `email` in WHERE clause on table `users` has no index. Consider: CREATE INDEX idx_users_email ON users(email);"
  ]
}
```

### Fixes

**1. Add missing indexes** based on WHERE clauses:

```sql
-- If your queries filter by email
CREATE INDEX idx_users_email ON users(email);

-- If your queries filter by multiple columns
CREATE INDEX idx_orders_user_date ON orders(user_id, created_at);
```

**2. Use composite indexes** for multi-column filters:

```sql
-- Bad: Separate indexes on user_id and status
CREATE INDEX idx_user ON orders(user_id);
CREATE INDEX idx_status ON orders(status);

-- Good: Composite index for common query pattern
CREATE INDEX idx_user_status ON orders(user_id, status);
```

**3. Reduce result set size** with better WHERE clauses:

```sql
-- Bad: Selecting all then filtering client-side
SELECT * FROM logs;

-- Good: Server-side filtering
SELECT * FROM logs WHERE created_at > DATE_SUB(NOW(), INTERVAL 7 DAY);
```

**4. Increase memory limits** if hitting caps on legitimate large results:

```toml
[pool]
max_result_memory_mb = 512      # Default is 256
```

**5. Adjust max_rows** for large exports:

```toml
[pool]
max_rows = 10000                # Or 0 to disable cap entirely
```

**6. Run EXPLAIN before heavy queries** in production:

```json
{
  "name": "mysql_explain_plan",
  "arguments": {
    "sql": "SELECT complex_query_here"
  }
}
```

---

## SSL/TLS Issues

See [README.md SSL/TLS Troubleshooting section](README.md#ssltls-troubleshooting) for a quick reference table.

### Additional SSL Issues

**Certificate path issues on Windows:**

```
SSL CA file not found: /path/to/ca.pem
```

Fix: Use Windows-style paths with forward slashes or escaped backslashes:

```toml
[security]
ssl_ca = "C:/certs/ca.pem"
# or
ssl_ca = "C:\\certs\\ca.pem"
```

Or via environment variable:
```cmd
set MYSQL_SSL_CA=C:\certs\ca.pem
```

**TLS version mismatch with older MySQL:**

```
Bad handshake
SSL connection error: unknown error number
```

MySQL 5.7 may require older TLS versions. Check MySQL server TLS support:

```sql
SHOW VARIABLES LIKE 'tls_version';
```

---

## SSH Tunneling Issues

See [README.md SSH Tunneling Troubleshooting section](README.md#ssh-tunneling) for common errors.

### Additional SSH Issues

**SSH agent not available on Windows:**

```
Permission denied (publickey)
```

Fix: Ensure Pageant (PuTTY agent) or Windows OpenSSH agent is running:

```powershell
# Check if ssh-agent is running
Get-Service ssh-agent

# Start it if needed
Start-Service ssh-agent

# Add your key
ssh-add C:\Users\You\.ssh\id_rsa
```

**Tunnel timeout on slow connections:**

```
Timed out waiting for local port
```

Fix: The tunnel startup timeout is fixed, but ensure your bastion host is reachable:

```bash
# Test SSH connectivity first
ssh -v user@bastion.example.com
```

**Dynamic port conflicts:**

If you see random connection failures with SSH tunnels, the local port detection may be conflicting with other services. Usually resolves on retry.

---

## Container/Docker Networking

### Symptoms

```
Connection refused
Can't connect to MySQL server on 'mysql' (111)
Connection timed out
Host is unreachable
```

### Running mysql-mcp in Docker

**1. Use Docker network for container-to-container communication:**

```yaml
# docker-compose.yml
version: '3'
services:
  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: secret
    networks:
      - mcp-network
    
  mcp-server:
    image: mysql-mcp:latest
    environment:
      MYSQL_HOST: mysql           # Use service name as hostname
      MYSQL_PORT: 3306
      MYSQL_USER: root
      MYSQL_PASS: secret
    networks:
      - mcp-network
    depends_on:
      - mysql

networks:
  mcp-network:
    driver: bridge
```

**2. Connecting to MySQL on the host from a container:**

```bash
# For Docker Desktop (Mac/Windows) - use special DNS
docker run -e MYSQL_HOST=host.docker.internal mysql-mcp

# For Linux - use host network or explicit IP
docker run --network=host -e MYSQL_HOST=localhost mysql-mcp
# OR
docker run -e MYSQL_HOST=172.17.0.1 mysql-mcp  # Docker bridge IP
```

**3. Port not exposed:**

Ensure MySQL container exposes the port:

```yaml
services:
  mysql:
    ports:
      - "3306:3306"    # Expose for external connections
```

**4. Firewall/SELinux blocking:**

```bash
# Check if MySQL is listening on all interfaces
docker exec mysql-container netstat -tlnp | grep 3306

# Should show 0.0.0.0:3306, not 127.0.0.1:3306
# If MySQL only binds to localhost, update my.cnf:
bind-address = 0.0.0.0
```

### Using mysql-mcp with Dockerized MySQL

**Health check to ensure MySQL is ready:**

```yaml
services:
  mysql:
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      timeout: 20s
      retries: 10
    
  mcp-server:
    depends_on:
      mysql:
        condition: service_healthy
```

**Connection string in Docker:**

```toml
[connection]
host = "mysql"          # Service name from docker-compose
port = 3306
user = "root"
password = "secret"
database = "myapp"
```

---

## Windows-Specific Issues

### OpenSSH Not Found

**Symptom:**
```
'ssh' binary not found
```

**Fix 1 - Install via Windows Features:**

1. Settings → Apps → Optional features → Add a feature
2. Search for "OpenSSH Client"
3. Install and restart terminal

**Fix 2 - Install via winget:**

```powershell
winget install Microsoft.OpenSSH.Beta
```

**Fix 3 - Use WSL2:**

Run mysql-mcp from WSL2 where OpenSSH is pre-installed.

### Path Separator Issues

**Symptom:**
```
Config file not found: mysql-mcp.toml
SSL CA file not found: C:\path\to\ca.pem
```

**Fix:**

Use forward slashes in TOML config (works on all platforms):

```toml
[security]
ssl_ca = "C:/Users/You/certs/ca.pem"

[ssh]
private_key = "C:/Users/You/.ssh/id_rsa"
```

Or escape backslashes:

```toml
ssl_ca = "C:\\Users\\You\\certs\\ca.pem"
```

### Terminal/Encoding Issues

**Symptom:**
```
Error: invalid UTF-8 in configuration
Password contains invalid characters
```

**Fix:**

Ensure your terminal uses UTF-8:

```powershell
# In PowerShell
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Or set system-wide:
# Settings → Time & Language → Language & Region → Administrative language settings
# → Change system locale → Check "Beta: Use Unicode UTF-8 for worldwide language support"
```

### Windows Defender/Firewall Blocking

**Symptom:**
```
Connection timeout
Network error: No connection could be made
```

**Fix:**

Add Windows Defender exclusions if running locally:

```powershell
# Run as Administrator
New-NetFirewallRule -DisplayName "MySQL MCP" -Direction Outbound -Action Allow
```

Or check if Windows Defender is blocking the SSH or MySQL connection.

### Environment Variables in PowerShell vs CMD

**PowerShell syntax:**
```powershell
$env:MYSQL_HOST = "localhost"
$env:MYSQL_USER = "root"
.\mysql-mcp.exe
```

**CMD syntax:**
```cmd
set MYSQL_HOST=localhost
set MYSQL_USER=root
mysql-mcp.exe
```

### Long Path Issues

**Symptom:**
```
Error loading config: path too long
```

**Fix:**

1. Move config file closer to root: `C:\mysql-mcp\config.toml`
2. Enable long path support in Windows 10/11:
   - Group Policy: `Computer Configuration > Administrative Templates > System > Filesystem > Enable Win32 long paths`
   - Or registry: `HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Control\FileSystem\LongPathsEnabled = 1`

### WSL2 Integration

If using WSL2 for development but MySQL on Windows host:

```toml
[connection]
host = "172.21.192.1"    # WSL2 gateway IP (check with `ip route` in WSL2)
port = 3306
```

Or use the special WSL2 hostname for accessing Windows host:

```toml
host = "host.wsl.internal"  # Or check `cat /etc/resolv.conf` for nameserver
```

---

## Getting Help

If you're still stuck:

1. **Check the logs**: Run with debug logging enabled
   ```bash
   RUST_LOG=debug ./mysql-mcp
   ```

2. **Test the connection directly**:
   ```bash
   mysql -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER -p$MYSQL_PASS -e "SELECT 1"
   ```

3. **Verify your config**:
   ```bash
   # Print effective configuration
   ./mysql-mcp --print-config 2>/dev/null || echo "Check your TOML/env vars"
   ```

4. **Open an issue** with:
   - mysql-mcp version (`./mysql-mcp --version`)
   - Operating system and version
   - MySQL server version
   - Relevant configuration (redact passwords)
   - Full error message
   - Steps to reproduce
