# Deployment and Operations Guide

This guide covers production deployment scenarios for mysql-mcp, including service setup, containerization, configuration management, secrets handling, and monitoring.

## Table of Contents

- [Overview](#overview)
- [Deployment Scenarios](#deployment-scenarios)
  - [systemd Service Setup](#systemd-service-setup)
  - [Docker Containerization](#docker-containerization)
  - [Kubernetes Deployment](#kubernetes-deployment)
- [Environment-Specific Configuration](#environment-specific-configuration)
- [Secrets Management](#secrets-management)
- [Health Checks](#health-checks)
- [Graceful Shutdown](#graceful-shutdown)
- [Monitoring and Observability](#monitoring-and-observability)
- [Security Hardening](#security-hardening)
- [Troubleshooting](#troubleshooting)

---

## Overview

mysql-mcp is a Model Context Protocol (MCP) server that exposes MySQL databases to LLM assistants. It communicates via stdio (standard input/output) using JSON-RPC, making it different from traditional HTTP services.

**Key Deployment Characteristics:**
- Uses **stdio transport** (not HTTP) — runs as a child process of the MCP client
- Single-threaded async runtime with connection pooling
- Stateless except for named session state (stored in memory)
- Supports graceful shutdown on SIGTERM and Ctrl-C

---

## Deployment Scenarios

### systemd Service Setup

For running mysql-mcp as a system service on Linux, typically when used with a local MCP client (like Claude Desktop) or as a standalone service.

#### Service User Setup

```bash
# Create dedicated service user
sudo useradd -r -s /bin/false -d /var/lib/mysql-mcp -m mysql-mcp

# Set up configuration directory
sudo mkdir -p /etc/mysql-mcp
sudo mkdir -p /var/lib/mysql-mcp
sudo chown mysql-mcp:mysql-mcp /etc/mysql-mcp /var/lib/mysql-mcp
sudo chmod 750 /etc/mysql-mcp /var/lib/mysql-mcp
```

#### Basic systemd Service

Create `/etc/systemd/system/mysql-mcp.service`:

```ini
[Unit]
Description=MySQL MCP Server
After=network.target
Documentation=https://github.com/cecil-the-coder/mysql-mcp

[Service]
Type=simple
User=mysql-mcp
Group=mysql-mcp

# Working directory for .env file support
WorkingDirectory=/var/lib/mysql-mcp

# Binary location
ExecStart=/usr/local/bin/mysql-mcp

# Restart policy
Restart=on-failure
RestartSec=5

# Resource limits
LimitNOFILE=65535
LimitNPROC=4096

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/mysql-mcp
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictRealtime=true
RestrictSUIDSGID=true
LockPersonality=true
MemoryDenyWriteExecute=true

# Environment
Environment="RUST_LOG=info"
EnvironmentFile=-/etc/mysql-mcp/environment

# Graceful shutdown
TimeoutStopSec=30
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
```

#### Production systemd Service (with SSH tunnel support)

For production with SSH key access and SSL certificates:

```ini
[Unit]
Description=MySQL MCP Server (Production)
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=mysql-mcp
Group=mysql-mcp

WorkingDirectory=/var/lib/mysql-mcp
ExecStart=/usr/local/bin/mysql-mcp

# Allow SSH agent access for tunneling
Environment="SSH_AUTH_SOCK=%t/ssh-agent.socket"
Environment="RUST_LOG=warn"

# Restart configuration
Restart=on-failure
RestartSec=10
StartLimitInterval=60
StartLimitBurst=3

# Resource limits
LimitNOFILE=65535
LimitNPROC=4096

# Security (relaxed for SSH and SSL cert access)
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=/var/lib/mysql-mcp /etc/mysql-mcp
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true

# Private directories
PrivateTmp=true

# Capability bounding
CapabilityBoundingSet=CAP_NET_BIND_SERVICE

# Graceful shutdown - 30 seconds for in-flight queries
TimeoutStopSec=30
KillSignal=SIGTERM
SendSIGKILL=yes

[Install]
WantedBy=multi-user.target
```

#### Service Management

```bash
# Install the binary
sudo cp mysql-mcp /usr/local/bin/
sudo chmod +x /usr/local/bin/mysql-mcp

# Create configuration
sudo cp mysql-mcp.toml /etc/mysql-mcp/
sudo chmod 600 /etc/mysql-mcp/mysql-mcp.toml

# Reload systemd and start service
sudo systemctl daemon-reload
sudo systemctl enable mysql-mcp
sudo systemctl start mysql-mcp

# Check status
sudo systemctl status mysql-mcp
sudo journalctl -u mysql-mcp -f
```

### Docker Containerization

#### Basic Dockerfile

```dockerfile
FROM rust:1.75-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Runtime image
FROM debian:bookworm-slim

# Install OpenSSH for tunnel support
RUN apt-get update && apt-get install -y \
    openssh-client \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -m -s /bin/false mysql-mcp

# Copy binary
COPY --from=builder /app/target/release/mysql-mcp /usr/local/bin/

# Set up directories
RUN mkdir -p /etc/mysql-mcp /home/mysql-mcp/.ssh && \
    chown -R mysql-mcp:mysql-mcp /etc/mysql-mcp /home/mysql-mcp

USER mysql-mcp

# Environment defaults
ENV RUST_LOG=info \
    MYSQL_HOST=localhost \
    MYSQL_PORT=3306 \
    MYSQL_USER=root \
    MYSQL_POOL_SIZE=20

# MCP servers use stdio, not ports
# No EXPOSE needed

ENTRYPOINT ["mysql-mcp"]
```

#### Building and Running

```bash
# Build image
docker build -t mysql-mcp:latest .

# Run with environment variables
docker run -i --rm \
  -e MYSQL_HOST=db.example.com \
  -e MYSQL_PORT=3306 \
  -e MYSQL_USER=analytics \
  -e MYSQL_PASS="${DB_PASSWORD}" \
  -e MYSQL_DB=production \
  -e MYSQL_SSL=true \
  mysql-mcp:latest

# Run with config file mounted
docker run -i --rm \
  -v /path/to/mysql-mcp.toml:/etc/mysql-mcp/mysql-mcp.toml:ro \
  -v /path/to/ca.pem:/etc/mysql-mcp/ca.pem:ro \
  -e MCP_CONFIG_FILE=/etc/mysql-mcp/mysql-mcp.toml \
  mysql-mcp:latest

# Interactive debug shell
docker run -it --rm --entrypoint /bin/bash mysql-mcp:latest
```

**Note:** The `-i` flag is required for stdio transport to work correctly.

#### Docker Compose Example

```yaml
version: '3.8'

services:
  mysql-mcp:
    image: mysql-mcp:latest
    container_name: mysql-mcp
    stdin_open: true  # Required for MCP stdio
    tty: true         # Recommended for proper signal handling
    restart: unless-stopped
    
    environment:
      MYSQL_HOST: ${MYSQL_HOST}
      MYSQL_PORT: ${MYSQL_PORT:-3306}
      MYSQL_USER: ${MYSQL_USER}
      MYSQL_PASS: ${MYSQL_PASS}
      MYSQL_DB: ${MYSQL_DB}
      MYSQL_POOL_SIZE: ${MYSQL_POOL_SIZE:-20}
      MYSQL_SSL: ${MYSQL_SSL:-true}
      MYSQL_SSL_CA: /etc/mysql-mcp/ca.pem
      RUST_LOG: ${RUST_LOG:-info}
    
    volumes:
      - ./config/mysql-mcp.toml:/etc/mysql-mcp/mysql-mcp.toml:ro
      - ./certs/ca.pem:/etc/mysql-mcp/ca.pem:ro
      - ./ssh/id_rsa:/home/mysql-mcp/.ssh/id_rsa:ro
      - ./ssh/known_hosts:/home/mysql-mcp/.ssh/known_hosts:ro
    
    # Resource limits
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 512M
        reservations:
          cpus: '0.25'
          memory: 128M
    
    # Security options
    security_opt:
      - no-new-privileges:true
    read_only: true
    tmpfs:
      - /tmp:noexec,nosuid,size=100m
```

### Kubernetes Deployment

Since mysql-mcp uses stdio transport, it runs as a sidecar container alongside the MCP client, not as a standalone service with ingress.

#### Sidecar Pattern Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp-with-mysql-mcp
spec:
  replicas: 3
  selector:
    matchLabels:
      app: myapp
  template:
    metadata:
      labels:
        app: myapp
    spec:
      containers:
        # Main application that uses MCP
        - name: myapp
          image: myapp:latest
          env:
            - name: MCP_SERVER_PATH
              value: "/usr/local/bin/mysql-mcp"
          volumeMounts:
            - name: mcp-binary
              mountPath: /usr/local/bin/mysql-mcp
              subPath: mysql-mcp
          resources:
            limits:
              memory: "512Mi"
              cpu: "500m"
        
        # MCP server sidecar
        - name: mysql-mcp
          image: mysql-mcp:latest
          stdin: true      # Required for MCP stdio
          tty: true
          env:
            - name: MYSQL_HOST
              valueFrom:
                secretKeyRef:
                  name: db-credentials
                  key: host
            - name: MYSQL_PORT
              value: "3306"
            - name: MYSQL_USER
              valueFrom:
                secretKeyRef:
                  name: db-credentials
                  key: username
            - name: MYSQL_PASS
              valueFrom:
                secretKeyRef:
                  name: db-credentials
                  key: password
            - name: MYSQL_DB
              value: "production"
            - name: MYSQL_SSL
              value: "true"
            - name: MYSQL_SSL_CA
              value: "/etc/mysql-mcp/certs/ca.pem"
            - name: RUST_LOG
              value: "warn"
          volumeMounts:
            - name: db-certs
              mountPath: /etc/mysql-mcp/certs
              readOnly: true
            - name: config
              mountPath: /etc/mysql-mcp/mysql-mcp.toml
              subPath: mysql-mcp.toml
          resources:
            limits:
              memory: "256Mi"
              cpu: "250m"
            requests:
              memory: "128Mi"
              cpu: "100m"
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 1000
            capabilities:
              drop:
                - ALL
      
      volumes:
        - name: mcp-binary
          hostPath:
            path: /opt/bin/mysql-mcp
            type: File
        - name: db-certs
          secret:
            secretName: db-ca-cert
        - name: config
          configMap:
            name: mysql-mcp-config
```

#### ConfigMap for Configuration

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mysql-mcp-config
data:
  mysql-mcp.toml: |
    [connection]
    host = "${MYSQL_HOST}"
    port = 3306
    user = "${MYSQL_USER}"
    password = "${MYSQL_PASS}"
    database = "production"
    
    [pool]
    size = 20
    query_timeout_ms = 30000
    connect_timeout_ms = 10000
    cache_ttl_secs = 300
    max_rows = 10000
    performance_hints = "auto"
    
    [security]
    allow_insert = false
    allow_update = false
    allow_delete = false
    allow_ddl = false
    ssl = true
    ssl_ca = "/etc/mysql-mcp/certs/ca.pem"
    allow_runtime_connections = false
```

#### Secrets Management

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: db-credentials
type: Opaque
stringData:
  host: "db.production.internal"
  username: "analytics"
  password: "<redacted>"
---
apiVersion: v1
kind: Secret
metadata:
  name: db-ca-cert
type: Opaque
data:
  ca.pem: <base64-encoded CA certificate>
```

---

## Environment-Specific Configuration

### Development Environment

**Characteristics:**
- Relaxed permissions (allow writes for testing)
- Shorter timeouts for faster feedback
- No SSL (local Docker/MySQL)
- Debug logging enabled

**Configuration (mysql-mcp.toml):**
```toml
[connection]
host = "localhost"
port = 3306
user = "root"
password = "devpass"
database = "dev_db"

[pool]
size = 10
query_timeout_ms = 15000
connect_timeout_ms = 5000
cache_ttl_secs = 0
max_rows = 5000
performance_hints = "auto"
slow_query_threshold_ms = 100

[security]
allow_insert = true
allow_update = true
allow_delete = true
allow_ddl = true
ssl = false
allow_runtime_connections = true
max_sessions = 10
```

### Staging Environment

**Characteristics:**
- Production-like but with relaxed limits
- SSL enabled with staging certificates
- Read-only or limited write permissions
- Moderate logging

**Configuration:**
```toml
[connection]
host = "db.staging.internal"
port = 3306
user = "staging_readonly"
password = "${MYSQL_PASSWORD}"
database = "staging_db"

[pool]
size = 15
query_timeout_ms = 30000
connect_timeout_ms = 10000
cache_ttl_secs = 120
max_rows = 5000
performance_hints = "always"

[security]
allow_insert = false
allow_update = false
allow_delete = false
allow_ddl = false
ssl = true
ssl_ca = "/etc/mysql-mcp/ca-staging.pem"
allow_runtime_connections = false
max_sessions = 20
```

### Production Environment

**Characteristics:**
- Strict read-only by default
- SSL mandatory with CA verification
- Conservative timeouts and limits
- Minimal logging (warn level)
- Connection pooling tuned for production load

See `examples/production-ssl.toml` for a complete production configuration template.

**Key Production Settings:**
```toml
[connection]
host = "db.production.internal"
user = "analytics"  # Dedicated read-only user

[pool]
size = 20
query_timeout_ms = 60000      # Allow for complex analytical queries
connect_timeout_ms = 15000    # Account for network latency
cache_ttl_secs = 300          # 5-minute schema cache
max_rows = 10000              # Limit memory usage
performance_hints = "auto"    # Only EXPLAIN slow queries

[security]
allow_insert = false
allow_update = false
allow_delete = false
allow_ddl = false
ssl = true
ssl_ca = "/etc/mysql-mcp/ca.pem"
allow_runtime_connections = false  # Prevent arbitrary connections
max_sessions = 50
max_total_connections = 100
```

---

## Secrets Management

### Environment Variables (Basic)

```bash
# .env file (chmod 600)
MYSQL_HOST=db.production.internal
MYSQL_PORT=3306
MYSQL_USER=analytics
MYSQL_PASS=<redacted>
MYSQL_DB=production_db
```

### Docker Secrets

```bash
# Create secrets
echo "mypassword" | docker secret create mysql_password -
docker secret create mysql_ca_cert ./ca.pem
```

```yaml
# docker-compose.yml
services:
  mysql-mcp:
    image: mysql-mcp:latest
    secrets:
      - mysql_password
      - mysql_ca_cert
    environment:
      MYSQL_PASS_FILE: /run/secrets/mysql_password
      MYSQL_SSL_CA: /run/secrets/mysql_ca_cert
```

### Kubernetes Secrets

See the [Kubernetes Deployment](#kubernetes-deployment) section above.

### HashiCorp Vault Integration

```bash
# Using Vault agent sidecar
vault kv put secret/mysql-mcp \
  host=db.production.internal \
  user=analytics \
  password=<redacted>
```

With Vault agent template:
```hcl
template {
  destination = "/etc/mysql-mcp/credentials.env"
  contents = <<EOT
{{ with secret "secret/mysql-mcp" }}
MYSQL_HOST={{ .Data.host }}
MYSQL_USER={{ .Data.user }}
MYSQL_PASS={{ .Data.password }}
{{ end }}
EOT
}
```

### AWS Secrets Manager

```yaml
# Using AWS Secrets Manager CSI driver for Kubernetes
volumes:
  - name: secrets-store-inline
    csi:
      driver: secrets-store.csi.k8s.io
      readOnly: true
      volumeAttributes:
        secretProviderClass: mysql-mcp-credentials
```

---

## Health Checks

**Important:** mysql-mcp uses stdio transport (not HTTP), so traditional HTTP health endpoints are not available. Health checking must be done via process monitoring and log analysis.

### Process-Based Health Checks

#### systemd
```bash
# Check service status
systemctl is-active mysql-mcp

# Monitor via journald
journalctl -u mysql-mcp -n 50 -f
```

#### Docker
```bash
# Check container is running
docker ps --filter "name=mysql-mcp" --format "table {{.Names}}\t{{.Status}}"

# Check logs
docker logs mysql-mcp --tail 50 -f
```

#### Kubernetes
```bash
# Check pod status
kubectl get pods -l app=myapp

# View logs
kubectl logs deployment/myapp -c mysql-mcp

# Check for restarts
kubectl get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.containerStatuses[?(@.name=="mysql-mcp")].restartCount}{"\n"}{end}'
```

### Log-Based Health Indicators

**Healthy startup indicators:**
```
INFO mysql_mcp: mysql-mcp starting
INFO mysql_mcp: Configuration loaded
INFO mysql_mcp: Database pool created
INFO mysql_mcp: MCP server starting on stdio
```

**Error indicators requiring attention:**
```
WARN mysql_mcp: Pool warmup attempt failed
ERROR mysql_mcp: Configuration validation failed
ERROR mysql_mcp: SSH tunnel or connection failed
```

### Application-Level Health

Use the `mysql_server_info` tool to verify database connectivity:

```json
{
  "name": "mysql_server_info",
  "arguments": {}
}
```

Expected healthy response includes:
- `mysql_version` — confirms database connectivity
- `accessible_features` — confirms permission setup
- No `error` field

---

## Graceful Shutdown

mysql-mcp supports graceful shutdown via:
- **SIGTERM** — primary shutdown signal
- **Ctrl-C (SIGINT)** — interactive shutdown

### Shutdown Behavior

1. **Signal received** — Server begins shutdown sequence
2. **Session cleanup** — Background reaper task terminates
3. **Connection draining** — In-flight queries complete (up to query_timeout_ms)
4. **Pool closure** — Database connections are released
5. **Process exit** — Exit code 0 on clean shutdown

### systemd Integration

```ini
[Service]
TimeoutStopSec=30
KillSignal=SIGTERM
SendSIGKILL=yes
```

The 30-second timeout allows in-flight queries to complete. Adjust based on your `query_timeout_ms` setting.

### Kubernetes Integration

```yaml
lifecycle:
  preStop:
    exec:
      command: ["/bin/sh", "-c", "sleep 5"]
terminationGracePeriodSeconds: 30
```

The preStop hook provides time for the MCP client to finish its current request before the container terminates.

### Docker Integration

```bash
# Graceful stop
docker stop --time=30 mysql-mcp

# Or with docker-compose
docker-compose stop -t 30 mysql-mcp
```

---

## Monitoring and Observability

### Logging

mysql-mcp uses the `tracing` crate for structured logging. Configure via the `RUST_LOG` environment variable.

**Log Levels:**
- `error` — Critical failures (connection failures, config errors)
- `warn` — Warning conditions (pool warmup failures, slow queries)
- `info` — Normal operations (startup, shutdown, major events)
- `debug` — Detailed operation info (query execution, pool stats)
- `trace` — Very detailed debug info

**Recommended settings:**
```bash
# Production
RUST_LOG=warn

# Staging/Debugging
RUST_LOG=info,mysql_mcp=debug

# Trace all operations (verbose)
RUST_LOG=trace
```

### Log Shipping

#### systemd with journald
```bash
# Forward to central logging
journalctl -u mysql-mcp -o json | jq -c '.' | fluent-bit

# Or via rsyslog
echo ':programname, isequal, "mysql-mcp" /var/log/mysql-mcp.log' | sudo tee /etc/rsyslog.d/mysql-mcp.conf
```

#### Docker
```yaml
# docker-compose.yml with log shipping
services:
  mysql-mcp:
    logging:
      driver: "fluentd"
      options:
        fluentd-address: localhost:24224
        tag: docker.mysql-mcp
```

#### Kubernetes
```yaml
# Pod logging with fluent-bit/fluentd
# Logs are automatically collected from stdout/stderr
spec:
  containers:
    - name: mysql-mcp
      env:
        - name: RUST_LOG
          value: "info"
```

**Important:** All logging goes to **stderr** to avoid corrupting the MCP JSON-RPC stream on stdout.

### Metrics

mysql-mcp does not expose Prometheus/metrics endpoints (uses stdio transport). Monitor via:

1. **Log analysis** for error rates and query patterns
2. **Process metrics** (CPU, memory, file descriptors)
3. **Database metrics** (via MySQL `SHOW STATUS` and `SHOW PROCESSLIST`)

### Query Performance Monitoring

Enable `performance_hints=auto` to automatically capture slow query plans:

```toml
[pool]
performance_hints = "auto"
slow_query_threshold_ms = 500
```

This logs query plans for slow queries, which can be analyzed for missing indexes or full table scans.

### Alerting Rules

#### systemd (node_exporter + Prometheus)

```yaml
# Alert on service down
- alert: MysqlMcpServiceDown
  expr: node_systemd_unit_state{name="mysql-mcp.service", state="active"} == 0
  for: 1m
  severity: critical

# Alert on high restart rate
- alert: MysqlMcpFrequentRestarts
  expr: increase(node_systemd_service_restarts{name="mysql-mcp.service"}[1h]) > 3
  severity: warning
```

#### Kubernetes

```yaml
# Alert on pod restarts
- alert: MysqlMcpPodCrashLooping
  expr: rate(kube_pod_container_status_restarts_total{container="mysql-mcp"}[10m]) > 0
  severity: critical

# Alert on high memory usage
- alert: MysqlMcpHighMemoryUsage
  expr: container_memory_usage_bytes{container="mysql-mcp"} > 200Mi
  severity: warning
```

### Trace Context

mysql-mcp supports correlation IDs via the MCP protocol. When available, trace IDs are logged for request tracking.

---

## Security Hardening

### File Permissions

```bash
# Configuration file (contains credentials)
chmod 600 /etc/mysql-mcp/mysql-mcp.toml
chown mysql-mcp:mysql-mcp /etc/mysql-mcp/mysql-mcp.toml

# SSL certificates
chmod 644 /etc/mysql-mcp/ca.pem
chown mysql-mcp:mysql-mcp /etc/mysql-mcp/ca.pem

# SSH keys (if using tunneling)
chmod 600 /home/mysql-mcp/.ssh/id_rsa
chown mysql-mcp:mysql-mcp /home/mysql-mcp/.ssh/id_rsa
chmod 644 /home/mysql-mcp/.ssh/known_hosts
```

### Network Security

1. **Use SSL/TLS** for all production connections:
   ```toml
   [security]
   ssl = true
   ssl_ca = "/etc/mysql-mcp/ca.pem"
   ```

2. **Restrict runtime connections** in production:
   ```toml
   [security]
   allow_runtime_connections = false
   ```

3. **SSH tunnel security**:
   ```toml
   [ssh]
   known_hosts_check = "strict"  # Never use "insecure" in production
   ```

### Container Security

```dockerfile
# Run as non-root
USER mysql-mcp

# Read-only root filesystem
read_only: true

# Drop all capabilities
securityContext:
  capabilities:
    drop:
      - ALL
```

### Database User Permissions

Grant minimal database privileges:

```sql
-- Read-only analytics user
CREATE USER 'analytics'@'%' IDENTIFIED BY '<strong_password>';
GRANT SELECT ON production_db.* TO 'analytics'@'%';

-- Optional: Allow writes to specific tables
GRANT INSERT, UPDATE ON production_db.audit_log TO 'analytics'@'%';

-- Never grant these for read-only access:
-- GRANT CREATE, DROP, ALTER, DELETE
```

---

## Troubleshooting

### Common Issues

#### Connection Failures

**Symptoms:** Pool warmup fails, `mysql_server_info` returns error

**Diagnostic steps:**
```bash
# Check network connectivity
nc -zv $MYSQL_HOST $MYSQL_PORT

# Verify credentials
mysql -h $MYSQL_HOST -u $MYSQL_USER -p -e "SELECT 1"

# Check SSL certificate
openssl x509 -in /etc/mysql-mcp/ca.pem -text -noout

# Review logs
journalctl -u mysql-mcp -n 100 --no-pager
```

#### High Memory Usage

**Symptoms:** OOM kills, container restarts

**Causes & Solutions:**
- `max_rows` too high → Reduce to 1000-5000
- `max_result_memory_mb` exceeded → Reduce or increase container memory
- Large result sets without LIMIT → Enable `max_rows` cap

#### Slow Query Performance

**Diagnosis:**
- Enable `performance_hints=auto` to capture slow query plans
- Check for `full_table_scan: true` in plan output
- Review `suggestions` field for missing index recommendations

**Fix:**
```sql
-- Example: Add index for frequently filtered column
CREATE INDEX idx_users_email ON users(email);
```

#### SSH Tunnel Issues

**Symptoms:** Tunnel establishment fails, connection timeouts

**Diagnostic steps:**
```bash
# Verify SSH connectivity
ssh -i /path/to/key -o StrictHostKeyChecking=accept-new \
    $MYSQL_SSH_USER@$MYSQL_SSH_HOST "echo OK"

# Check known_hosts
ssh-keyscan -H $MYSQL_SSH_HOST >> ~/.ssh/known_hosts

# Test tunnel manually
ssh -N -L 13306:$MYSQL_HOST:3306 $MYSQL_SSH_USER@$MYSQL_SSH_HOST
mysql -h 127.0.0.1 -P 13306 -u $MYSQL_USER -p
```

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
RUST_LOG=debug,mysql_mcp=trace mysql-mcp
```

**Warning:** Debug logs may contain sensitive data (SQL queries, connection strings). Use only in non-production environments.

### Getting Help

- Check logs first: `journalctl -u mysql-mcp -n 200`
- Review configuration validation errors on startup
- Verify file permissions on credentials and certificates
- Test database connectivity independently using `mysql` client
- File issues: https://github.com/cecil-the-coder/mysql-mcp/issues

---

## Appendix: Configuration Quick Reference

| Environment Variable | TOML Path | Description |
|---------------------|-----------|-------------|
| `MYSQL_HOST` | `connection.host` | MySQL server hostname |
| `MYSQL_PORT` | `connection.port` | MySQL server port |
| `MYSQL_USER` | `connection.user` | MySQL username |
| `MYSQL_PASS` | `connection.password` | MySQL password |
| `MYSQL_DB` | `connection.database` | Default database |
| `MYSQL_POOL_SIZE` | `pool.size` | Connection pool size |
| `MYSQL_SSL` | `security.ssl` | Enable SSL/TLS |
| `MYSQL_SSL_CA` | `security.ssl_ca` | Path to CA certificate |
| `MYSQL_ALLOW_INSERT` | `security.allow_insert` | Allow INSERT statements |
| `MYSQL_ALLOW_UPDATE` | `security.allow_update` | Allow UPDATE statements |
| `MYSQL_ALLOW_DELETE` | `security.allow_delete` | Allow DELETE statements |
| `MYSQL_ALLOW_DDL` | `security.allow_ddl` | Allow DDL statements |
| `MYSQL_ALLOW_RUNTIME_CONNECTIONS` | `security.allow_runtime_connections` | Enable mysql_connect tool |
| `RUST_LOG` | — | Log level (error/warn/info/debug/trace) |
| `MCP_CONFIG_FILE` | — | Path to TOML config file |
