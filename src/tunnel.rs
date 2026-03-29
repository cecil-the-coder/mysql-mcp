//! SSH tunnel management for secure database access.
//!
//! This module provides SSH tunnel functionality for connecting to MySQL databases
//! through bastion hosts or jump servers. It spawns and manages `ssh` child processes
//! with local port forwarding, handling the full lifecycle from establishment to
//! graceful shutdown.
//!
//! # Key Types
//!
//! - [`TunnelHandle`] - Owning handle to a live SSH tunnel; dropping it tears down the tunnel
//!
//! # Key Functions
//!
//! - [`spawn_ssh_tunnel`] - Establishes a new SSH tunnel and returns a handle
//! - [`build_ssh_args`] - Constructs SSH command-line arguments (usable for testing)
//!
//! # Features
//!
//! - Automatic local port allocation
//! - Configurable host key verification (strict, accept-new, insecure)
//! - Zombie process prevention via background reaper task
//! - Graceful async shutdown via [`TunnelHandle::close`]
//!
//! # Example
//!
//! ```ignore
//! let tunnel = spawn_ssh_tunnel(&ssh_config, "db.internal", 3306).await?;
//! // Connect to 127.0.0.1:tunnel.local_port
//! tunnel.close().await?;
//! ```

use crate::config::SshConfig;
use anyhow::Result;
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

/// Global registry of Child handles that need to be reaped.
/// This prevents zombie processes when tunnels are dropped without a proper close()
/// (e.g., during session reaper timeout scenarios).
/// Uses tokio::sync::Mutex since reaping happens in async context.
static ZOMBIE_REAPER: OnceLock<ZombieReaper> = OnceLock::new();

/// Holds child processes awaiting reaping and ensures the background task is spawned.
struct ZombieReaper {
    children: Mutex<Vec<Child>>,
}

impl ZombieReaper {
    /// Get or initialize the global zombie reaper.
    fn get() -> &'static Self {
        ZOMBIE_REAPER.get_or_init(|| {
            // Spawn the background reaper task
            spawn_reaper_task();
            ZombieReaper {
                children: Mutex::new(Vec::new()),
            }
        })
    }

    /// Register a child for eventual reaping.
    /// Called from Drop via a spawned task since we can't await in Drop.
    async fn register_child(&self, child: Child) {
        self.children.lock().await.push(child);
    }
}

/// Spawn the background task that periodically reaps zombie SSH processes.
fn spawn_reaper_task() {
    tokio::spawn(async {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            reap_zombies().await;
        }
    });
}

/// Attempt to reap any registered zombie processes by calling wait() on them.
async fn reap_zombies() {
    let Some(reaper) = ZOMBIE_REAPER.get() else {
        return;
    };

    let mut children = reaper.children.lock().await;
    let mut still_running = Vec::new();

    for mut child in children.drain(..) {
        // Check if the child has exited using try_wait (non-blocking)
        match child.try_wait() {
            Ok(Some(_status)) => {
                // Child has exited and been reaped
                tracing::trace!(pid = child.id().unwrap_or(0), "Reaped zombie SSH process");
            }
            Ok(None) => {
                // Child still running, keep it for next cycle
                still_running.push(child);
            }
            Err(e) => {
                // Error checking status (e.g., process already reaped elsewhere)
                tracing::trace!("Error checking SSH process status: {}", e);
            }
        }
    }

    // Put back the children that are still running
    *children = still_running;
}

/// A live SSH tunnel that forwards `127.0.0.1:{local_port}` through an SSH connection
/// to a remote database host/port.
///
/// Dropping this handle kills the `ssh` child process — the tunnel tears down automatically
/// when the owning `Session` is dropped or `close()` is called explicitly.
pub struct TunnelHandle {
    /// The SSH child process. Wrapped in Option to allow taking ownership in Drop.
    pub(crate) child: Option<Child>,
    /// The local port on 127.0.0.1 that sqlx should connect to.
    pub local_port: u16,
}

impl TunnelHandle {
    /// Gracefully close the tunnel: kill the ssh process and wait for it to exit.
    /// Prefer this over relying on Drop when you want clean teardown.
    pub async fn close(mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            // Best-effort kill — the process may have already exited, which is fine.
            if let Err(e) = child.kill().await {
                tracing::debug!("Failed to kill SSH process in close(): {}", e);
                // Do NOT return early: we still need to wait() to reap the child
                // and prevent a zombie process.
            }
            // Always wait for the child to be fully reaped so no zombie process is left behind.
            if let Err(e) = child.wait().await {
                tracing::debug!("Failed to wait for SSH process: {}", e);
            }
        }
        Ok(())
    }
}

impl Drop for TunnelHandle {
    fn drop(&mut self) {
        // Take the child to hand off to the zombie reaper
        if let Some(mut child) = self.child.take() {
            // Non-blocking best-effort kill. Cannot await in Drop.
            if let Err(e) = child.start_kill() {
                tracing::debug!("Failed to kill SSH process: {}", e);
                // Fall through to still register with reaper - the child may already be dead
                // and we still need to call wait() on it to reap it.
            }

            // To prevent zombies, we need to ensure wait() is called eventually.
            // Spawn a task to hand off the child to the zombie reaper.
            // Try to register with the reaper via a spawned task
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    ZombieReaper::get().register_child(child).await;
                });
            } else {
                // No tokio runtime available; directly reap the child in a
                // background thread. We cannot use ZombieReaper here because
                // its background reaper task would be spawned on a temporary
                // runtime that is dropped immediately, leaving children
                // un-reaped. Instead, do a direct best-effort try_wait().
                std::thread::spawn(move || {
                    // Give the child a brief moment to exit after start_kill()
                    std::thread::sleep(Duration::from_millis(100));
                    match child.try_wait() {
                        Ok(Some(_)) => {
                            tracing::trace!("Reaped zombie SSH process in fallback path");
                        }
                        Ok(None) => {
                            // Child still running after kill; best effort.
                            tracing::trace!(
                                "SSH process still running after kill in fallback path"
                            );
                        }
                        Err(e) => {
                            tracing::trace!(
                                "Error checking SSH process status in fallback path: {}",
                                e
                            );
                        }
                    }
                    // Child is dropped here; if still running, Drop sends SIGKILL (Unix)
                });
            }
        }
    }
}

/// Validate that the SSH host contains only valid hostname characters.
/// Valid characters are alphanumeric (a-z, A-Z, 0-9), hyphen (-), and dot (.).
/// IPv6 addresses are allowed, including those enclosed in brackets (e.g., [::1]).
fn validate_ssh_host(host: &str) -> Result<()> {
    if host.is_empty() {
        return Err(anyhow::anyhow!("SSH host cannot be empty"));
    }
    // Enforce RFC 1123 maximum hostname length of 255 characters. Very long
    // hostnames can be silently truncated by downstream systems (DNS resolvers,
    // SSH client argument buffers, etc.), leading to confusing errors.
    if host.len() > 255 {
        return Err(anyhow::anyhow!(
            "SSH host exceeds maximum length of 255 characters (got {})",
            host.len()
        ));
    }
    // Explicitly reject null bytes and ASCII control characters before the general
    // character loop. These characters can cause subtle issues with subprocess spawning
    // (e.g., null bytes truncating strings, newlines injecting arguments) and deserve
    // clear, specific error messages.
    if let Some(pos) = host.find('\0') {
        return Err(anyhow::anyhow!(
            "SSH host contains a null byte at position {} — this is not a valid hostname",
            pos
        ));
    }
    if let Some(c) = host.chars().find(|c| c.is_ascii_control()) {
        return Err(anyhow::anyhow!(
            "SSH host contains control character '{:#x}' — hostnames must not contain control characters",
            c as u32
        ));
    }
    for c in host.chars() {
        // Allow alphanumeric, hyphen, dot, and IPv6-related characters ([, ], :)
        if !c.is_ascii_alphanumeric() && c != '-' && c != '.' && c != ':' && c != '[' && c != ']' {
            return Err(anyhow::anyhow!(
                "SSH host contains invalid character '{}'. Host may only contain alphanumeric characters, hyphens, dots, and IPv6 notation",
                c
            ));
        }
    }
    Ok(())
}

/// Build the argument list for the `ssh` subprocess.
/// Extracted as a pure function so it can be unit-tested without spawning a real process.
pub(crate) fn build_ssh_args(
    ssh: &SshConfig,
    db_host: &str,
    db_port: u16,
    local_port: u16,
) -> Result<Vec<String>> {
    // Validate the SSH host to prevent potential command injection
    validate_ssh_host(&ssh.host)?;
    // Known hosts check: "strict" -> "yes", "accept-new" -> "accept-new", "insecure" -> "no"
    let shk = match ssh.known_hosts_check.as_str() {
        "accept-new" => "accept-new",
        "insecure" => "no",
        _ => "yes", // "strict" and any unknown value -> safest default
    };

    // Build final Vec<String> directly, using String::from for static literals
    let mut args = Vec::new();

    // -N: do not execute a remote command (tunnel only)
    args.push(String::from("-N"));

    // Local port forwarding: 127.0.0.1:local_port -> db_host:db_port
    args.push(String::from("-L"));
    args.push(format!("127.0.0.1:{}:{}:{}", local_port, db_host, db_port));

    // SSH server port
    args.push(String::from("-p"));
    args.push(ssh.port.to_string());

    // Prevent any interactive prompts — essential for a headless server
    args.push(String::from("-o"));
    args.push(String::from("BatchMode=yes"));

    // Connection timeout for the SSH handshake itself
    args.push(String::from("-o"));
    args.push(String::from("ConnectTimeout=10"));

    // StrictHostKeyChecking option
    args.push(String::from("-o"));
    args.push(format!("StrictHostKeyChecking={}", shk));

    // Custom known_hosts file (e.g. for Docker/CI environments)
    if let Some(ref kh_file) = ssh.known_hosts_file {
        args.push(String::from("-o"));
        args.push(format!("UserKnownHostsFile={}", kh_file));
    }

    // Private key file (if provided; otherwise relies on SSH agent or default ~/.ssh/id_*)
    if let Some(ref key_path) = ssh.private_key {
        args.push(String::from("-i"));
        args.push(key_path.clone());
    }

    // user@host — always last
    args.push(format!("{}@{}", ssh.user, ssh.host));

    Ok(args)
}

/// Spawn an SSH tunnel and wait until the local forwarding port is accepting connections.
///
/// Binds a random local TCP port, spawns `ssh -N -L local_port:db_host:db_port user@bastion`,
/// and polls until the local port is accepting connections or the timeout expires.
///
/// Returns a `TunnelHandle` that keeps the tunnel alive for its lifetime.
/// Dropping the handle kills the ssh process.
pub async fn spawn_ssh_tunnel(
    ssh: &SshConfig,
    db_host: &str,
    db_port: u16,
) -> Result<TunnelHandle> {
    // Allocate a free local port by binding briefly and releasing.
    // Use tokio's async TCP listener with a timeout to avoid blocking indefinitely
    // on local port allocation issues.
    let local_port = {
        let listener =
            tokio::time::timeout(Duration::from_secs(5), TcpListener::bind("127.0.0.1:0"))
                .await
                .map_err(|_| {
                    anyhow::anyhow!("SSH tunnel: timed out waiting for local port allocation")
                })?
                .map_err(|e| {
                    anyhow::anyhow!("Failed to allocate local port for SSH tunnel: {}", e)
                })?;
        let port = listener.local_addr()?.port();
        drop(listener);
        port
    };

    let args = build_ssh_args(ssh, db_host, db_port, local_port)?;

    tracing::debug!(
        bastion = %ssh.host,
        ssh_port = ssh.port,
        local_port = local_port,
        db_host = %db_host,
        db_port = db_port,
        "spawning SSH tunnel"
    );

    let child = Command::new("ssh")
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        // Capture stderr so we can include it in error messages on premature exit
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                anyhow::anyhow!(
                    "SSH tunnel: 'ssh' binary not found. \
                     Install OpenSSH (macOS/Linux: built-in; \
                     Windows: enable via Windows Features or 'winget install Microsoft.OpenSSH.Beta')"
                )
            } else {
                anyhow::anyhow!("SSH tunnel: failed to spawn ssh process: {}", e)
            }
        })?;

    let mut handle = TunnelHandle {
        child: Some(child),
        local_port,
    };

    // Poll until the local port is accepting connections or the child exits early.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        // Check if the child process has already exited (tunnel failed to start)
        let Some(child) = handle.child.as_mut() else {
            return Err(anyhow::anyhow!(
                "SSH tunnel: child process is not available (unexpected state)"
            ));
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                // Collect any buffered stderr for diagnostics (best effort, 500 ms cap).
                let stderr_snippet = if let Some(mut stderr) = child.stderr.take() {
                    let mut buf = vec![0u8; 2048];
                    let n = tokio::time::timeout(
                        Duration::from_millis(500),
                        tokio::io::AsyncReadExt::read(&mut stderr, &mut buf),
                    )
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .unwrap_or(0);
                    let s = String::from_utf8_lossy(&buf[..n]).trim().to_string();
                    if s.is_empty() {
                        None
                    } else {
                        Some(s)
                    }
                } else {
                    None
                };
                return Err(anyhow::anyhow!(
                    "SSH tunnel: ssh process exited prematurely with status {} \
                     (bastion=[REDACTED]). {}",
                    status,
                    stderr_snippet
                        .as_deref()
                        .map(|s| format!("SSH error output: {}", s))
                        .unwrap_or_else(|| "No stderr output captured.".to_string())
                ));
            }
            Ok(None) => {} // still running, continue polling
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "SSH tunnel: error checking ssh process status: {}",
                    e
                ));
            }
        }

        // Try to connect to the local forwarding port
        match tokio::net::TcpStream::connect(std::net::SocketAddr::from((
            [127, 0, 0, 1],
            local_port,
        )))
        .await
        {
            Ok(_) => {
                tracing::debug!(local_port = local_port, "SSH tunnel ready");
                return Ok(handle);
            }
            Err(_) => {
                if std::time::Instant::now() >= deadline {
                    // Timeout — kill the child before returning error
                    if let Some(ref mut child) = handle.child {
                        if let Err(e) = child.start_kill() {
                            tracing::debug!("Failed to kill SSH process on timeout: {}", e);
                        }
                    }
                    return Err(anyhow::anyhow!(
                        "SSH tunnel: timed out waiting for local port {} to become ready after 30s. \
                         Check SSH connectivity to [REDACTED] and server logs for details.",
                        local_port
                    ));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SshConfig;

    fn base_ssh() -> SshConfig {
        SshConfig {
            host: "bastion.example.com".to_string(),
            port: 22,
            user: "ubuntu".to_string(),
            private_key: None,
            known_hosts_check: "strict".to_string(),
            known_hosts_file: None,
        }
    }

    #[test]
    fn test_args_basic_structure() {
        let ssh = base_ssh();
        let args = build_ssh_args(&ssh, "db.internal", 3306, 54321).unwrap();

        // Must include -N flag
        assert!(args.contains(&"-N".to_string()), "must include -N");
        // Must include -L with correct forwarding spec
        let l_idx = args
            .iter()
            .position(|a| a == "-L")
            .expect("-L must be present");
        assert_eq!(args[l_idx + 1], "127.0.0.1:54321:db.internal:3306");
        // Must end with user@host
        assert_eq!(args.last().unwrap(), "ubuntu@bastion.example.com");
        // Must include BatchMode=yes
        assert!(args
            .windows(2)
            .any(|w| w[0] == "-o" && w[1] == "BatchMode=yes"));
        // Must include ConnectTimeout
        assert!(args
            .windows(2)
            .any(|w| w[0] == "-o" && w[1].starts_with("ConnectTimeout=")));
    }

    #[test]
    fn test_args_strict_maps_to_yes() {
        let ssh = base_ssh(); // known_hosts_check = "strict"
        let args = build_ssh_args(&ssh, "db", 3306, 12345).unwrap();
        assert!(args
            .windows(2)
            .any(|w| w[0] == "-o" && w[1] == "StrictHostKeyChecking=yes"));
    }

    #[test]
    fn test_args_accept_new() {
        let mut ssh = base_ssh();
        ssh.known_hosts_check = "accept-new".to_string();
        let args = build_ssh_args(&ssh, "db", 3306, 12345).unwrap();
        assert!(args
            .windows(2)
            .any(|w| w[0] == "-o" && w[1] == "StrictHostKeyChecking=accept-new"));
    }

    #[test]
    fn test_args_insecure_maps_to_no() {
        let mut ssh = base_ssh();
        ssh.known_hosts_check = "insecure".to_string();
        let args = build_ssh_args(&ssh, "db", 3306, 12345).unwrap();
        assert!(args
            .windows(2)
            .any(|w| w[0] == "-o" && w[1] == "StrictHostKeyChecking=no"));
    }

    #[test]
    fn test_args_with_private_key() {
        let mut ssh = base_ssh();
        ssh.private_key = Some("/home/user/.ssh/id_rsa".to_string());
        let args = build_ssh_args(&ssh, "db", 3306, 12345).unwrap();
        let i_idx = args
            .iter()
            .position(|a| a == "-i")
            .expect("-i must be present when private_key set");
        assert_eq!(args[i_idx + 1], "/home/user/.ssh/id_rsa");
    }

    #[test]
    fn test_args_without_private_key_has_no_dash_i() {
        let ssh = base_ssh(); // no private_key
        let args = build_ssh_args(&ssh, "db", 3306, 12345).unwrap();
        assert!(
            !args.contains(&"-i".to_string()),
            "-i must NOT be present when no private_key"
        );
    }

    #[test]
    fn test_args_custom_ssh_port() {
        let mut ssh = base_ssh();
        ssh.port = 2222;
        let args = build_ssh_args(&ssh, "db", 3306, 12345).unwrap();
        let p_idx = args
            .iter()
            .position(|a| a == "-p")
            .expect("-p must be present");
        assert_eq!(args[p_idx + 1], "2222");
    }

    #[test]
    fn test_args_known_hosts_file() {
        let mut ssh = base_ssh();
        ssh.known_hosts_file = Some("/etc/ssh/known_hosts".to_string());
        let args = build_ssh_args(&ssh, "db", 3306, 12345).unwrap();
        assert!(
            args.windows(2)
                .any(|w| { w[0] == "-o" && w[1] == "UserKnownHostsFile=/etc/ssh/known_hosts" }),
            "UserKnownHostsFile option must be present"
        );
    }

    #[test]
    fn test_args_no_known_hosts_file_by_default() {
        let ssh = base_ssh();
        let args = build_ssh_args(&ssh, "db", 3306, 12345).unwrap();
        assert!(!args.iter().any(|a| a.starts_with("UserKnownHostsFile=")));
    }

    #[test]
    fn test_args_user_at_host_is_last() {
        let ssh = base_ssh();
        let args = build_ssh_args(&ssh, "db", 3306, 12345).unwrap();
        assert_eq!(
            args.last().unwrap(),
            "ubuntu@bastion.example.com",
            "user@host must be the last argument"
        );
    }

    #[test]
    fn test_validate_rejects_empty_host() {
        assert!(validate_ssh_host("").is_err());
        let err = validate_ssh_host("").unwrap_err();
        assert!(
            err.to_string().contains("cannot be empty"),
            "error should mention empty: {:?}",
            err
        );
    }

    #[test]
    fn test_validate_rejects_null_byte_in_host() {
        let host = "evil\0host.com";
        let result = validate_ssh_host(host);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("null byte"),
            "error should mention null byte: {:?}",
            err
        );
    }

    #[test]
    fn test_validate_rejects_newline_in_host() {
        let host = "evil\nhost.com";
        let result = validate_ssh_host(host);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("control character"),
            "error should mention control character: {:?}",
            err
        );
    }

    #[test]
    fn test_validate_rejects_tab_in_host() {
        let host = "evil\thost.com";
        let result = validate_ssh_host(host);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("control character"),
            "error should mention control character: {:?}",
            err
        );
    }

    #[test]
    fn test_validate_rejects_carriage_return_in_host() {
        let host = "evil\rhost.com";
        let result = validate_ssh_host(host);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("control character"),
            "error should mention control character: {:?}",
            err
        );
    }

    #[test]
    fn test_validate_accepts_valid_hostname() {
        assert!(validate_ssh_host("bastion.example.com").is_ok());
    }

    #[test]
    fn test_validate_accepts_ipv4() {
        assert!(validate_ssh_host("10.0.0.1").is_ok());
    }

    #[test]
    fn test_validate_accepts_ipv6_bracketed() {
        assert!(validate_ssh_host("[::1]").is_ok());
    }

    #[test]
    fn test_validate_accepts_ipv6_full() {
        assert!(validate_ssh_host("[2001:db8::1]").is_ok());
    }

    #[test]
    fn test_validate_rejects_excessively_long_host() {
        let long_host = "a".repeat(256);
        let result = validate_ssh_host(&long_host);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("maximum length of 255"),
            "error should mention maximum length: {:?}",
            err
        );
    }

    #[test]
    fn test_validate_accepts_255_char_host() {
        // Exactly 255 characters should be allowed (RFC 1123 limit)
        let host = "a".repeat(255);
        assert!(validate_ssh_host(&host).is_ok());
    }

    #[test]
    fn test_args_db_host_and_port_in_forwarding() {
        let ssh = base_ssh();
        let args = build_ssh_args(&ssh, "10.0.0.5", 5432, 44444).unwrap();
        let l_idx = args.iter().position(|a| a == "-L").unwrap();
        assert_eq!(args[l_idx + 1], "127.0.0.1:44444:10.0.0.5:5432");
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::config::SshConfig;

    /// Build an SshConfig from environment variables.
    /// Returns None (causing test to skip) if MYSQL_SSH_TEST_HOST is not set.
    fn get_test_ssh_config() -> Option<(SshConfig, String, u16)> {
        let host = std::env::var("MYSQL_SSH_TEST_HOST").ok()?;
        let user = std::env::var("MYSQL_SSH_TEST_USER").unwrap_or_else(|_| "root".to_string());
        let private_key = std::env::var("MYSQL_SSH_TEST_KEY")
            .ok()
            .filter(|s| !s.is_empty());
        // Default: tunnel to the SSH server's own port 22 (always reachable if SSH works)
        let db_host =
            std::env::var("MYSQL_SSH_TEST_DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let db_port: u16 = std::env::var("MYSQL_SSH_TEST_DB_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(22);

        let ssh = SshConfig {
            host,
            port: 22,
            user,
            private_key,
            known_hosts_check: "accept-new".to_string(),
            known_hosts_file: None,
        };
        Some((ssh, db_host, db_port))
    }

    /// Verify that spawn_ssh_tunnel establishes a tunnel and the local port is connectable.
    #[tokio::test]
    async fn test_tunnel_establishes_and_port_connectable() {
        let Some((ssh, db_host, db_port)) = get_test_ssh_config() else {
            eprintln!("[skip] MYSQL_SSH_TEST_HOST not set — skipping SSH integration test");
            return;
        };
        eprintln!(
            "SSH integration test: bastion={}, target={}:{}",
            ssh.host, db_host, db_port
        );

        let result = spawn_ssh_tunnel(&ssh, &db_host, db_port).await;
        assert!(
            result.is_ok(),
            "spawn_ssh_tunnel should succeed: {:?}",
            result.err()
        );

        let handle = result.unwrap();
        let local_port = handle.local_port;
        eprintln!("Tunnel established on local port {}", local_port);

        // Verify the forwarded local port accepts TCP connections
        let connect = tokio::net::TcpStream::connect(std::net::SocketAddr::from((
            [127, 0, 0, 1],
            local_port,
        )))
        .await;
        assert!(
            connect.is_ok(),
            "local tunnel port {} should be connectable",
            local_port
        );
    }

    /// Verify that dropping a TunnelHandle kills the ssh process.
    /// After drop, the forwarded local port should no longer accept connections.
    #[tokio::test]
    async fn test_tunnel_closes_on_drop() {
        let Some((ssh, db_host, db_port)) = get_test_ssh_config() else {
            return; // skip
        };

        let handle = spawn_ssh_tunnel(&ssh, &db_host, db_port)
            .await
            .expect("tunnel should establish");
        let local_port = handle.local_port;

        // Drop the handle — this should kill the ssh child process
        drop(handle);
        // Give the OS a moment to clean up the port binding
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // The port should now be closed
        let reconnect = tokio::net::TcpStream::connect(std::net::SocketAddr::from((
            [127, 0, 0, 1],
            local_port,
        )))
        .await;
        assert!(
            reconnect.is_err(),
            "port {} should be closed after TunnelHandle drop",
            local_port
        );
    }

    /// Verify that close() provides clean async teardown equivalent to drop.
    #[tokio::test]
    async fn test_tunnel_close_method() {
        let Some((ssh, db_host, db_port)) = get_test_ssh_config() else {
            return; // skip
        };

        let handle = spawn_ssh_tunnel(&ssh, &db_host, db_port)
            .await
            .expect("tunnel should establish");
        let local_port = handle.local_port;

        handle.close().await.expect("close() should not error");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let reconnect = tokio::net::TcpStream::connect(std::net::SocketAddr::from((
            [127, 0, 0, 1],
            local_port,
        )))
        .await;
        assert!(
            reconnect.is_err(),
            "port {} should be closed after close()",
            local_port
        );
    }

    /// Verify that a bad/unreachable SSH host fails within the timeout, not hanging forever.
    #[tokio::test]
    async fn test_tunnel_fails_on_unreachable_host() {
        if std::env::var("MYSQL_SSH_TEST_HOST").is_err() {
            return; // only run this when SSH testing is active
        }
        let ssh = SshConfig {
            // 192.0.2.0/24 is TEST-NET-1 (RFC 5737): routable but no real host should answer
            host: "192.0.2.1".to_string(),
            port: 22,
            user: "nobody".to_string(),
            private_key: None,
            known_hosts_check: "insecure".to_string(),
            known_hosts_file: None,
        };
        let start = std::time::Instant::now();
        let result = spawn_ssh_tunnel(&ssh, "127.0.0.1", 3306).await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "tunnel to unreachable host should fail");
        // Should fail within 35 seconds (ConnectTimeout=10 + readiness poll deadline=30)
        assert!(
            elapsed.as_secs() < 35,
            "failure should occur within timeout, took {:?}",
            elapsed
        );
    }
}
