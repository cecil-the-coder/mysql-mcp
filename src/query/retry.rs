use anyhow::Result;
use std::future::Future;
use std::time::{Duration, Instant};

/// Lazily-initialized monotonic epoch used for jitter calculation.
/// Unlike `SystemTime`, `Instant` is guaranteed to be monotonic and won't
/// jump backwards due to NTP syncs or leap seconds.
static JITTER_EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

/// Returns a monotonically increasing nanosecond value suitable for jitter.
fn jitter_nanos() -> u32 {
    let epoch = JITTER_EPOCH.get_or_init(Instant::now);
    epoch.elapsed().subsec_nanos()
}

/// Error patterns that indicate a transient network failure.
/// These are safe to retry because the operation was never completed.
const TRANSIENT_ERROR_PATTERNS: &[&str] = &[
    "connection reset",
    "broken pipe",
    "timed out",
    "connection refused",
    "reset by peer",
    "connection closed",
    "unexpected eof",
];

/// Check if an error message indicates a transient network failure.
fn is_transient_error(error: &anyhow::Error) -> bool {
    let error_string = error.to_string().to_lowercase();
    TRANSIENT_ERROR_PATTERNS
        .iter()
        .any(|pattern| error_string.contains(pattern))
}

/// Execute an async operation with retry logic for transient network failures.
///
/// # Arguments
/// * `operation` - Async closure that performs the database operation
/// * `max_retries` - Maximum number of retry attempts (not including the initial attempt)
/// * `operation_name` - Human-readable name for logging purposes
///
/// # Behavior
/// - Executes the operation once initially
/// - On transient errors (connection reset, broken pipe, timeout, etc.), retries up to max_retries times
/// - Uses exponential backoff with ±25% jitter (min 50ms) between retries
/// - Logs retry attempts with tracing::warn
/// - Returns immediately on non-transient errors (syntax errors, permission errors, data errors)
pub async fn retry_on_transient_error<F, Fut, T>(
    operation: F,
    max_retries: u32,
    operation_name: &str,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    /// Maximum exponent for backoff: 2^10 = 1024x multiplier (~102.4s at 100ms base).
    const MAX_BACKOFF_SHIFT: u32 = 10;
    /// Base delay for exponential backoff in milliseconds.
    const BASE_BACKOFF_MS: u64 = 100;
    /// Minimum backoff duration in milliseconds (floor after jitter).
    const MIN_BACKOFF_MS: u64 = 50;

    let mut attempt = 0u32;
    let max_attempts = max_retries + 1; // initial attempt + retries

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempt += 1;

                // Check if we've exhausted all attempts
                if attempt >= max_attempts {
                    tracing::error!(
                        attempt = attempt,
                        max_attempts = max_attempts,
                        error = %e,
                        "{} failed after all retry attempts",
                        operation_name
                    );
                    return Err(e);
                }

                // Only retry on transient network errors
                if !is_transient_error(&e) {
                    tracing::debug!(
                        attempt = attempt,
                        error = %e,
                        "{} failed with non-transient error, not retrying",
                        operation_name
                    );
                    return Err(e);
                }

                // Calculate backoff: BASE_BACKOFF_MS * 2^(attempt-1), capped at MAX_BACKOFF_SHIFT
                // e.g. attempt=1→100ms, attempt=2→200ms, attempt=3→400ms, ...
                let shift = attempt.saturating_sub(1).min(MAX_BACKOFF_SHIFT);
                let base_ms = BASE_BACKOFF_MS.saturating_mul(1u64 << shift);

                // ±25% jitter using integer arithmetic to prevent thundering herd
                let nanos = jitter_nanos();
                let jitter_pct = nanos % 51; // 0..=50
                let jitter_range = base_ms / 2; // half of base = ±25%
                let jitter = jitter_range * jitter_pct as u64 / 50;
                let backoff_ms = (base_ms * 3 / 4 + jitter).max(MIN_BACKOFF_MS);
                let backoff = Duration::from_millis(backoff_ms);

                tracing::warn!(
                    attempt = attempt,
                    max_attempts = max_attempts,
                    backoff_ms = backoff_ms,
                    error = %e,
                    "{} failed with transient error, retrying",
                    operation_name
                );

                tokio::time::sleep(backoff).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_is_transient_error_connection_reset() {
        let err = anyhow::anyhow!("Connection reset by peer");
        assert!(is_transient_error(&err));
    }

    #[test]
    fn test_is_transient_error_broken_pipe() {
        let err = anyhow::anyhow!("broken pipe");
        assert!(is_transient_error(&err));
    }

    #[test]
    fn test_is_transient_error_timeout() {
        let err = anyhow::anyhow!("Operation timed out");
        assert!(is_transient_error(&err));
    }

    #[test]
    fn test_is_transient_error_connection_refused() {
        let err = anyhow::anyhow!("Connection refused");
        assert!(is_transient_error(&err));
    }

    #[test]
    fn test_is_transient_error_unexpected_eof() {
        let err = anyhow::anyhow!("unexpected eof");
        assert!(is_transient_error(&err));
    }

    #[test]
    fn test_is_transient_error_case_insensitive() {
        let err = anyhow::anyhow!("CONNECTION RESET BY PEER");
        assert!(is_transient_error(&err));
    }

    #[test]
    fn test_is_not_transient_error_syntax() {
        let err = anyhow::anyhow!("You have an error in your SQL syntax");
        assert!(!is_transient_error(&err));
    }

    #[test]
    fn test_is_not_transient_error_permission() {
        let err = anyhow::anyhow!("Access denied for user 'root'@'localhost'");
        assert!(!is_transient_error(&err));
    }

    #[test]
    fn test_is_not_transient_error_unknown_column() {
        let err = anyhow::anyhow!("Unknown column 'foo' in 'field list'");
        assert!(!is_transient_error(&err));
    }

    #[tokio::test]
    async fn test_retry_succeeds_on_second_attempt() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result = retry_on_transient_error(
            || {
                let attempts = attempts_clone.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    if current == 0 {
                        Err(anyhow::anyhow!("connection reset by peer"))
                    } else {
                        Ok("success")
                    }
                }
            },
            2,
            "test_operation",
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_retry_fails_after_max_attempts() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result: Result<(), _> = retry_on_transient_error(
            || {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(anyhow::anyhow!("connection reset by peer"))
                }
            },
            2,
            "test_operation",
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // initial + 2 retries
    }

    #[tokio::test]
    async fn test_no_retry_on_non_transient_error() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result: Result<(), _> = retry_on_transient_error(
            || {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(anyhow::anyhow!("syntax error"))
                }
            },
            2,
            "test_operation",
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1); // only initial attempt
    }

    #[tokio::test]
    async fn test_retry_with_zero_max_retries() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result: Result<(), _> = retry_on_transient_error(
            || {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(anyhow::anyhow!("connection reset"))
                }
            },
            0,
            "test_operation",
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1); // only initial attempt
    }
}
