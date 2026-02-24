//! Query execution module.
//!
//! This module handles all aspects of executing SQL queries against MySQL databases:
//!
//! - `read`  — SELECT query execution, including automatic EXPLAIN analysis
//! - `write` — INSERT, UPDATE, DELETE, and DDL statement execution
//! - `explain` — EXPLAIN plan parsing and analysis (v1 and v2 formats)
//! - `retry` — Retry logic for transient connection failures
//!
//! The module also provides a generic [`with_timeout`] helper for applying
//! optional query timeouts to async operations.

pub mod explain;
pub(crate) mod explain_parse;
pub mod read;
pub mod retry;
pub mod write;

use std::future::Future;

/// Execute an async operation with an optional timeout.
///
/// If `timeout_ms > 0`, wraps the future in `tokio::time::timeout`.
/// If the timeout elapses, returns an error message that mentions the operation name
/// and suggests adjusting `MYSQL_QUERY_TIMEOUT`.
///
/// If `timeout_ms == 0`, runs the future without a timeout.
pub async fn with_timeout<T, F>(timeout_ms: u64, operation_name: &str, fut: F) -> anyhow::Result<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    if timeout_ms > 0 {
        tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), fut)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "{} timed out after {}ms. Set MYSQL_QUERY_TIMEOUT to adjust.",
                    operation_name,
                    timeout_ms
                )
            })?
    } else {
        fut.await
    }
}
