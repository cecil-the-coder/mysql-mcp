//! PostgreSQL-specific backend implementation.
//!
//! Implements `Backend` and `PoolOps` traits for PostgreSQL databases.
//! All PostgreSQL-specific code (pool creation, type serialization,
//! information_schema queries, EXPLAIN execution) lives here.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::{Column, Row, TypeInfo, ValueRef};
use std::str::FromStr;
use std::time::Duration;

use super::{
    Backend, BackendKind, ExecuteResult, PoolHandle, PoolOps, RowData, SessionConnectParams,
};
use crate::config::{Config, SecurityConfig};
use crate::query::explain::ExplainResult;
use crate::query::explain_postgres::parse_postgres_explain;
use crate::query::with_timeout;
use crate::schema::{ColumnInfo, IndexDef, TableInfo};
use crate::tunnel::TunnelHandle;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of prepared statements cached per connection.
const STATEMENT_CACHE_CAPACITY: usize = 100;

/// Idle connection lifetime before it is closed and removed from the pool.
const POOL_IDLE_TIMEOUT_SECS: u64 = 300;

/// Maximum lifetime of any pooled connection before it is recycled.
const POOL_MAX_LIFETIME_SECS: u64 = 1800;

/// Binary columns with invalid UTF-8 are hex-encoded. Cap the output at 512 KB.
const MAX_BINARY_DISPLAY_BYTES: usize = 512 * 1024;

// ---------------------------------------------------------------------------
// PgPoolWrapper — wraps PgPool, implements PoolOps
// ---------------------------------------------------------------------------

/// Internal wrapper that holds a `PgPool` and implements `PoolOps`.
/// This is the concrete type behind `PoolHandle` for the PostgreSQL backend.
#[derive(Clone)]
pub(crate) struct PgPoolWrapper {
    pool: sqlx::PgPool,
}

impl PgPoolWrapper {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PoolOps for PgPoolWrapper {
    async fn fetch_all(&self, sql: &str) -> Result<Vec<RowData>> {
        let rows = sqlx::query(sql).fetch_all(&self.pool).await?;
        let mut warnings = Vec::new();
        let result: Vec<RowData> = rows
            .iter()
            .map(|row| pg_row_to_row_data(row, &mut warnings))
            .collect();
        for w in warnings {
            tracing::debug!("{}", w);
        }
        Ok(result)
    }

    async fn execute(&self, sql: &str) -> Result<ExecuteResult> {
        let result: sqlx::postgres::PgQueryResult = sqlx::query(sql).execute(&self.pool).await?;
        Ok(ExecuteResult {
            rows_affected: result.rows_affected(),
            // PostgreSQL does not have last_insert_id; use RETURNING instead.
            last_insert_id: None,
        })
    }

    async fn execute_in_transaction(&self, sql: &str) -> Result<ExecuteResult> {
        let mut tx = self.pool.begin().await?;
        let result: sqlx::postgres::PgQueryResult = sqlx::query(sql).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(ExecuteResult {
            rows_affected: result.rows_affected(),
            last_insert_id: None,
        })
    }

    async fn close(&self) {
        self.pool.close().await;
    }

    fn clone_box(&self) -> Box<dyn PoolOps> {
        Box::new(self.clone())
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL type-aware row serialization
// ---------------------------------------------------------------------------
// PostgreSQL numeric binary format decoder
// ---------------------------------------------------------------------------

/// Decode a PostgreSQL `numeric` value from its binary wire format into a
/// decimal string. The binary format is:
///
///   - `ndigits: i16` — number of base-10000 digits
///   - `weight: i16`  — exponent of the first digit (digits[0] * 10000^weight)
///   - `sign: u16`    — 0x0000 = positive, 0x4000 = negative, 0xC000 = NaN
///   - `dscale: i16`  — number of decimal digits after the decimal point
///   - `digits: [i16; ndigits]` — base-10000 digits, most significant first
fn decode_pg_numeric_binary(raw: &[u8]) -> String {
    if raw.len() < 8 {
        return "0".to_string();
    }
    let ndigits = i16::from_be_bytes([raw[0], raw[1]]) as usize;
    let weight = i16::from_be_bytes([raw[2], raw[3]]);
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = i16::from_be_bytes([raw[6], raw[7]]) as usize;

    if sign == 0xC000 {
        return "NaN".to_string();
    }
    if ndigits == 0 {
        return "0".to_string();
    }

    let expected_len = 8 + ndigits * 2;
    if raw.len() < expected_len {
        return "0".to_string();
    }

    // Extract base-10000 digits
    let mut digits = Vec::with_capacity(ndigits);
    for i in 0..ndigits {
        let offset = 8 + i * 2;
        digits.push(i16::from_be_bytes([raw[offset], raw[offset + 1]]) as i64);
    }

    // The integer part is (weight + 1) base-10000 digits, each contributing
    // up to 4 decimal digits. The remaining digits are fractional.
    // weight >= 0 means some digits are integer part; weight < 0 means all
    // digits are fractional (number < 1).
    let integer_digit_count = if weight >= 0 {
        (weight as usize) + 1
    } else {
        0
    };

    // Build the integer part: concatenate base-10000 digits with zero-padding.
    let mut int_str = String::new();
    for (i, &d) in digits.iter().enumerate() {
        if i >= integer_digit_count {
            break;
        }
        if i == 0 {
            int_str.push_str(&d.to_string()); // first digit: no leading zeros
        } else {
            int_str.push_str(&format!("{:04}", d)); // subsequent: pad to 4 digits
        }
    }

    // Build the fractional part: concatenate remaining base-10000 digits,
    // each padded to 4 decimal digits, then truncate to dscale.
    let mut frac_str = String::new();
    for (i, &d) in digits.iter().enumerate() {
        if i < integer_digit_count {
            continue;
        }
        frac_str.push_str(&format!("{:04}", d));
    }

    // Truncate or pad fractional part to match dscale.
    if frac_str.len() > dscale {
        frac_str.truncate(dscale);
    } else {
        while frac_str.len() < dscale {
            frac_str.push('0');
        }
    }

    // Handle negative weight (all digits are fractional, pad integer with zeros).
    if weight < 0 {
        int_str = "0".to_string();
        // Prepend zeros to frac_str to account for negative weight
        let total_frac_needed = (-weight as usize) * 4;
        while frac_str.len() < total_frac_needed {
            frac_str.insert(0, '0');
        }
    }

    // Handle the case where integer_digit_count > ndigits (pad integer with zeros).
    while digits.len() < integer_digit_count {
        // Missing integer digits are zero.
        if int_str.is_empty() {
            int_str = "0".to_string();
        }
        digits.push(0);
    }

    let mut result = String::new();
    let negative = sign == 0x4000;
    if negative {
        result.push('-');
    }

    if int_str.is_empty() {
        result.push('0');
    } else {
        result.push_str(&int_str);
    }

    if dscale > 0 && !frac_str.is_empty() {
        result.push('.');
        result.push_str(&frac_str);
    }

    // If result is empty, it's zero
    if result.is_empty() || result == "-" {
        return "0".to_string();
    }

    result
}

// ---------------------------------------------------------------------------

/// Convert a PostgreSQL row to `RowData`, performing type-aware serialization.
fn pg_row_to_row_data(row: &sqlx::postgres::PgRow, warnings: &mut Vec<String>) -> RowData {
    let mut columns = Vec::with_capacity(row.columns().len());
    for (i, col) in row.columns().iter().enumerate() {
        let value = pg_column_to_json(row, i, col, warnings);
        columns.push((col.name().to_string(), value));
    }
    RowData { columns }
}

/// Convert a single PostgreSQL column value to JSON based on its type name.
fn pg_column_to_json(
    row: &sqlx::postgres::PgRow,
    idx: usize,
    col: &sqlx::postgres::PgColumn,
    warnings: &mut Vec<String>,
) -> Value {
    let type_name = col.type_info().name();
    // PostgreSQL type names from sqlx may be uppercase (e.g. "INT4", "BOOL",
    // "VARCHAR") depending on the protocol mode. Normalize to lowercase so
    // that match arms work regardless of casing.
    let type_name_lower = type_name.to_lowercase();

    match type_name_lower.as_str() {
        // Boolean
        "bool" | "boolean" => {
            if let Ok(v) = row.try_get::<Option<bool>, _>(idx) {
                return v.map(Value::Bool).unwrap_or(Value::Null);
            }
        }

        // Small integers
        "int2" | "smallint" | "int2vector" => {
            if let Ok(v) = row.try_get::<Option<i16>, _>(idx) {
                return v
                    .map(|n| Value::Number((n as i64).into()))
                    .unwrap_or(Value::Null);
            }
        }

        // Regular integers
        "int4" | "integer" | "serial" => {
            if let Ok(v) = row.try_get::<Option<i32>, _>(idx) {
                return v
                    .map(|n| Value::Number((n as i64).into()))
                    .unwrap_or(Value::Null);
            }
        }

        // Big integers — use string for values > 2^53 to preserve precision
        "int8" | "bigint" | "bigserial" => {
            if let Ok(v) = row.try_get::<Option<i64>, _>(idx) {
                return v
                    .map(|n| {
                        if !(-9_007_199_254_740_992i64..=9_007_199_254_740_992i64).contains(&n) {
                            Value::String(n.to_string())
                        } else {
                            Value::Number(n.into())
                        }
                    })
                    .unwrap_or(Value::Null);
            }
        }

        // Float types
        "float4" | "real" => {
            if let Ok(v) = row.try_get::<Option<f32>, _>(idx) {
                return v
                    .map(|n| {
                        serde_json::Number::from_f64(n as f64)
                            .map(Value::Number)
                            .unwrap_or_else(|| Value::String(n.to_string()))
                    })
                    .unwrap_or(Value::Null);
            }
        }
        "float8" | "double precision" => {
            if let Ok(v) = row.try_get::<Option<f64>, _>(idx) {
                return v
                    .map(|n| {
                        serde_json::Number::from_f64(n)
                            .map(Value::Number)
                            .unwrap_or_else(|| {
                                warnings.push(format!(
                                    "Column '{}' contains NaN/Infinity value converted to NULL",
                                    col.name()
                                ));
                                Value::Null
                            })
                    })
                    .unwrap_or(Value::Null);
            }
        }

        // Numeric/Decimal — always as string to preserve precision.
        // In binary wire format, sqlx cannot decode numeric as String directly
        // without a decimal crate. Use raw value access to decode manually.
        "numeric" | "decimal" | "money" => {
            // Try text format first (works when PG sends in text mode)
            if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
            // Try f64 (sqlx can decode binary numeric to f64 in some configs)
            if let Ok(v) = row.try_get::<Option<f64>, _>(idx) {
                return v
                    .map(|n| {
                        Value::String(if n.fract() == 0.0 && n.abs() < 1e15 {
                            format!("{:.1}", n)
                        } else {
                            n.to_string()
                        })
                    })
                    .unwrap_or(Value::Null);
            }
            // Binary format fallback: use try_get_raw to access raw bytes.
            if let Ok(raw) = row.try_get_raw(idx) {
                if raw.is_null() {
                    return Value::Null;
                }
                // Decode the raw bytes as the PG numeric binary format:
                //   ndigits: i16, weight: i16, sign: u16, dscale: i16, digits: [i16; ndigits]
                if let Ok(bytes) = raw.as_bytes() {
                    if !bytes.is_empty() {
                        return Value::String(decode_pg_numeric_binary(bytes));
                    }
                } else {
                    eprintln!("DEBUG numeric: as_bytes failed: {:?}", raw.as_bytes().err());
                }
            } else {
                eprintln!("DEBUG numeric: try_get_raw failed");
            }
        }

        // Character types
        "varchar" | "text" | "bpchar" | "char" | "name" | "citext" | "character varying"
        | "character" => {
            if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        // Binary
        "bytea" => {
            if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
                return match v {
                    None => Value::Null,
                    Some(b) => {
                        let total = b.len();
                        let display = &b[..total.min(MAX_BINARY_DISPLAY_BYTES)];
                        let mut hex = String::with_capacity(display.len() * 2 + 2);
                        hex.push_str("\\x");
                        for byte in display {
                            use std::fmt::Write as _;
                            let _ = write!(hex, "{:02x}", byte);
                        }
                        if total > MAX_BINARY_DISPLAY_BYTES {
                            warnings.push(format!(
                                "Binary column '{}' truncated: {} bytes total, displayed first {} bytes as hex",
                                col.name(),
                                total,
                                MAX_BINARY_DISPLAY_BYTES
                            ));
                        }
                        Value::String(hex)
                    }
                };
            }
        }

        // JSON types
        "json" | "jsonb" => {
            // Try getting as serde_json::Value directly (sqlx supports this for
            // json/jsonb types via its type system). This handles both text and
            // binary wire formats transparently.
            if let Ok(Some(v)) = row.try_get::<Option<serde_json::Value>, _>(idx) {
                return v;
            }
            // Fallback: try typed String first (text format), then raw bytes.
            if let Ok(Some(s)) = row.try_get::<Option<String>, _>(idx) {
                match serde_json::from_str::<Value>(&s) {
                    Ok(v) => return v,
                    Err(e) => {
                        tracing::warn!(
                            "JSON column could not be parsed (returning as string): {}",
                            e
                        );
                        return Value::String(s);
                    }
                }
            }
            // Binary jsonb: version byte (0x01) followed by JSON text
            if let Ok(Some(bytes)) = row.try_get::<Option<Vec<u8>>, _>(idx) {
                let s = if type_name_lower == "jsonb" && !bytes.is_empty() && bytes[0] == 1 {
                    // Strip the jsonb version byte
                    String::from_utf8_lossy(&bytes[1..]).to_string()
                } else {
                    match String::from_utf8(bytes) {
                        Ok(s) => s,
                        Err(_) => return Value::Null,
                    }
                };
                match serde_json::from_str::<Value>(&s) {
                    Ok(v) => return v,
                    Err(e) => {
                        tracing::warn!(
                            "JSON/JSONB column could not be parsed (returning as string): {}",
                            e
                        );
                        return Value::String(s);
                    }
                }
            }
            return Value::Null;
        }

        // UUID — keep as string, don't import uuid crate
        "uuid" => {
            if let Ok(v) = row.try_get_unchecked::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        // Temporal types
        "timestamp" | "timestamp without time zone" => {
            if let Ok(v) = row.try_get_unchecked::<Option<chrono::NaiveDateTime>, _>(idx) {
                return v
                    .map(|dt| Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
                    .unwrap_or(Value::Null);
            }
        }
        "timestamptz" | "timestamp with time zone" => {
            if let Ok(v) = row.try_get_unchecked::<Option<chrono::DateTime<chrono::Utc>>, _>(idx) {
                return v
                    .map(|dt| Value::String(dt.format("%Y-%m-%d %H:%M:%S%:z").to_string()))
                    .unwrap_or(Value::Null);
            }
        }
        "date" => {
            if let Ok(v) = row.try_get_unchecked::<Option<chrono::NaiveDate>, _>(idx) {
                return v
                    .map(|d| Value::String(d.format("%Y-%m-%d").to_string()))
                    .unwrap_or(Value::Null);
            }
        }
        "time" | "time without time zone" => {
            if let Ok(v) = row.try_get_unchecked::<Option<chrono::NaiveTime>, _>(idx) {
                return v
                    .map(|t| Value::String(t.format("%H:%M:%S").to_string()))
                    .unwrap_or(Value::Null);
            }
        }
        "timetz" | "time with time zone" => {
            // PG time with time zone — try various decode approaches.
            if let Ok(v) = row.try_get::<Option<chrono::NaiveTime>, _>(idx) {
                return v
                    .map(|t| Value::String(t.format("%H:%M:%S").to_string()))
                    .unwrap_or(Value::Null);
            }
            // Try as a formatted string via unchecked (text format)
            if let Ok(v) = row.try_get_unchecked::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
            // Binary fallback: decode raw bytes manually.
            // timetz binary format: 8 bytes (int64 microseconds since midnight) + 4 bytes (int32 timezone offset in seconds)
            if let Ok(raw) = row.try_get_raw(idx) {
                if let Ok(bytes) = raw.as_bytes() {
                    if bytes.len() >= 8 {
                        let micros = i64::from_be_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7],
                        ]);
                        let total_secs = micros / 1_000_000;
                        let secs = total_secs % 86400;
                        let hours = (secs / 3600) as u32;
                        let minutes = ((secs % 3600) / 60) as u32;
                        let seconds = (secs % 60) as u32;
                        return Value::String(format!(
                            "{:02}:{:02}:{:02}",
                            hours, minutes, seconds
                        ));
                    }
                }
            }
        }

        // Array types — store as JSON arrays.
        // PG reports array type names using bracket notation (e.g. "int4[]")
        // or the internal underscore prefix (e.g. "_int4"). Match both.
        "_bool" | "bool[]" | "boolean[]" => {
            if let Ok(v) = row.try_get::<Option<Vec<bool>>, _>(idx) {
                return v
                    .map(|arr| Value::Array(arr.into_iter().map(Value::Bool).collect()))
                    .unwrap_or(Value::Null);
            }
        }
        "_int2" | "_smallint" | "int2[]" | "smallint[]" | "smallserial[]" => {
            if let Ok(v) = row.try_get::<Option<Vec<i16>>, _>(idx) {
                return v
                    .map(|arr| {
                        Value::Array(
                            arr.into_iter()
                                .map(|n| Value::Number((n as i64).into()))
                                .collect(),
                        )
                    })
                    .unwrap_or(Value::Null);
            }
        }
        "_int4" | "_integer" | "int4[]" | "integer[]" | "serial[]" => {
            if let Ok(v) = row.try_get::<Option<Vec<i32>>, _>(idx) {
                return v
                    .map(|arr| {
                        Value::Array(
                            arr.into_iter()
                                .map(|n| Value::Number((n as i64).into()))
                                .collect(),
                        )
                    })
                    .unwrap_or(Value::Null);
            }
        }
        "_int8" | "_bigint" | "int8[]" | "bigint[]" | "bigserial[]" => {
            if let Ok(v) = row.try_get::<Option<Vec<i64>>, _>(idx) {
                return v
                    .map(|arr| {
                        Value::Array(
                            arr.into_iter()
                                .map(|n| {
                                    if !(-9_007_199_254_740_992i64..=9_007_199_254_740_992i64)
                                        .contains(&n)
                                    {
                                        Value::String(n.to_string())
                                    } else {
                                        Value::Number(n.into())
                                    }
                                })
                                .collect(),
                        )
                    })
                    .unwrap_or(Value::Null);
            }
        }
        "_float4" | "float4[]" | "real[]" => {
            if let Ok(v) = row.try_get::<Option<Vec<f32>>, _>(idx) {
                return v
                    .map(|arr| {
                        Value::Array(
                            arr.into_iter()
                                .map(|n| {
                                    serde_json::Number::from_f64(n as f64)
                                        .map(Value::Number)
                                        .unwrap_or(Value::String(n.to_string()))
                                })
                                .collect(),
                        )
                    })
                    .unwrap_or(Value::Null);
            }
        }
        "_float8" | "float8[]" | "double precision[]" => {
            if let Ok(v) = row.try_get::<Option<Vec<f64>>, _>(idx) {
                return v
                    .map(|arr| {
                        Value::Array(
                            arr.into_iter()
                                .map(|n| {
                                    serde_json::Number::from_f64(n)
                                        .map(Value::Number)
                                        .unwrap_or(Value::Null)
                                })
                                .collect(),
                        )
                    })
                    .unwrap_or(Value::Null);
            }
        }
        "_text"
        | "_varchar"
        | "_bpchar"
        | "_char"
        | "_name"
        | "_citext"
        | "text[]"
        | "varchar[]"
        | "character varying[]"
        | "bpchar[]"
        | "char[]"
        | "character[]"
        | "name[]"
        | "citext[]" => {
            if let Ok(v) = row.try_get::<Option<Vec<String>>, _>(idx) {
                return v
                    .map(|arr| Value::Array(arr.into_iter().map(Value::String).collect()))
                    .unwrap_or(Value::Null);
            }
        }
        "_numeric" | "_decimal" | "numeric[]" | "decimal[]" | "money[]" => {
            if let Ok(v) = row.try_get_unchecked::<Option<Vec<String>>, _>(idx) {
                return v
                    .map(|arr| Value::Array(arr.into_iter().map(Value::String).collect()))
                    .unwrap_or(Value::Null);
            }
        }
        "_json" | "_jsonb" | "json[]" | "jsonb[]" => {
            if let Ok(v) = row.try_get_unchecked::<Option<Vec<String>>, _>(idx) {
                return v
                    .map(|arr| {
                        Value::Array(
                            arr.into_iter()
                                .filter_map(|s| serde_json::from_str(&s).ok())
                                .collect(),
                        )
                    })
                    .unwrap_or(Value::Null);
            }
        }
        "_uuid" | "uuid[]" => {
            if let Ok(v) = row.try_get_unchecked::<Option<Vec<String>>, _>(idx) {
                return v
                    .map(|arr| Value::Array(arr.into_iter().map(Value::String).collect()))
                    .unwrap_or(Value::Null);
            }
        }

        // OID / system types — PG uses i32 for oid in sqlx
        "oid" | "regclass" | "regtype" | "regproc" => {
            if let Ok(v) = row.try_get::<Option<i32>, _>(idx) {
                return v
                    .map(|n| {
                        if n >= 0 {
                            Value::Number((n as u64).into())
                        } else {
                            Value::String(n.to_string())
                        }
                    })
                    .unwrap_or(Value::Null);
            }
        }

        // inet / cidr / macaddr
        "inet" | "cidr" | "macaddr" | "macaddr8" => {
            if let Ok(v) = row.try_get_unchecked::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        // bit / varbit
        "bit" | "varbit" | "bit varying" => {
            if let Ok(v) = row.try_get_unchecked::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        // interval
        "interval" => {
            if let Ok(v) = row.try_get_unchecked::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        // tsvector / tsquery (full-text search)
        "tsvector" | "tsquery" => {
            if let Ok(v) = row.try_get_unchecked::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        // point / line / lseg / box / path / polygon / circle (geometric)
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            if let Ok(v) = row.try_get_unchecked::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        // pg_lsn (log sequence number)
        "pg_lsn" => {
            if let Ok(v) = row.try_get_unchecked::<Option<String>, _>(idx) {
                return v.map(Value::String).unwrap_or(Value::Null);
            }
        }

        _ => {
            // Unknown type — try string first, then bytes
        }
    }

    // Fallback: try string (unchecked — unknown type may not be TEXT)
    if let Ok(v) = row.try_get_unchecked::<Option<String>, _>(idx) {
        return v.map(Value::String).unwrap_or(Value::Null);
    }
    // Fallback: try bytes as hex
    if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
        return match v {
            None => Value::Null,
            Some(b) => String::from_utf8(b).map(Value::String).unwrap_or_else(|e| {
                let bytes = e.into_bytes();
                let total = bytes.len();
                let display = &bytes[..total.min(MAX_BINARY_DISPLAY_BYTES)];
                let mut hex = String::with_capacity(display.len() * 2 + 2);
                hex.push_str("\\x");
                for byte in display {
                    use std::fmt::Write as _;
                    let _ = write!(hex, "{:02x}", byte);
                }
                if total > MAX_BINARY_DISPLAY_BYTES {
                    warnings.push(format!(
                        "Binary column '{}' (type '{}') truncated: {} bytes total, displayed first {} bytes as hex",
                        col.name(),
                        type_name,
                        total,
                        MAX_BINARY_DISPLAY_BYTES
                    ));
                }
                Value::String(hex)
            }),
        };
    }

    warnings.push(format!(
        "Column '{}' (type '{}') could not be decoded, returning NULL",
        col.name(),
        type_name
    ));
    Value::Null
}

// ---------------------------------------------------------------------------
// Helper: escape a PG identifier for double-quote quoting
// ---------------------------------------------------------------------------

/// Escape a PostgreSQL identifier for use in double-quote contexts.
/// Double-quotes are escaped by doubling them (`"id"` stays `"id"`, `"a""b"` for `a"b`).
pub(crate) fn escape_pg_identifier(name: &str) -> String {
    name.replace('"', "\"\"")
}

// ---------------------------------------------------------------------------
// SSL mode mapping
// ---------------------------------------------------------------------------

/// Map the three SSL flags to a `PgSslMode`.
pub(crate) fn determine_pg_ssl_mode(
    ssl: bool,
    accept_invalid: bool,
    has_ca: bool,
) -> sqlx::postgres::PgSslMode {
    match (ssl, accept_invalid, has_ca) {
        (false, _, _) => sqlx::postgres::PgSslMode::Disable,
        (true, true, _) => sqlx::postgres::PgSslMode::Require,
        (true, false, true) => sqlx::postgres::PgSslMode::VerifyCa,
        (true, false, false) => sqlx::postgres::PgSslMode::VerifyFull,
    }
}

// ---------------------------------------------------------------------------
// Pool creation helpers
// ---------------------------------------------------------------------------

/// Apply consistent pool sizing, timeouts, and lifetime settings.
async fn create_pool(
    opts: sqlx::postgres::PgConnectOptions,
    max_connections: u32,
    acquire_timeout_ms: u64,
) -> Result<sqlx::PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_millis(acquire_timeout_ms))
        .idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
        .max_lifetime(Duration::from_secs(POOL_MAX_LIFETIME_SECS))
        .connect_with(opts)
        .await?;
    Ok(pool)
}

/// Build PgConnectOptions from the application Config.
pub(crate) fn build_connect_options(config: &Config) -> Result<sqlx::postgres::PgConnectOptions> {
    let conn = &config.connection;

    if let Some(cs) = &conn.connection_string {
        // Accept both postgres:// and postgresql:// schemes
        if cs.starts_with("postgres://") || cs.starts_with("postgresql://") {
            let opts = sqlx::postgres::PgConnectOptions::from_str(cs)?;
            return Ok(opts);
        } else {
            let split_pos = if let Some(i) = cs.find("://") {
                i + 3
            } else {
                let mut pos = cs.len().min(32);
                while pos > 0 && !cs.is_char_boundary(pos) {
                    pos -= 1;
                }
                pos
            };
            anyhow::bail!(
                "connection_string must start with 'postgres://' or 'postgresql://', got: '{}'",
                &cs[..split_pos]
            );
        }
    }

    let ssl_mode = determine_pg_ssl_mode(
        config.security.ssl,
        config.security.ssl_accept_invalid_certs,
        config.security.ssl_ca.is_some(),
    );
    let mut opts = sqlx::postgres::PgConnectOptions::new()
        .host(&conn.host)
        .port(conn.port.unwrap_or(5432))
        .username(&conn.user)
        .password(&conn.password)
        .ssl_mode(ssl_mode);
    if let Some(db) = &conn.database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = &config.security.ssl_ca {
        opts = opts.ssl_root_cert(ca_path.as_str());
    }
    Ok(opts)
}

/// Build a PgPool from the application config.
pub(crate) async fn build_pool(config: &Config) -> Result<sqlx::PgPool> {
    let connect_options =
        build_connect_options(config)?.statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    create_pool(
        connect_options,
        config.pool.size,
        config.pool.connect_timeout_ms,
    )
    .await
}

/// Build a pool through an SSH tunnel.
#[allow(dead_code)]
pub(crate) async fn build_pool_tunneled(
    config: &Config,
    tunnel: &TunnelHandle,
) -> Result<sqlx::PgPool> {
    let ssl_mode = determine_pg_ssl_mode(
        config.security.ssl,
        config.security.ssl_accept_invalid_certs,
        config.security.ssl_ca.is_some(),
    );
    let mut opts = sqlx::postgres::PgConnectOptions::new()
        .host("127.0.0.1")
        .port(tunnel.local_port)
        .username(&config.connection.user)
        .password(&config.connection.password)
        .ssl_mode(ssl_mode)
        .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    if let Some(ref db) = config.connection.database {
        opts = opts.database(db);
    }
    if let Some(ref ca_path) = config.security.ssl_ca {
        opts = opts.ssl_root_cert(ca_path.as_str());
    }
    create_pool(opts, config.pool.size, config.pool.connect_timeout_ms).await
}

/// Build a small session pool from raw connection fields.
pub(crate) async fn build_session_pool_internal(
    params: &SessionConnectParams,
) -> Result<sqlx::PgPool> {
    let mut opts = sqlx::postgres::PgConnectOptions::new()
        .host(&params.host)
        .port(params.port)
        .username(&params.user)
        .password(&params.password)
        .ssl_mode(determine_pg_ssl_mode(
            params.ssl,
            params.ssl_accept_invalid_certs,
            params.ssl_ca.is_some(),
        ));
    if let Some(db) = &params.database {
        opts = opts.database(db);
    }
    if let Some(ca_path) = &params.ssl_ca {
        opts = opts.ssl_root_cert(ca_path.as_str());
    }
    create_pool(opts, 5, params.connect_timeout_ms).await
}

// ---------------------------------------------------------------------------
// RowData helper functions — extract values from RowData (backend-agnostic)
// ---------------------------------------------------------------------------

fn get_str(row: &RowData, col: &str) -> String {
    row.columns
        .iter()
        .find(|(name, _)| name == col)
        .map(|(_, v)| {
            if v.is_string() {
                v.as_str().unwrap_or_default().to_string()
            } else if v.is_null() {
                String::new()
            } else {
                v.to_string()
            }
        })
        .unwrap_or_default()
}

fn get_opt_str(row: &RowData, col: &str) -> Option<String> {
    row.columns
        .iter()
        .find(|(name, _)| name == col)
        .and_then(|(_, v)| v.as_str().map(String::from))
        .filter(|s| !s.is_empty())
}

fn get_opt_i64(row: &RowData, col: &str) -> Option<i64> {
    row.columns
        .iter()
        .find(|(name, _)| name == col)
        .and_then(|(_, v)| v.as_i64())
}

#[allow(dead_code)]
fn get_opt_f64(row: &RowData, col: &str) -> Option<f64> {
    row.columns
        .iter()
        .find(|(name, _)| name == col)
        .and_then(|(_, v)| v.as_f64())
}

fn get_opt_u64(row: &RowData, col: &str) -> Option<u64> {
    row.columns
        .iter()
        .find(|(name, _)| name == col)
        .and_then(|(_, v)| v.as_u64())
}

// ---------------------------------------------------------------------------
// PgBackend — implements Backend trait
// ---------------------------------------------------------------------------

/// PostgreSQL backend implementation.
pub struct PgBackend;

impl PgBackend {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PgBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Backend for PgBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::Postgres
    }

    fn backend_name(&self) -> &str {
        "PostgreSQL"
    }

    fn default_port(&self) -> u16 {
        5432
    }

    fn sql_dialect(&self) -> &str {
        "PostgreSQL"
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", escape_pg_identifier(name))
    }

    fn supports_ssl(&self) -> bool {
        true
    }

    fn supports_ssh_tunnel(&self) -> bool {
        true
    }

    fn supports_network(&self) -> bool {
        true
    }

    async fn create_pool(&self, config: &Config, _security: &SecurityConfig) -> Result<PoolHandle> {
        let pool = build_pool(config).await?;
        Ok(PoolHandle::new(Box::new(PgPoolWrapper::new(pool))))
    }

    async fn create_session_pool(&self, params: &SessionConnectParams) -> Result<PoolHandle> {
        let pool = build_session_pool_internal(params).await?;
        Ok(PoolHandle::new(Box::new(PgPoolWrapper::new(pool))))
    }

    async fn create_tunnel_pool(
        &self,
        _tunnel: &TunnelHandle,
        _params: &SessionConnectParams,
    ) -> Result<(PoolHandle, TunnelHandle)> {
        anyhow::bail!("PostgreSQL tunnel pool creation must go through the session store")
    }

    async fn fetch_tables(
        &self,
        pool: &PoolHandle,
        database: Option<&str>,
    ) -> Result<Vec<TableInfo>> {
        // In PostgreSQL, "database" corresponds to schema in PG terminology when
        // filtering. But we use "database" to mean the connected database and
        // "schema" to filter by schema name. When database is None, show all
        // user tables (excluding pg_catalog and information_schema).
        let sql = if let Some(schema) = database {
            format!(
                r#"SELECT
                    t.table_name AS name,
                    t.table_schema AS "schema",
                    COALESCE(c.reltuples::bigint, 0) AS row_count,
                    pg_total_relation_size(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name)) AS data_size_bytes,
                    NULL::text AS create_time,
                    NULL::text AS update_time
                FROM information_schema.tables t
                JOIN pg_class c ON c.relname = t.table_name
                JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
                WHERE t.table_schema = '{schema}'
                  AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_schema, t.table_name"#,
                schema = schema.replace('\'', "''"),
            )
        } else {
            r#"SELECT
                t.table_name AS name,
                t.table_schema AS "schema",
                COALESCE(c.reltuples::bigint, 0) AS row_count,
                pg_total_relation_size(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name)) AS data_size_bytes,
                NULL::text AS create_time,
                NULL::text AS update_time
            FROM information_schema.tables t
            JOIN pg_class c ON c.relname = t.table_name
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
              AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_schema, t.table_name"#
                .to_string()
        };
        let rows = pool.fetch_all(&sql).await?;

        let tables: Vec<TableInfo> = rows
            .iter()
            .map(|row| TableInfo {
                name: get_str(row, "name"),
                schema: get_str(row, "schema"),
                row_count: get_opt_i64(row, "row_count"),
                data_size_bytes: get_opt_i64(row, "data_size_bytes"),
                create_time: get_opt_str(row, "create_time"),
                update_time: get_opt_str(row, "update_time"),
            })
            .collect();

        Ok(tables)
    }

    async fn fetch_columns(
        &self,
        pool: &PoolHandle,
        table_name: &str,
        database: Option<&str>,
    ) -> Result<Vec<ColumnInfo>> {
        let schema_filter = match database {
            Some(s) => format!("= '{}'", s.replace('\'', "''")),
            None => "= current_schema()".to_string(),
        };
        let sql = format!(
            r#"SELECT
                column_name AS name,
                data_type AS data_type,
                CASE
                    WHEN character_maximum_length IS NOT NULL THEN
                        data_type || '(' || character_maximum_length || ')'
                    WHEN numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL AND numeric_scale > 0 THEN
                        data_type || '(' || numeric_precision || ',' || numeric_scale || ')'
                    WHEN numeric_precision IS NOT NULL THEN
                        data_type || '(' || numeric_precision || ')'
                    ELSE data_type
                END AS column_type,
                is_nullable AS is_nullable,
                column_default AS column_default,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                          AND tc.table_schema = kcu.table_schema
                        WHERE tc.table_schema {schema_filter}
                          AND tc.table_name = '{table}'
                          AND kcu.column_name = c.column_name
                          AND tc.constraint_type = 'PRIMARY KEY'
                    ) THEN 'PRI'
                    WHEN EXISTS (
                        SELECT 1 FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                          AND tc.table_schema = kcu.table_schema
                        WHERE tc.table_schema {schema_filter}
                          AND tc.table_name = '{table}'
                          AND kcu.column_name = c.column_name
                          AND tc.constraint_type = 'UNIQUE'
                    ) THEN 'UNI'
                    WHEN EXISTS (
                        SELECT 1 FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                          AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.referential_constraints rc
                          ON rc.constraint_name = tc.constraint_name
                          AND rc.constraint_schema = tc.table_schema
                        WHERE tc.table_schema {schema_filter}
                          AND tc.table_name = '{table}'
                          AND kcu.column_name = c.column_name
                    ) THEN 'MUL'
                    ELSE NULL
                END AS column_key,
                CASE
                    WHEN column_default LIKE 'nextval(%' THEN 'auto_increment'
                    ELSE NULL
                END AS extra
            FROM information_schema.columns c
            WHERE table_schema {schema_filter}
              AND table_name = '{table}'
            ORDER BY ordinal_position"#,
            schema_filter = schema_filter,
            table = table_name.replace('\'', "''"),
        );
        let rows = pool.fetch_all(&sql).await?;

        let columns: Vec<ColumnInfo> = rows
            .iter()
            .map(|row| {
                let nullable_str = get_str(row, "is_nullable");
                ColumnInfo {
                    name: get_str(row, "name"),
                    data_type: get_str(row, "data_type"),
                    column_type: get_str(row, "column_type"),
                    is_nullable: nullable_str == "YES",
                    column_default: get_opt_str(row, "column_default"),
                    column_key: get_opt_str(row, "column_key"),
                    extra: get_opt_str(row, "extra"),
                }
            })
            .collect();

        Ok(columns)
    }

    async fn fetch_indexed_columns(
        &self,
        pool: &PoolHandle,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<String>> {
        let schema_filter = match database {
            Some(s) => format!("= '{}'", s.replace('\'', "''")),
            None => "= current_schema()".to_string(),
        };
        let sql = format!(
            r#"SELECT DISTINCT a.attname AS column_name
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
            WHERE n.nspname {schema_filter}
              AND c.relname = '{table}'
              AND NOT i.indisprimary
            ORDER BY a.attname"#,
            schema_filter = schema_filter,
            table = table.replace('\'', "''"),
        );
        let rows = pool.fetch_all(&sql).await?;

        let cols: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                let col_name = get_str(row, "column_name");
                if col_name.is_empty() {
                    None
                } else {
                    Some(col_name)
                }
            })
            .collect();
        Ok(cols)
    }

    async fn fetch_composite_indexes(
        &self,
        pool: &PoolHandle,
        table: &str,
        database: Option<&str>,
    ) -> Result<Vec<IndexDef>> {
        let schema_filter = match database {
            Some(s) => format!("= '{}'", s.replace('\'', "''")),
            None => "= current_schema()".to_string(),
        };
        let sql = format!(
            r#"SELECT
                ic.relname AS index_name,
                NOT i.indisunique AS non_unique,
                a.attnum,
                a.attname AS column_name,
                am.amname AS index_type
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_class ic ON ic.oid = i.indexrelid
            JOIN pg_am am ON am.oid = ic.relam
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
            WHERE n.nspname {schema_filter}
              AND c.relname = '{table}'
            ORDER BY ic.relname, array_position(i.indkey, a.attnum)"#,
            schema_filter = schema_filter,
            table = table.replace('\'', "''"),
        );
        let rows = pool.fetch_all(&sql).await?;

        let mut index_map: std::collections::BTreeMap<String, IndexDef> =
            std::collections::BTreeMap::new();
        for row in &rows {
            let name = get_str(row, "index_name");
            let non_unique_str = get_str(row, "non_unique");
            let is_unique = non_unique_str != "t";
            let col = get_str(row, "column_name");
            let entry = index_map.entry(name.clone()).or_insert_with(|| IndexDef {
                name,
                unique: is_unique,
                columns: Vec::new(),
            });
            if !col.is_empty() {
                entry.columns.push(col);
            }
        }

        Ok(index_map.into_values().collect())
    }

    async fn fetch_schema_details(
        &self,
        pool: &PoolHandle,
        table_name: &str,
        database: Option<&str>,
        include_indexes: bool,
        include_fks: bool,
        include_size: bool,
    ) -> Result<serde_json::Value> {
        // Fetch columns
        let columns = self.fetch_columns(pool, table_name, database).await?;

        let schema_filter = match database {
            Some(s) => format!("= '{}'", s.replace('\'', "''")),
            None => "= current_schema()".to_string(),
        };
        let table = table_name.replace('\'', "''");

        // Indexes
        let indexes: serde_json::Value = if include_indexes {
            let sql = format!(
                r#"SELECT
                    ic.relname AS index_name,
                    NOT ix.indisunique AS non_unique,
                    am.amname AS index_type,
                    a.attname AS column_name,
                    NOT a.attnotnull AS nullable
                FROM pg_index ix
                JOIN pg_class tc ON tc.oid = ix.indrelid
                JOIN pg_namespace tn ON tn.oid = tc.relnamespace
                JOIN pg_class ic ON ic.oid = ix.indexrelid
                JOIN pg_am am ON am.oid = ic.relam
                JOIN pg_attribute a ON a.attrelid = tc.oid AND a.attnum = ANY(ix.indkey)
                WHERE tn.nspname {schema_filter}
                  AND tc.relname = '{table}'
                ORDER BY ic.relname, array_position(ix.indkey, a.attnum)"#,
                schema_filter = schema_filter,
                table = table,
            );
            let rows = pool.fetch_all(&sql).await?;

            let mut idx_map: std::collections::BTreeMap<String, serde_json::Value> =
                std::collections::BTreeMap::new();
            for row in &rows {
                let name: String = get_str(row, "index_name");
                let non_unique: i64 = get_str(row, "non_unique").parse().unwrap_or(0);
                let idx_type: String = get_str(row, "index_type");
                let col: String = get_str(row, "column_name");
                let nullable: String = get_str(row, "nullable");
                let entry = idx_map.entry(name.clone()).or_insert_with(|| {
                    serde_json::json!({
                        "name": name, "unique": non_unique == 0, "type": idx_type, "columns": [],
                    })
                });
                if let Some(cols) = entry.get_mut("columns").and_then(|v| v.as_array_mut()) {
                    cols.push(serde_json::json!({ "column": col, "nullable": nullable == "t" }));
                }
            }
            serde_json::Value::Array(idx_map.into_values().collect())
        } else {
            serde_json::Value::Null
        };

        // Foreign keys
        let foreign_keys: serde_json::Value = if include_fks {
            let sql = format!(
                r#"SELECT
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS referenced_table,
                    ccu.column_name AS referenced_column,
                    rc.update_rule AS update_rule,
                    rc.delete_rule AS delete_rule
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
                JOIN information_schema.referential_constraints rc
                  ON rc.constraint_name = tc.constraint_name
                  AND rc.constraint_schema = tc.table_schema
                WHERE tc.table_schema {schema_filter}
                  AND tc.table_name = '{table}'
                  AND tc.constraint_type = 'FOREIGN KEY'"#,
                schema_filter = schema_filter,
                table = table,
            );
            let rows = pool.fetch_all(&sql).await?;
            serde_json::Value::Array(
                rows.iter()
                    .map(|row| {
                        serde_json::json!({
                            "constraint":        get_str(row, "constraint_name"),
                            "column":            get_str(row, "column_name"),
                            "references_table":  get_str(row, "referenced_table"),
                            "references_column": get_str(row, "referenced_column"),
                            "on_update":         get_str(row, "update_rule"),
                            "on_delete":         get_str(row, "delete_rule"),
                        })
                    })
                    .collect(),
            )
        } else {
            serde_json::Value::Null
        };

        // Table size
        let size: serde_json::Value = if include_size {
            let sql = format!(
                r#"SELECT
                    COALESCE(c.reltuples::bigint, 0) AS estimated_rows,
                    pg_total_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) AS total_bytes,
                    pg_relation_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) AS data_bytes,
                    pg_indexes_size(quote_ident(n.nspname) || '.' || quote_ident(c.relname)) AS index_bytes
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname {schema_filter}
                  AND c.relname = '{table}'"#,
                schema_filter = schema_filter,
                table = table,
            );
            let rows = pool.fetch_all(&sql).await?;
            match rows.into_iter().next() {
                Some(row) => {
                    serde_json::json!({
                        "estimated_rows": get_opt_u64(&row, "estimated_rows"),
                        "data_bytes":     get_opt_u64(&row, "data_bytes"),
                        "index_bytes":    get_opt_u64(&row, "index_bytes"),
                        "total_bytes":    get_opt_u64(&row, "total_bytes"),
                    })
                }
                None => serde_json::Value::Null,
            }
        } else {
            serde_json::Value::Null
        };

        // Assemble result
        let cols_json: Vec<serde_json::Value> = columns
            .iter()
            .map(|c| {
                serde_json::json!({
                    "name": c.name, "type": c.column_type, "nullable": c.is_nullable,
                    "default": c.column_default, "key": c.column_key, "extra": c.extra,
                })
            })
            .collect();

        let mut result = serde_json::json!({ "table": table_name, "columns": cols_json });
        if !indexes.is_null() {
            result["indexes"] = indexes;
        }
        if !foreign_keys.is_null() {
            result["foreign_keys"] = foreign_keys;
        }
        if !size.is_null() {
            result["size"] = size;
        }
        Ok(result)
    }

    async fn fetch_server_info(
        &self,
        pool: &PoolHandle,
        security: &SecurityConfig,
    ) -> Result<serde_json::Value> {
        let sql =
            "SELECT version() AS pg_version, \
                    current_user AS current_user, \
                    current_database() AS current_database, \
                    (SELECT setting FROM pg_settings WHERE name = 'server_version') AS server_version, \
                    (SELECT setting FROM pg_settings WHERE name = 'server_encoding') AS server_encoding, \
                    (SELECT setting FROM pg_settings WHERE name = 'lc_messages') AS lc_messages, \
                    (SELECT setting FROM pg_settings WHERE name = 'TimeZone') AS time_zone, \
                    (SELECT setting FROM pg_settings WHERE name = 'max_connections') AS max_connections, \
                    (SELECT current_setting('is_superuser') = 'on') AS is_superuser";
        let rows = pool.fetch_all(sql).await?;
        if rows.is_empty() {
            anyhow::bail!("No response from server");
        }
        let row = &rows[0];

        let version = get_str(row, "pg_version");
        let user = get_str(row, "current_user");
        let db = get_opt_str(row, "current_database");
        let server_version = get_str(row, "server_version");
        let server_encoding = get_str(row, "server_encoding");
        let lc_messages = get_str(row, "lc_messages");
        let time_zone = get_str(row, "time_zone");
        let is_superuser_str = get_str(row, "is_superuser");
        let is_superuser = is_superuser_str == "t";

        let mut accessible_features = vec!["SELECT", "SHOW", "EXPLAIN"];
        for (enabled, name) in [
            (security.allow_insert, "INSERT"),
            (security.allow_update, "UPDATE"),
            (security.allow_delete, "DELETE"),
            (security.allow_ddl, "DDL (CREATE/ALTER/DROP)"),
        ] {
            if enabled {
                accessible_features.push(name);
            }
        }

        let info = json!({
            "pg_version": version,
            "current_user": user,
            "current_database": db,
            "server_version": server_version,
            "server_encoding": server_encoding,
            "lc_messages": lc_messages,
            "time_zone": time_zone,
            "is_superuser": is_superuser,
            "accessible_features": accessible_features,
        });

        let mut response = info;
        let warnings = security.security_warnings();
        if !warnings.is_empty() {
            response["security_warnings"] = json!(warnings);
        }

        Ok(response)
    }

    async fn run_explain(
        &self,
        pool: &PoolHandle,
        sql: &str,
        query_timeout_ms: u64,
    ) -> Result<ExplainResult> {
        let explain_sql = format!("EXPLAIN (FORMAT JSON) {}", sql);
        let explain_fut = async { pool.fetch_all(&explain_sql).await };

        let rows = with_timeout(query_timeout_ms, "EXPLAIN", explain_fut).await?;

        if rows.is_empty() {
            anyhow::bail!("EXPLAIN returned no rows");
        }

        let row = &rows[0];
        // The EXPLAIN column may be returned as a parsed JSON Value (when the
        // type-aware serializer recognises the "json" column type) or as a
        // raw string.  Handle both cases.
        let (_, col_value) = row
            .columns
            .first()
            .ok_or_else(|| anyhow::anyhow!("EXPLAIN row has no columns"))?;

        let v = if col_value.is_string() {
            let json_str = col_value.as_str().unwrap();
            serde_json::from_str(json_str).map_err(|e| {
                let location = format!("at line {}, column {}", e.line(), e.column());
                anyhow::anyhow!(
                    "Failed to parse PostgreSQL EXPLAIN JSON: {} {}",
                    e,
                    location
                )
            })?
        } else {
            // Already a parsed Value (array or object) from the json type handler.
            col_value.clone()
        };

        parse_postgres_explain(&v)
    }

    async fn fetch_list_tables(&self, pool: &PoolHandle, database: &str) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT table_name AS table_name FROM information_schema.tables \
             WHERE table_schema = '{}' AND table_type = 'BASE TABLE' \
             ORDER BY table_name",
            database.replace('\'', "''")
        );
        let rows = pool.fetch_all(&sql).await?;
        let tables: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                row.columns
                    .first()
                    .and_then(|(_, v)| v.as_str().map(String::from))
            })
            .collect();
        Ok(tables)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_determine_pg_ssl_mode_disabled() {
        assert_eq!(
            format!("{:?}", determine_pg_ssl_mode(false, false, false)),
            "Disable"
        );
        assert_eq!(
            format!("{:?}", determine_pg_ssl_mode(false, true, false)),
            "Disable"
        );
    }

    #[test]
    fn test_determine_pg_ssl_mode_require() {
        assert_eq!(
            format!("{:?}", determine_pg_ssl_mode(true, true, false)),
            "Require"
        );
        assert_eq!(
            format!("{:?}", determine_pg_ssl_mode(true, true, true)),
            "Require"
        );
    }

    #[test]
    fn test_determine_pg_ssl_mode_verify_ca() {
        assert_eq!(
            format!("{:?}", determine_pg_ssl_mode(true, false, true)),
            "VerifyCa"
        );
    }

    #[test]
    fn test_determine_pg_ssl_mode_verify_full() {
        assert_eq!(
            format!("{:?}", determine_pg_ssl_mode(true, false, false)),
            "VerifyFull"
        );
    }

    #[test]
    fn test_pg_backend_kind() {
        let backend = PgBackend::new();
        assert_eq!(backend.kind(), BackendKind::Postgres);
        assert_eq!(backend.backend_name(), "PostgreSQL");
        assert_eq!(backend.default_port(), 5432);
        assert!(backend.supports_ssl());
        assert!(backend.supports_ssh_tunnel());
        assert!(backend.supports_network());
    }

    #[test]
    fn test_pg_quote_identifier() {
        let backend = PgBackend::new();
        assert_eq!(backend.quote_identifier("id"), "\"id\"");
        assert_eq!(backend.quote_identifier("my table"), "\"my table\"");
        assert_eq!(
            backend.quote_identifier("col\"with\"quotes"),
            "\"col\"\"with\"\"quotes\""
        );
    }
}
