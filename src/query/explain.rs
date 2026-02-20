use anyhow::Result;
use sqlx::MySqlPool;
use serde_json::Value;

pub struct ExplainResult {
    pub full_table_scan: bool,
    pub index_used: Option<String>,
    pub rows_examined_estimate: u64,
    pub filtered_pct: f64,
    pub efficiency: f64,          // rows_returned / rows_examined (0.0–1.0, higher is better)
    pub extra_flags: Vec<String>, // "Using filesort", "Using temporary", etc.
    pub tier: String,             // "fast" | "slow" | "very_slow"
}

pub async fn run_explain(pool: &MySqlPool, sql: &str) -> Result<ExplainResult> {
    let explain_sql = format!("EXPLAIN FORMAT=JSON {}", sql);
    let row: sqlx::mysql::MySqlRow = sqlx::query(&explain_sql)
        .fetch_one(pool)
        .await?;

    // EXPLAIN FORMAT=JSON returns a single row with one column: the JSON string
    use sqlx::Row;
    let json_str: String = row.try_get(0)?;
    let v: Value = serde_json::from_str(&json_str)?;

    // Navigate the EXPLAIN JSON structure
    // Top level: { "query_block": { "select_id": 1, "table": { ... } } }
    let qb = &v["query_block"];
    let table = &qb["table"];

    let access_type = table["access_type"].as_str().unwrap_or("").to_string();
    let full_table_scan = access_type == "ALL";
    let index_used = table["key"].as_str().map(|s| s.to_string());
    let rows_examined_estimate = table["rows_examined_per_scan"]
        .as_u64()
        .or_else(|| table["rows_produced_per_join"].as_u64())
        .unwrap_or(0);
    let filtered_pct = table["filtered"].as_f64().unwrap_or(100.0);

    // Extra flags from "using_filesort", "using_temporary_table", and other fields
    let mut extra_flags = Vec::new();
    if let Some(s) = table["using_filesort"].as_bool() { if s { extra_flags.push("Using filesort".to_string()); } }
    if let Some(s) = table["using_temporary_table"].as_bool() { if s { extra_flags.push("Using temporary".to_string()); } }

    Ok(ExplainResult {
        full_table_scan,
        index_used,
        rows_examined_estimate,
        filtered_pct,
        efficiency: 1.0, // will be set by caller after knowing rows_returned
        extra_flags,
        tier: "fast".to_string(), // will be set by caller
    })
}
