//! Criterion benchmarks for EXPLAIN FORMAT=JSON parsing (parse_v2).
//!
//! Run with:
//!   cargo bench --bench explain_parsing
//!
//! parse_v2 is called on every query that triggers auto-EXPLAIN (slow queries
//! or when performance_hints="always"). It recursively walks the JSON plan tree
//! produced by MySQL 8.0 EXPLAIN FORMAT=JSON schema v2.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use mysql_mcp::query::explain_parse::parse_v2;
use serde_json::json;

fn make_v2(query_plan: serde_json::Value) -> serde_json::Value {
    json!({
        "query": "SELECT ...",
        "query_plan": query_plan,
        "query_type": "select",
        "json_schema_version": "2.0"
    })
}

fn bench_explain_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_v2");

    // 1. Constant query — optimizer resolves at parse time, no table scan
    let constant = make_v2(json!({
        "operation": "Rows fetched before execution",
        "access_type": "rows_fetched_before_execution",
        "estimated_rows": 1.0,
        "estimated_total_cost": 0.0,
        "estimated_first_row_cost": 0.0
    }));

    // 2. Single index lookup (primary key)
    let index_lookup = make_v2(json!({
        "alias": "u",
        "covering": false,
        "operation": "Single-row index lookup on u using PRIMARY (id = 1)",
        "index_name": "PRIMARY",
        "table_name": "users",
        "access_type": "index",
        "key_columns": ["id"],
        "schema_name": "mydb",
        "estimated_rows": 1.0,
        "estimated_total_cost": 0.25
    }));

    // 3. Full table scan wrapped in a filter node
    let full_table_scan = make_v2(json!({
        "inputs": [{
            "operation": "Table scan on users",
            "table_name": "users",
            "access_type": "table",
            "schema_name": "mydb",
            "estimated_rows": 50000.0,
            "estimated_total_cost": 5075.25
        }],
        "condition": "(users.status = 'active')",
        "operation": "Filter: (users.status = 'active')",
        "access_type": "filter",
        "estimated_rows": 25000.0,
        "estimated_total_cost": 5075.25
    }));

    // 4. Sort wrapping a table scan (triggers "Using filesort")
    let sort_table_scan = make_v2(json!({
        "inputs": [{
            "operation": "Table scan on events",
            "table_name": "events",
            "access_type": "table",
            "schema_name": "mydb",
            "estimated_rows": 1000.0,
            "estimated_total_cost": 101.25
        }],
        "operation": "Sort: events.created_at",
        "access_type": "sort",
        "sort_fields": ["events.created_at"],
        "estimated_rows": 1000.0,
        "estimated_total_cost": 101.25
    }));

    // 5. Nested loop join: filter+table_scan on left, index_lookup on right
    let nested_loop_join = make_v2(json!({
        "inputs": [
            {
                "inputs": [{
                    "operation": "Table scan on orders",
                    "table_name": "orders",
                    "access_type": "table",
                    "schema_name": "mydb",
                    "estimated_rows": 10000.0,
                    "estimated_total_cost": 1012.75
                }],
                "condition": "(orders.status = 'pending')",
                "operation": "Filter: (orders.status = 'pending')",
                "access_type": "filter",
                "estimated_rows": 500.0,
                "estimated_total_cost": 1012.75
            },
            {
                "alias": "u",
                "covering": false,
                "operation": "Single-row index lookup on u using PRIMARY (id = orders.user_id)",
                "index_name": "PRIMARY",
                "table_name": "users",
                "access_type": "index",
                "key_columns": ["id"],
                "schema_name": "mydb",
                "estimated_rows": 1.0,
                "estimated_total_cost": 0.25
            }
        ],
        "join_type": "inner join",
        "operation": "Nested loop inner join",
        "access_type": "join",
        "estimated_rows": 500.0,
        "join_algorithm": "nested_loop",
        "estimated_total_cost": 1138.25
    }));

    // 6. Deeply nested: sort → join → (filter+table, index) — worst-case tree walk
    let deep_plan = make_v2(json!({
        "inputs": [{
            "inputs": [
                {
                    "inputs": [{
                        "operation": "Table scan on products",
                        "table_name": "products",
                        "access_type": "table",
                        "schema_name": "mydb",
                        "estimated_rows": 5000.0,
                        "estimated_total_cost": 506.25
                    }],
                    "condition": "(products.category = 'electronics')",
                    "operation": "Filter: (products.category = 'electronics')",
                    "access_type": "filter",
                    "estimated_rows": 200.0,
                    "estimated_total_cost": 506.25
                },
                {
                    "alias": "oi",
                    "covering": false,
                    "operation": "Index lookup on oi using idx_product_id (product_id = products.id)",
                    "index_name": "idx_product_id",
                    "table_name": "order_items",
                    "access_type": "index",
                    "key_columns": ["product_id"],
                    "schema_name": "mydb",
                    "estimated_rows": 3.0,
                    "estimated_total_cost": 1.05
                }
            ],
            "join_type": "inner join",
            "operation": "Nested loop inner join",
            "access_type": "join",
            "estimated_rows": 600.0,
            "join_algorithm": "nested_loop",
            "estimated_total_cost": 1116.75
        }],
        "operation": "Sort: products.name",
        "access_type": "sort",
        "sort_fields": ["products.name"],
        "estimated_rows": 600.0,
        "estimated_total_cost": 1116.75
    }));

    let cases: &[(&str, serde_json::Value)] = &[
        ("constant_query", constant),
        ("index_lookup", index_lookup),
        ("full_table_scan", full_table_scan),
        ("sort_table_scan", sort_table_scan),
        ("nested_loop_join", nested_loop_join),
        ("deep_nested_sort_join", deep_plan),
    ];

    for (name, v) in cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), v, |b, v| {
            b.iter(|| parse_v2(black_box(v)))
        });
    }

    group.finish();
}

criterion_group!(benches, bench_explain_parsing);
criterion_main!(benches);
