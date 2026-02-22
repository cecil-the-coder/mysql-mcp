//! Criterion benchmarks for SQL parsing.
//!
//! Run with:
//!   cargo bench --bench sql_parsing
//!
//! These benchmarks cover the full `parse_sql` pipeline:
//! sqlparser AST construction → classify_statement → collect_where_info → compute_select_warnings.
//! That is the only significant Rust-level CPU work that happens on every MCP query call.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use mysql_mcp::sql_parser::parse_sql;

/// Representative SELECT queries exercising different branches of classify_statement.
fn bench_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_sql/read");

    let cases: &[(&str, &str)] = &[
        (
            "simple_select",
            "SELECT id, name FROM users WHERE id = 1",
        ),
        (
            "select_star_no_limit",
            "SELECT * FROM users",
        ),
        (
            "no_where_no_limit",
            "SELECT id, created_at FROM orders",
        ),
        (
            "with_limit",
            "SELECT id, name FROM users WHERE active = 1 LIMIT 100",
        ),
        (
            "complex_where",
            "SELECT id FROM events \
             WHERE user_id = 5 \
             AND created_at > '2024-01-01' \
             AND name LIKE '%click%' \
             AND status IN ('open', 'pending')",
        ),
        (
            "leading_wildcard_like",
            "SELECT id FROM users WHERE email LIKE '%@example.com'",
        ),
        (
            "three_table_join",
            "SELECT u.id, u.name, o.total \
             FROM users u \
             JOIN orders o ON u.id = o.user_id \
             JOIN products p ON o.product_id = p.id \
             WHERE u.active = 1",
        ),
        (
            "aggregate_group_by",
            "SELECT COUNT(*), user_id \
             FROM events \
             WHERE created_at > '2024-01-01' \
             GROUP BY user_id",
        ),
        (
            "show_tables",
            "SHOW TABLES",
        ),
        (
            "show_columns",
            "SHOW COLUMNS FROM users",
        ),
    ];

    for (name, sql) in cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), sql, |b, sql| {
            b.iter(|| parse_sql(black_box(sql)))
        });
    }

    group.finish();
}

/// Write and DDL statements: these take a different path through classify_statement.
fn bench_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_sql/write");

    let cases: &[(&str, &str)] = &[
        (
            "insert",
            "INSERT INTO users (name, email, created_at) VALUES ('Alice', 'alice@example.com', NOW())",
        ),
        (
            "update_with_where",
            "UPDATE users SET name = 'Bob', updated_at = NOW() WHERE id = 42",
        ),
        (
            "update_no_where",
            "UPDATE users SET active = 0",
        ),
        (
            "delete_with_where",
            "DELETE FROM sessions WHERE expires_at < NOW()",
        ),
        (
            "delete_no_where",
            "DELETE FROM temp_cache",
        ),
        (
            "create_table",
            "CREATE TABLE audit_logs (\
               id BIGINT AUTO_INCREMENT PRIMARY KEY, \
               user_id INT NOT NULL, \
               action VARCHAR(255) NOT NULL, \
               payload JSON, \
               created_at DATETIME NOT NULL\
             )",
        ),
        (
            "drop_table",
            "DROP TABLE IF EXISTS old_sessions",
        ),
        (
            "alter_table",
            "ALTER TABLE users ADD COLUMN last_login DATETIME",
        ),
    ];

    for (name, sql) in cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), sql, |b, sql| {
            b.iter(|| parse_sql(black_box(sql)))
        });
    }

    group.finish();
}

criterion_group!(benches, bench_reads, bench_writes);
criterion_main!(benches);
