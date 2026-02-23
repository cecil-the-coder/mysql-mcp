//! Criterion benchmarks for check_permission.
//!
//! Run with:
//!   cargo bench --bench permissions
//!
//! check_permission runs on every query. It performs a schema permission lookup
//! (HashMap), string lowercasing, and several match branches.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mysql_mcp::config::{Config, SchemaPermissions};
use mysql_mcp::permissions::check_permission;
use mysql_mcp::sql_parser::StatementType;

fn make_config_allow_all() -> Config {
    let mut c = Config::default();
    c.connection.database = Some("mydb".to_string());
    c.security.allow_insert = true;
    c.security.allow_update = true;
    c.security.allow_delete = true;
    c.security.allow_ddl = true;
    c
}

fn make_config_deny_all() -> Config {
    // Default Config has all writes denied
    let mut c = Config::default();
    c.connection.database = Some("mydb".to_string());
    c
}

fn make_config_schema_override() -> Config {
    let mut c = make_config_deny_all();
    c.security.schema_permissions.insert(
        "mydb".to_string(),
        SchemaPermissions {
            allow_insert: Some(true),
            allow_update: Some(false),
            allow_delete: Some(false),
            allow_ddl: Some(false),
        },
    );
    c
}

fn make_config_multi_db() -> Config {
    // No database set = multi-DB mode
    let mut c = Config::default();
    c.security.allow_insert = true;
    c.security.multi_db_write_mode = true;
    c
}

fn bench_check_permission(c: &mut Criterion) {
    let allow_all = make_config_allow_all();
    let deny_all = make_config_deny_all();
    let schema_override = make_config_schema_override();
    let multi_db = make_config_multi_db();

    let mut group = c.benchmark_group("check_permission");

    // SELECT: fast path (always allowed, no HashMap lookup)
    group.bench_function("select/allowed", |b| {
        b.iter(|| {
            check_permission(
                black_box(&allow_all),
                black_box(&StatementType::Select),
                black_box(None),
            )
        })
    });

    // INSERT allowed: HashMap lookup hits, permission granted, multi-db check passes (single-db mode)
    group.bench_function("insert/allowed_single_db", |b| {
        b.iter(|| {
            check_permission(
                black_box(&allow_all),
                black_box(&StatementType::Insert),
                black_box(Some("mydb")),
            )
        })
    });

    // INSERT denied: bails early after permission check
    group.bench_function("insert/denied", |b| {
        b.iter(|| {
            check_permission(
                black_box(&deny_all),
                black_box(&StatementType::Insert),
                black_box(None),
            )
        })
    });

    // INSERT allowed via schema-specific override (HashMap lookup + schema match)
    group.bench_function("insert/schema_override_allow", |b| {
        b.iter(|| {
            check_permission(
                black_box(&schema_override),
                black_box(&StatementType::Insert),
                black_box(Some("mydb")),
            )
        })
    });

    // INSERT in multi-DB mode with write flag set
    group.bench_function("insert/multi_db_allowed", |b| {
        b.iter(|| {
            check_permission(
                black_box(&multi_db),
                black_box(&StatementType::Insert),
                black_box(Some("somedb")),
            )
        })
    });

    // DDL allowed
    group.bench_function("ddl/allowed", |b| {
        b.iter(|| {
            check_permission(
                black_box(&allow_all),
                black_box(&StatementType::Create),
                black_box(Some("mydb")),
            )
        })
    });

    // DDL denied
    group.bench_function("ddl/denied", |b| {
        b.iter(|| {
            check_permission(
                black_box(&deny_all),
                black_box(&StatementType::Create),
                black_box(None),
            )
        })
    });

    group.finish();
}

criterion_group!(benches, bench_check_permission);
criterion_main!(benches);
