use anyhow::Result;
use sqlparser::ast::{
    Expr, FromTable, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, Use,
    Value,
};
use std::collections::HashSet;

use super::ParsedStatement;
use super::StatementType;

/// Classify a parsed AST statement, computing all cached analysis fields in one pass.
pub(super) fn classify_statement(stmt: &Statement) -> Result<ParsedStatement> {
    let mut has_limit = false;
    let mut has_where = false;
    let mut has_wildcard = false;
    let mut where_columns: Vec<String> = vec![];
    // Additional fields pre-computed during classify for use by callers without re-parse.
    let mut has_named_table = false;
    let mut has_group_by = false;
    let mut has_aggregate = false;
    let mut join_count: usize = 0;
    let mut has_leading_wildcard_like = false;
    let mut is_compound_query = false;

    let (statement_type, target_schema, target_table) = match stmt {
        Statement::Query(query) if query.with.is_some() => (
            StatementType::Other(
                "WITH (CTE) queries are not supported. Rewrite using a subquery instead."
                    .to_string(),
            ),
            None,
            None,
        ),

        Statement::Query(query) => {
            let tname = extract_first_from_table_name(query);
            has_limit = query.limit.is_some();
            if let Some(select) = extract_select(query) {
                has_where = select.selection.is_some();
                has_wildcard = select.projection.iter().any(|item| {
                    matches!(
                        item,
                        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                    )
                });
                if let Some(selection) = &select.selection {
                    let mut seen = HashSet::new();
                    collect_where_info(
                        selection,
                        &mut where_columns,
                        &mut seen,
                        &mut has_leading_wildcard_like,
                    );
                }
                has_named_table = select
                    .from
                    .iter()
                    .any(|t| matches!(t.relation, TableFactor::Table { .. }));
                has_group_by = match &select.group_by {
                    sqlparser::ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
                    sqlparser::ast::GroupByExpr::All(_) => true,
                };
                has_aggregate = has_aggregate_function(&select.projection)
                    || select.having.as_ref().is_some_and(expr_has_aggregate);
                // Count explicit JOIN clauses only. Comma-separated FROM tables
                // (implicit cross joins) are NOT counted here — their WHERE conditions
                // are already analysed by collect_where_info.
                join_count = select.from.iter().map(|twj| twj.joins.len()).sum();
            } else {
                // UNION / INTERSECT / EXCEPT: body is SetExpr::SetOperation, not Select.
                // Analysis fields (has_where, has_named_table, etc.) remain false — no
                // false-positive warnings. Set the flag so we can add an informational note.
                is_compound_query = true;
            }
            (StatementType::Select, None, tname)
        }

        Statement::Insert(insert) => {
            let schema = extract_schema_from_object_name(&insert.table_name);
            let table = extract_table_from_object_name(&insert.table_name);
            (StatementType::Insert, schema, table)
        }

        Statement::Update {
            table, selection, ..
        } => {
            let schema = extract_schema_from_table_factor(&table.relation);
            has_where = selection.is_some();
            (StatementType::Update, schema, None)
        }

        Statement::Delete(delete) => {
            let schema = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables
                    .first()
                    .and_then(|t| extract_schema_from_table_factor(&t.relation)),
            };
            has_where = delete.selection.is_some();
            (StatementType::Delete, schema, None)
        }

        Statement::CreateTable(ct) => {
            let schema = extract_schema_from_object_name(&ct.name);
            let table = extract_table_from_object_name(&ct.name);
            (StatementType::Create, schema, table)
        }

        Statement::CreateDatabase { db_name, .. } => {
            let schema = Some(db_name.to_string());
            (StatementType::Create, schema, None)
        }

        Statement::AlterTable { name, .. } => {
            let schema = extract_schema_from_object_name(name);
            let table = extract_table_from_object_name(name);
            (StatementType::Alter, schema, table)
        }

        Statement::Drop { names, .. } => {
            let schema = names.first().and_then(extract_schema_from_object_name);
            let table = names.first().and_then(extract_table_from_object_name);
            (StatementType::Drop, schema, table)
        }

        Statement::Truncate { table_names, .. } => {
            let schema = table_names
                .first()
                .and_then(|t| extract_schema_from_object_name(&t.name));
            let table = table_names
                .first()
                .and_then(|t| extract_table_from_object_name(&t.name));
            (StatementType::Truncate, schema, table)
        }

        Statement::Use(use_stmt) => {
            let schema = match use_stmt {
                Use::Object(name)
                | Use::Database(name)
                | Use::Schema(name)
                | Use::Catalog(name) => Some(name.to_string()),
                _ => None,
            };
            (StatementType::Use, schema, None)
        }

        Statement::ShowTables { show_options, .. } => {
            let schema = show_options
                .show_in
                .as_ref()
                .and_then(|si| si.parent_name.as_ref())
                .map(|n| n.to_string());
            (StatementType::Show, schema, None)
        }

        Statement::ShowColumns { show_options, .. } => {
            let schema = show_options
                .show_in
                .as_ref()
                .and_then(|si| si.parent_name.as_ref())
                .map(|n| n.to_string());
            (StatementType::Show, schema, None)
        }

        Statement::ShowDatabases { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowViews { .. } => (StatementType::Show, None, None),

        Statement::ShowCreate { obj_name, .. } => {
            let schema = extract_schema_from_object_name(obj_name);
            (StatementType::Show, schema, None)
        }

        Statement::Explain { .. } => (StatementType::Explain, None, None),

        Statement::SetVariable { .. }
        | Statement::SetNames { .. }
        | Statement::SetNamesDefault { .. }
        | Statement::SetTimeZone { .. } => (StatementType::Set, None, None),

        other => {
            // Extract only the variant name from the debug representation.
            // `format!("{:?}", other)` produces strings like "Call(...)" or
            // "LockTables { ... }"; we take everything before the first '{', '(',
            // or space to get just "Call" or "LockTables".
            // This is far more useful than `std::mem::discriminant` which
            // produces an opaque "Discriminant(N)".
            let debug = format!("{other:?}");
            let name = debug
                .split(['{', '(', ' '])
                .next()
                .unwrap_or("Unknown")
                .to_string();
            (StatementType::Other(name), None, None)
        }
    };

    // Pre-compute performance warnings from cached fields (SELECT only).
    let mut warnings = compute_select_warnings(
        &statement_type,
        has_wildcard,
        has_named_table,
        has_limit,
        has_where,
        has_leading_wildcard_like,
        has_group_by,
        has_aggregate,
        join_count,
    );
    if is_compound_query {
        warnings.push(
            "UNION/INTERSECT/EXCEPT: index usage and row estimates are not available for compound queries".to_string()
        );
    }

    Ok(ParsedStatement {
        statement_type,
        target_schema,
        target_table,
        has_limit,
        has_where,
        has_wildcard,
        where_columns,
        has_leading_wildcard_like,
        warnings,
    })
}

/// Compute SELECT performance warnings from pre-parsed fields.
/// Returns an empty vec for non-SELECT statements.
#[allow(clippy::too_many_arguments)]
pub(super) fn compute_select_warnings(
    statement_type: &StatementType,
    has_wildcard: bool,
    has_named_table: bool,
    has_limit: bool,
    has_where: bool,
    has_leading_wildcard_like: bool,
    has_group_by: bool,
    has_aggregate: bool,
    join_count: usize,
) -> Vec<String> {
    if *statement_type != StatementType::Select {
        return vec![];
    }

    let mut warnings = Vec::new();

    // 1. SELECT * on a real table — advise column selection regardless of LIMIT.
    // Deliberately does NOT gate on !has_limit so it doesn't overlap with Warning 4.
    if has_wildcard && has_named_table {
        warnings.push(
            "SELECT * returns all columns — specify needed columns for efficiency".to_string(),
        );
    }

    // 2. No WHERE and no LIMIT on a real table — full scan
    if has_named_table && !has_where && !has_limit {
        warnings
            .push("No WHERE clause and no LIMIT — query will scan the entire table".to_string());
    }

    // 3. LIKE with a leading wildcard — index unusable
    if has_leading_wildcard_like {
        warnings.push("Leading wildcard in LIKE — index on this column cannot be used".to_string());
    }

    // 4. No LIMIT on a non-aggregate SELECT that has a WHERE clause.
    // Only warn here when Warning 2 did NOT already fire (i.e., there IS a WHERE clause);
    // if there is neither WHERE nor LIMIT, Warning 2 already covers the full-scan risk.
    if !has_limit && !has_group_by && !has_aggregate && has_where {
        warnings.push("No LIMIT clause — query may return unbounded rows".to_string());
    }

    // 5. 3+ table joins
    if join_count >= 3 {
        warnings.push(format!(
            "Query joins {} tables — ensure join columns are indexed",
            join_count
        ));
    }

    warnings
}

// ── Internal AST helpers ──────────────────────────────────────────────────────

/// Extract the innermost Select from a Query (handles simple SELECT; ignores UNION etc.)
pub(super) fn extract_select(query: &Query) -> Option<&Select> {
    match query.body.as_ref() {
        SetExpr::Select(select) => Some(select),
        _ => None,
    }
}

/// Returns true if any aggregate function (COUNT, SUM, AVG, MIN, MAX) appears in the projection.
fn has_aggregate_function(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return false,
        };
        expr_has_aggregate(expr)
    })
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(f) => f.name.0.last().is_some_and(|id| {
            let v = id.value.as_str();
            // Standard SQL aggregates
            v.eq_ignore_ascii_case("COUNT")
                    || v.eq_ignore_ascii_case("SUM")
                    || v.eq_ignore_ascii_case("AVG")
                    || v.eq_ignore_ascii_case("MIN")
                    || v.eq_ignore_ascii_case("MAX")
                    // MySQL-specific aggregates
                    || v.eq_ignore_ascii_case("GROUP_CONCAT")
                    || v.eq_ignore_ascii_case("BIT_AND")
                    || v.eq_ignore_ascii_case("BIT_OR")
                    || v.eq_ignore_ascii_case("BIT_XOR")
                    || v.eq_ignore_ascii_case("STDDEV")
                    || v.eq_ignore_ascii_case("STDDEV_POP")
                    || v.eq_ignore_ascii_case("STDDEV_SAMP")
                    || v.eq_ignore_ascii_case("STD")
                    || v.eq_ignore_ascii_case("VARIANCE")
                    || v.eq_ignore_ascii_case("VAR_POP")
                    || v.eq_ignore_ascii_case("VAR_SAMP")
                    || v.eq_ignore_ascii_case("JSON_ARRAYAGG")
                    || v.eq_ignore_ascii_case("JSON_OBJECTAGG")
        }),
        Expr::BinaryOp { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::UnaryOp { expr, .. } => expr_has_aggregate(expr),
        Expr::Nested(inner) => expr_has_aggregate(inner),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(expr_has_aggregate)
                || conditions.iter().any(expr_has_aggregate)
                || results.iter().any(expr_has_aggregate)
                || else_result.as_deref().is_some_and(expr_has_aggregate)
        }
        _ => false,
    }
}

pub(super) fn extract_schema_from_object_name(name: &ObjectName) -> Option<String> {
    // MySQL fully-qualified: schema.table has 2 parts; the first is schema
    (name.0.len() >= 2).then(|| name.0[0].value.clone())
}

pub(super) fn extract_schema_from_table_factor(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { name, .. } => extract_schema_from_object_name(name),
        _ => None,
    }
}

/// Extract the bare table name (last identifier) from an ObjectName.
/// For `schema.table` returns `"table"`; for bare `table` returns `"table"`.
fn extract_table_from_object_name(name: &ObjectName) -> Option<String> {
    name.0.last().map(|ident| ident.value.clone())
}

/// Extract the primary FROM table name from a SELECT query AST.
/// Returns only the bare table name (last identifier), ignoring any schema prefix.
fn extract_first_from_table_name(query: &Query) -> Option<String> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    let first_from = select.from.first()?;
    match &first_from.relation {
        TableFactor::Table { name, .. } => {
            // Last part of a (possibly qualified) name is the table name
            name.0.last().map(|ident| ident.value.clone())
        }
        _ => None,
    }
}

/// Recursively walk a WHERE expression tree in a single pass, collecting column name
/// identifiers into `cols`/`seen` and setting `*has_leading_wildcard` when a LIKE/ILike
/// pattern starts with '%'.  Replaces the former separate `collect_where_columns` and
/// `expr_has_leading_wildcard_like` functions.
pub(super) fn collect_where_info(
    expr: &Expr,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
    has_leading_wildcard: &mut bool,
) {
    match expr {
        Expr::Identifier(ident) => {
            if seen.insert(ident.value.to_lowercase()) {
                cols.push(ident.value.clone());
            }
        }
        Expr::CompoundIdentifier(parts) => {
            // Take the last part as the column name (e.g., table.column -> column)
            if let Some(last) = parts.last() {
                if seen.insert(last.value.to_lowercase()) {
                    cols.push(last.value.clone());
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_where_info(left, cols, seen, has_leading_wildcard);
            collect_where_info(right, cols, seen, has_leading_wildcard);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_where_info(expr, cols, seen, has_leading_wildcard);
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_where_info(inner, cols, seen, has_leading_wildcard);
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            collect_where_info(left, cols, seen, has_leading_wildcard);
            collect_where_info(right, cols, seen, has_leading_wildcard);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_where_info(expr, cols, seen, has_leading_wildcard);
            collect_where_info(low, cols, seen, has_leading_wildcard);
            collect_where_info(high, cols, seen, has_leading_wildcard);
        }
        Expr::Like {
            expr: like_expr,
            pattern,
            ..
        }
        | Expr::ILike {
            expr: like_expr,
            pattern,
            ..
        } => {
            // Detect a leading-wildcard pattern string literal.
            if let Expr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) =
                pattern.as_ref()
            {
                if s.starts_with('%') {
                    *has_leading_wildcard = true;
                }
            }
            collect_where_info(like_expr, cols, seen, has_leading_wildcard);
            collect_where_info(pattern, cols, seen, has_leading_wildcard);
        }
        Expr::InList { expr, .. } => {
            collect_where_info(expr, cols, seen, has_leading_wildcard);
        }
        Expr::Function(func) => {
            // Recurse into function arguments so columns inside UPPER(col), COALESCE(a, b), etc.
            // are included in index-suggestion analysis.
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                for arg in &arg_list.args {
                    let arg_expr = match arg {
                        sqlparser::ast::FunctionArg::Named { arg, .. } => arg,
                        sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => arg,
                        sqlparser::ast::FunctionArg::Unnamed(arg) => arg,
                    };
                    if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg_expr {
                        collect_where_info(e, cols, seen, has_leading_wildcard);
                    }
                }
            }
        }
        Expr::Cast { expr, .. } => {
            collect_where_info(expr, cols, seen, has_leading_wildcard);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_where_info(op, cols, seen, has_leading_wildcard);
            }
            for cond in conditions {
                collect_where_info(cond, cols, seen, has_leading_wildcard);
            }
            for result in results {
                collect_where_info(result, cols, seen, has_leading_wildcard);
            }
            if let Some(else_expr) = else_result {
                collect_where_info(else_expr, cols, seen, has_leading_wildcard);
            }
        }
        Expr::Nested(inner) => {
            collect_where_info(inner, cols, seen, has_leading_wildcard);
        }
        _ => {}
    }
}
