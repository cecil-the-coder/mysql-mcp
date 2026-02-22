use anyhow::Result;
use sqlx::MySqlPool;

use super::{TableInfo, ColumnInfo, IndexDef};

// MySQL information_schema columns (TABLE_NAME, DATA_TYPE, COLUMN_KEY, etc.) are
// sometimes returned as binary blobs by sqlx. These helpers try String first,
// then fall back to Vec<u8> -> UTF-8 so callers always get a usable value.
pub(crate) fn is_col_str(row: &sqlx::mysql::MySqlRow, col: &str) -> String {
    use sqlx::Row;
    row.try_get::<String, _>(col)
        .or_else(|_| {
            row.try_get::<Vec<u8>, _>(col)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        })
        .unwrap_or_default()
}

pub(crate) fn is_col_str_opt(row: &sqlx::mysql::MySqlRow, col: &str) -> Option<String> {
    use sqlx::Row;
    let s = row.try_get::<Option<String>, _>(col)
        .ok()
        .flatten()
        .or_else(|| {
            row.try_get::<Option<Vec<u8>>, _>(col)
                .ok()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        })?;
    if s.is_empty() { None } else { Some(s) }
}

/// Escape a MySQL identifier for use in backtick-quoted contexts.
/// Doubles any backtick characters within the name.
pub(crate) fn escape_mysql_identifier(name: &str) -> String {
    name.replace('`', "``")
}

pub(crate) async fn fetch_tables(pool: &MySqlPool, database: Option<&str>) -> Result<Vec<TableInfo>> {
    let rows = if let Some(db) = database {
        sqlx::query(
            r#"SELECT
                TABLE_NAME as name,
                TABLE_SCHEMA as `schema`,
                TABLE_ROWS as row_count,
                DATA_LENGTH as data_size_bytes,
                CREATE_TIME as create_time,
                UPDATE_TIME as update_time
            FROM information_schema.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND table_schema = ?
            ORDER BY TABLE_SCHEMA, TABLE_NAME"#,
        )
        .bind(db)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"SELECT
                TABLE_NAME as name,
                TABLE_SCHEMA as `schema`,
                TABLE_ROWS as row_count,
                DATA_LENGTH as data_size_bytes,
                CREATE_TIME as create_time,
                UPDATE_TIME as update_time
            FROM information_schema.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
            ORDER BY TABLE_SCHEMA, TABLE_NAME"#,
        )
        .fetch_all(pool)
        .await?
    };

    let tables = rows.iter().map(|row| {
        use sqlx::Row;
        TableInfo {
            name: is_col_str(row, "name"),
            schema: is_col_str(row, "schema"),
            row_count: row.try_get("row_count").ok(),
            data_size_bytes: row.try_get("data_size_bytes").ok(),
            create_time: row.try_get::<Option<chrono::NaiveDateTime>, _>("create_time")
                .ok().flatten().map(|d| d.to_string()),
            update_time: row.try_get::<Option<chrono::NaiveDateTime>, _>("update_time")
                .ok().flatten().map(|d| d.to_string()),
        }
    }).collect();

    Ok(tables)
}

pub(crate) async fn fetch_columns(pool: &MySqlPool, table_name: &str, database: Option<&str>) -> Result<Vec<ColumnInfo>> {
    let rows = if let Some(db) = database {
        sqlx::query(
            r#"SELECT
                COLUMN_NAME as name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as column_default,
                COLUMN_KEY as column_key,
                EXTRA as extra
            FROM information_schema.COLUMNS
            WHERE TABLE_NAME = ?
            AND TABLE_SCHEMA = ?
            ORDER BY ORDINAL_POSITION"#,
        )
        .bind(table_name)
        .bind(db)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"SELECT
                COLUMN_NAME as name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as column_default,
                COLUMN_KEY as column_key,
                EXTRA as extra
            FROM information_schema.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION"#,
        )
        .bind(table_name)
        .fetch_all(pool)
        .await?
    };

    let columns = rows.iter().map(|row| {
        let nullable_str = is_col_str(row, "is_nullable");
        ColumnInfo {
            name: is_col_str(row, "name"),
            data_type: is_col_str(row, "data_type"),
            is_nullable: nullable_str == "YES",
            column_default: is_col_str_opt(row, "column_default"),
            column_key: is_col_str_opt(row, "column_key"),
            extra: {
                let e = is_col_str(row, "extra");
                if e.is_empty() { None } else { Some(e) }
            },
        }
    }).collect();

    Ok(columns)
}

pub(crate) async fn fetch_indexed_columns(pool: &MySqlPool, table: &str, database: Option<&str>) -> Result<Vec<String>> {
    let qualified = match database {
        Some(db) => format!("`{}`.`{}`", escape_mysql_identifier(db), escape_mysql_identifier(table)),
        None => format!("`{}`", escape_mysql_identifier(table)),
    };
    let sql = format!("SHOW INDEX FROM {}", qualified);
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    let mut cols: Vec<String> = Vec::new();
    for row in &rows {
        let col_name = is_col_str(row, "Column_name");
        if !col_name.is_empty() && !cols.iter().any(|c: &String| c.eq_ignore_ascii_case(&col_name)) {
            cols.push(col_name);
        }
    }
    Ok(cols)
}

pub(crate) async fn fetch_composite_indexes(pool: &MySqlPool, table: &str, database: Option<&str>) -> Result<Vec<IndexDef>> {
    let rows = if let Some(db) = database {
        sqlx::query(
            "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME \
             FROM information_schema.STATISTICS \
             WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ? \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX"
        )
        .bind(table)
        .bind(db)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME \
             FROM information_schema.STATISTICS \
             WHERE TABLE_NAME = ? \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX"
        )
        .bind(table)
        .fetch_all(pool)
        .await?
    };

    let mut index_map: std::collections::BTreeMap<String, IndexDef> = std::collections::BTreeMap::new();
    for row in &rows {
        use sqlx::Row;
        let name = is_col_str(row, "INDEX_NAME");
        let non_unique: i64 = row.try_get("NON_UNIQUE").unwrap_or(1);
        let col = is_col_str(row, "COLUMN_NAME");
        let entry = index_map.entry(name.clone()).or_insert_with(|| IndexDef {
            name: name.clone(),
            unique: non_unique == 0,
            columns: Vec::new(),
        });
        if !col.is_empty() {
            entry.columns.push(col);
        }
    }

    Ok(index_map.into_values().collect())
}
