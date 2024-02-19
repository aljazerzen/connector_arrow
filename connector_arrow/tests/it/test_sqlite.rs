use std::{env, path::PathBuf, str::FromStr};

use arrow::{datatypes::DataType, util::pretty::pretty_format_batches};
use connector_arrow::api::SchemaGet;
use connector_arrow::{self, api::SchemaEdit};
use insta::{assert_debug_snapshot, assert_display_snapshot};

fn init() -> rusqlite::Connection {
    let _ = env_logger::builder().is_test(true).try_init();

    rusqlite::Connection::open_in_memory().unwrap()
}

fn coerce_ty(ty: &DataType) -> Option<DataType> {
    match ty {
        DataType::Boolean => Some(DataType::Int64),
        DataType::Int8 => Some(DataType::Int64),
        DataType::Int16 => Some(DataType::Int64),
        DataType::Int32 => Some(DataType::Int64),
        DataType::Int64 => Some(DataType::Int64),
        DataType::UInt8 => Some(DataType::Int64),
        DataType::UInt16 => Some(DataType::Int64),
        DataType::UInt32 => Some(DataType::Int64),
        DataType::UInt64 => Some(DataType::Int64),
        DataType::Float16 => Some(DataType::Float64),
        DataType::Float32 => Some(DataType::Float64),
        DataType::Float64 => Some(DataType::Float64),
        DataType::Binary => Some(DataType::LargeBinary),
        DataType::FixedSizeBinary(_) => Some(DataType::LargeBinary),
        DataType::LargeBinary => Some(DataType::LargeBinary),
        DataType::Utf8 => Some(DataType::LargeUtf8),
        DataType::LargeUtf8 => Some(DataType::LargeUtf8),
        _ => None,
    }
}

#[test]
fn roundtrip_basic_small() {
    let table_name = "roundtrip_basic_small";

    let mut conn = init();
    let path = PathBuf::from_str("tests/data/basic_small.parquet").unwrap();
    super::util::roundtrip_of_parquet(&mut conn, path.as_path(), table_name, coerce_ty);
}

#[test]
#[ignore] // SQLite cannot infer schema from an empty response, as there is no rows to infer from
fn roundtrip_empty() {
    let table_name = "roundtrip_empty";

    let mut conn = init();
    let path = PathBuf::from_str("tests/data/empty.parquet").unwrap();
    super::util::roundtrip_of_parquet(&mut conn, path.as_path(), table_name, coerce_ty);
}

#[test]
fn query_04() {
    let mut conn = init();
    let query = "SELECT 1, NULL";
    let results = connector_arrow::query_one(&mut conn, &query).unwrap();
    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +---+------+
    | 1 | NULL |
    +---+------+
    | 1 |      |
    +---+------+
    "###);
}

#[test]
#[ignore] // cannot introspect the Null column
fn introspection_basic_small() {
    let table_name = "introspection_basic_small";

    let mut conn = init();
    let path = PathBuf::from_str("tests/data/basic_small.parquet").unwrap();
    let (schema_file, _) =
        super::util::load_parquet_if_not_exists(&mut conn, path.as_path(), table_name);
    let schema_file_coerced = super::util::cast_schema(&schema_file, &coerce_ty);

    let schema_introspection = conn.table_get(table_name).unwrap();
    similar_asserts::assert_eq!(schema_file_coerced, schema_introspection);
}

#[test]
fn schema_edit_01() {
    let table_name = "schema_edit_01";

    let mut conn = init();
    let path = PathBuf::from_str("tests/data/basic_small.parquet").unwrap();
    let (schema, _) =
        super::util::load_parquet_if_not_exists(&mut conn, path.as_path(), table_name);

    let _ignore = conn.table_drop("test_table2");

    conn.table_create("test_table2", schema.clone()).unwrap();
    assert_debug_snapshot!(
        conn.table_create("test_table2", schema.clone()).unwrap_err(), @"TableExists"
    );

    conn.table_drop("test_table2").unwrap();
    assert_debug_snapshot!(
        conn.table_drop("test_table2").unwrap_err(), @"TableNonexistent"
    );
}
