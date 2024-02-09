use arrow::datatypes::DataType;
use connector_arrow::api::{Connection, EditSchema};
use insta::assert_debug_snapshot;
use itertools::Itertools;
use std::{path::PathBuf, str::FromStr};

fn init() -> duckdb::Connection {
    let _ = env_logger::builder().is_test(true).try_init();

    duckdb::Connection::open_in_memory().unwrap()
}

fn coerce_ty(ty: &DataType) -> Option<DataType> {
    match ty {
        DataType::Null => Some(DataType::Int64),
        _ => None,
    }
}

#[test]
fn roundtrip_basic_small() {
    let mut conn = init();
    let path = PathBuf::from_str("tests/data/basic_small.parquet").unwrap();
    super::util::roundtrip_of_parquet(&mut conn, path.as_path(), coerce_ty);
}

#[test]
fn roundtrip_empty() {
    let mut conn = init();
    let path = PathBuf::from_str("tests/data/empty.parquet").unwrap();
    super::util::roundtrip_of_parquet(&mut conn, path.as_path(), coerce_ty);
}

#[test]
fn introspection_basic_small() {
    let mut conn = init();
    let path = PathBuf::from_str("tests/data/basic_small.parquet").unwrap();
    let (_table, schema_file, _) =
        super::util::load_parquet_if_not_exists(&mut conn, path.as_path());
    let schema_file_coerced = super::util::cast_schema(&schema_file, &coerce_ty);

    let refs = conn.get_table_schemas().unwrap();
    let schema_introspection = refs.into_iter().exactly_one().unwrap().schema;
    similar_asserts::assert_eq!(schema_file_coerced.as_ref(), &schema_introspection);
}

#[test]
fn schema_edit_01() {
    let mut conn = init();
    let path = PathBuf::from_str("tests/data/basic_small.parquet").unwrap();
    let (_, schema, _) = super::util::load_parquet_if_not_exists(&mut conn, path.as_path());

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
