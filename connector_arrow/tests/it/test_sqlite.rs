use std::{env, path::PathBuf, str::FromStr};

use arrow::{datatypes::DataType, util::pretty::pretty_format_batches};
use connector_arrow;
use connector_arrow::api::Connection;
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
fn basic_small() {
    let mut conn = init();
    let path = PathBuf::from_str("tests/data/basic_small.parquet").unwrap();
    super::util::roundtrip_of_parquet(&mut conn, path.as_path(), coerce_ty);
}

#[test]
#[ignore] // SQLite cannot infer schema from an empty response, as there is no rows to infer from
fn empty() {
    let mut conn = init();
    let path = PathBuf::from_str("tests/data/empty.parquet").unwrap();
    super::util::roundtrip_of_parquet(&mut conn, path.as_path(), coerce_ty);
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
fn introspection_01() {
    let mut conn = init();
    let path = PathBuf::from_str("tests/data/basic_small.parquet").unwrap();
    super::util::load_parquet_if_not_exists(&mut conn, path.as_path());

    let refs = conn.get_table_schemas().unwrap();
    assert_debug_snapshot!(refs, @r###"
    [
        TableSchema {
            name: "basic_small.parquet",
            schema: Schema {
                fields: [
                    Field {
                        name: "test_int",
                        data_type: Int64,
                        nullable: true,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                    Field {
                        name: "test_nullint",
                        data_type: Int64,
                        nullable: true,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                    Field {
                        name: "test_str",
                        data_type: LargeUtf8,
                        nullable: true,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                    Field {
                        name: "test_float",
                        data_type: Float64,
                        nullable: true,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                    Field {
                        name: "test_bool",
                        data_type: Int64,
                        nullable: true,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                ],
                metadata: {},
            },
        },
    ]
    "###);
}
