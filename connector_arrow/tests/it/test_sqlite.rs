use std::env;

use arrow::util::pretty::pretty_format_batches;
use connector_arrow::{self, api::Connection};
use insta::{assert_debug_snapshot, assert_display_snapshot};

fn init() -> rusqlite::Connection {
    let _ = env_logger::builder().is_test(true).try_init();

    let url = "../dbs/".to_string() + env::var("SQLITE_URL").unwrap().as_str();

    rusqlite::Connection::open(url).unwrap()
}

#[test]
fn test_query_01() {
    let mut conn = init();

    let query = "select * from test_table";
    let results = connector_arrow::query_one(&mut conn, &query).unwrap();
    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +----------+--------------+------------+------------+-----------+------------+-----------+---------------------+
    | test_int | test_nullint | test_str   | test_float | test_bool | test_date  | test_time | test_datetime       |
    +----------+--------------+------------+------------+-----------+------------+-----------+---------------------+
    | 1        | 3            | str1       |            | 1         | 1996-03-13 | 08:12:40  | 2007-01-01 10:00:19 |
    | 2        |              | str2       | 2.2        | 0         | 1996-01-30 | 10:03:00  | 2005-01-01 22:03:00 |
    | 0        | 5            | こんにちは | 3.1        |           | 1996-02-28 | 23:00:10  |                     |
    | 3        | 7            | b          | 3.0        | 0         | 2020-01-12 | 23:00:10  | 1987-01-01 11:00:00 |
    | 4        | 9            | Ha好ち😁ðy̆ | 7.8        |           | 1996-04-20 | 18:30:00  |                     |
    | 1314     | 2            |            | -10.0      | 1         |            | 18:30:00  | 2007-10-01 10:32:00 |
    +----------+--------------+------------+------------+-----------+------------+-----------+---------------------+
    "###);
}

#[test]
fn test_query_02() {
    let mut conn = init();
    let query = "select test_int, test_nullint, test_str from test_table where test_int >= 2";
    let results = connector_arrow::query_one(&mut conn, &query).unwrap();
    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +----------+--------------+------------+
    | test_int | test_nullint | test_str   |
    +----------+--------------+------------+
    | 2        |              | str2       |
    | 3        | 7            | b          |
    | 4        | 9            | Ha好ち😁ðy̆ |
    | 1314     | 2            |            |
    +----------+--------------+------------+
    "###);
}

#[test]
fn test_query_03() {
    let mut conn = init();
    let query = "select 1 + test_int as a from test_table order by test_int limit 3";
    let results = connector_arrow::query_one(&mut conn, &query).unwrap();
    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +---+
    | a |
    +---+
    | 1 |
    | 2 |
    | 3 |
    +---+
    "###);
}

#[test]
fn test_introspection_01() {
    let mut conn = init();

    let refs = conn.get_relation_defs().unwrap();
    assert_debug_snapshot!(refs, @r###"
    [
        RelationDef {
            name: "test_table",
            schema: Schema {
                fields: [
                    Field {
                        name: "test_int",
                        data_type: Int64,
                        nullable: false,
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
                    Field {
                        name: "test_date",
                        data_type: LargeUtf8,
                        nullable: true,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                    Field {
                        name: "test_time",
                        data_type: LargeUtf8,
                        nullable: true,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                    Field {
                        name: "test_datetime",
                        data_type: LargeUtf8,
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
