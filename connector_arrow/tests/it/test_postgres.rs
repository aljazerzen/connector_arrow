use arrow::util::pretty::pretty_format_batches;
use connector_arrow::postgres::{PostgresConnection, ProtocolExtended, ProtocolSimple};
use insta::assert_display_snapshot;
use postgres::{Client, NoTls};

use std::env;

fn init() -> Client {
    let _ = env_logger::builder().is_test(true).try_init();

    let dburl = env::var("POSTGRES_URL").unwrap();
    Client::connect(&dburl, NoTls).unwrap()
}

#[test]
fn test_protocol_simple() {
    let mut conn = init();
    let mut conn = PostgresConnection::<ProtocolSimple>::new(&mut conn);

    let query = "select * from test_table";
    let results = connector_arrow::query_one(&mut conn, query).unwrap();

    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +----------+--------------+----------+------------+-----------+
    | test_int | test_nullint | test_str | test_float | test_bool |
    +----------+--------------+----------+------------+-----------+
    | 1        | 3            | str1     |            | true      |
    | 2        |              | str2     | 2.2        | false     |
    | 0        | 5            | a        | 3.1        |           |
    | 3        | 7            | b        | 3.0        | false     |
    | 4        | 9            | c        | 7.8        |           |
    | 1314     | 2            |          | -10.0      | true      |
    +----------+--------------+----------+------------+-----------+
    "###);
}

#[test]
fn test_protocol_cursor() {
    let mut conn = init();
    let mut conn = PostgresConnection::<ProtocolExtended>::new(&mut conn);

    let query = "select * from test_table";
    let results = connector_arrow::query_one(&mut conn, query).unwrap();

    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +----------+--------------+----------+------------+-----------+
    | test_int | test_nullint | test_str | test_float | test_bool |
    +----------+--------------+----------+------------+-----------+
    | 1        | 3            | str1     |            | true      |
    | 2        |              | str2     | 2.2        | false     |
    | 0        | 5            | a        | 3.1        |           |
    | 3        | 7            | b        | 3.0        | false     |
    | 4        | 9            | c        | 7.8        |           |
    | 1314     | 2            |          | -10.0      | true      |
    +----------+--------------+----------+------------+-----------+
    "###);
}
