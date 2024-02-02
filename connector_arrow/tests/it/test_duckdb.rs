use arrow::util::pretty::pretty_format_batches;
use connector_arrow::rewrite;
use insta::assert_display_snapshot;
use std::env;

#[test]
fn load_and_parse() {
    let _ = env_logger::builder().is_test(true).try_init();

    let url = "../dbs/".to_string() + env::var("DUCKDB_URL").unwrap().as_str();
    let store = rewrite::duckdb::DuckDBDataStore::new(&url, 1).unwrap();

    let query = "select * from test_table";

    let results = rewrite::query_one(&store, query).unwrap();

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
