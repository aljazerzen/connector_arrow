use arrow::util::pretty::pretty_format_batches;
use connector_arrow::rewrite;
use duckdb::DuckdbConnectionManager;
use insta::assert_display_snapshot;
use std::{env, ops::DerefMut};

fn init() -> r2d2::Pool<DuckdbConnectionManager> {
    let _ = env_logger::builder().is_test(true).try_init();

    let url = env::var("DUCKDB_URL").unwrap();

    let manager = DuckdbConnectionManager::file(url).unwrap();

    r2d2::Pool::builder().max_size(5).build(manager).unwrap()
}

#[test]
fn load_and_parse() {
    let pool = init();
    let mut conn = pool.get().unwrap();

    let query = "select * from test_table";

    let results = rewrite::query_one(conn.deref_mut(), query).unwrap();

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
