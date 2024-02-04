use std::{env, ops::DerefMut};

use arrow::util::pretty::pretty_format_batches;
use connector_arrow::rewrite;
use insta::assert_display_snapshot;
use r2d2_sqlite::SqliteConnectionManager;

fn init() -> r2d2::Pool<SqliteConnectionManager> {
    let _ = env_logger::builder().is_test(true).try_init();

    let url = "../dbs/".to_string() + env::var("SQLITE_URL").unwrap().as_str();

    let manager = SqliteConnectionManager::file(url);

    r2d2::Pool::builder().max_size(5).build(manager).unwrap()
}

#[test]
fn test_sqlite() {
    let pool = init();

    let mut conn = pool.get().unwrap();

    log::debug!("main");

    log::debug!("source");

    let query = "select test_int, test_nullint, test_str from test_table where test_int < 2";
    let results = rewrite::query_one(conn.deref_mut(), &query).unwrap();
    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +----------+--------------+------------+
    | test_int | test_nullint | test_str   |
    +----------+--------------+------------+
    | 1        | 3            | str1       |
    | 0        | 5            | こんにちは |
    +----------+--------------+------------+
    "###);

    let query = "select test_int, test_nullint, test_str from test_table where test_int >= 2";
    let results = rewrite::query_one(conn.deref_mut(), &query).unwrap();
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

    let query = "select 1 + test_int as a from test_table order by test_int limit 3";
    let results = rewrite::query_one(conn.deref_mut(), &query).unwrap();
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
