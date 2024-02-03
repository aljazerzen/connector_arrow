use arrow::util::pretty::pretty_format_batches;
use connector_arrow::rewrite;
use insta::assert_display_snapshot;

#[test]
fn test_sqlite() {
    env_logger::init();

    log::debug!("main");

    let source = rewrite::sqlite::SQLiteSource::new("../dbs/sqlite.db", 2).unwrap();

    log::debug!("source");

    let query = "select test_int, test_nullint, test_str from test_table where test_int < 2";
    let results = rewrite::query_one(&source, &query).unwrap();
    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +----------+--------------+------------+
    | test_int | test_nullint | test_str   |
    +----------+--------------+------------+
    | 1        | 3            | str1       |
    | 0        | 5            | こんにちは |
    +----------+--------------+------------+
    "###);

    let query = "select test_int, test_nullint, test_str from test_table where test_int >= 2";
    let results = rewrite::query_one(&source, &query).unwrap();
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
    let results = rewrite::query_one(&source, &query).unwrap();
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
