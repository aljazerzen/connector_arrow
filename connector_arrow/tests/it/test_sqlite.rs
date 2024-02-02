use arrow::util::pretty::pretty_format_batches;
use connector_arrow::rewrite;
use insta::assert_display_snapshot;

#[test]
fn test_sqlite() {
    env_logger::init();

    log::debug!("main");

    let source = rewrite::sqlite::SQLiteSource::new("../dbs/sqlite.db", 2).unwrap();

    log::debug!("source");

    let queries = [
        "select test_int, test_nullint, test_str from test_table where test_int < 2",
        "select test_int, test_nullint, test_str from test_table where test_int >= 2",
        "select 1 as a from test_table",
    ];

    let results = rewrite::query_many(&source, &queries).unwrap();

    assert_display_snapshot!(pretty_format_batches(&results[0]).unwrap(), @r###"
    +----------+--------------+------------+
    | test_int | test_nullint | test_str   |
    +----------+--------------+------------+
    | 1        | 3            | str1       |
    | 0        | 5            | こんにちは |
    +----------+--------------+------------+
    "###);

    assert_display_snapshot!(pretty_format_batches(&results[1]).unwrap(), @r###"
    +----------+--------------+------------+
    | test_int | test_nullint | test_str   |
    +----------+--------------+------------+
    | 2        |              | str2       |
    | 3        | 7            | b          |
    | 4        | 9            | Ha好ち😁ðy̆ |
    | 1314     | 2            |            |
    +----------+--------------+------------+
    "###);

    assert_display_snapshot!(pretty_format_batches(&results[2]).unwrap(), @r###"
    +---+
    | a |
    +---+
    | 1 |
    | 1 |
    | 1 |
    | 1 |
    | 1 |
    | 1 |
    +---+
    "###);
}
