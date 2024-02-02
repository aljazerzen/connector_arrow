use connector_arrow::rewrite;

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

    rewrite::query_many(&source, &queries).unwrap()
}
