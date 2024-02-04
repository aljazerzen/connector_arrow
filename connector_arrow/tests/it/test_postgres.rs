use arrow::util::pretty::pretty_format_batches;
use connector_arrow::rewrite::postgres::{CursorProtocol, PostgresConnection, SimpleProtocol};
use connector_arrow::rewrite::query_one;
use connector_arrow::sources::postgres::rewrite_tls_args;
use insta::assert_display_snapshot;
use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use std::env;
use url::Url;

fn init() -> r2d2::Pool<PostgresConnectionManager<NoTls>> {
    let _ = env_logger::builder().is_test(true).try_init();

    let dburl = env::var("POSTGRES_URL").unwrap();

    let url = Url::parse(dburl.as_str()).unwrap();
    let (config, _tls) = rewrite_tls_args(&url).unwrap();

    let manager = PostgresConnectionManager::new(config, NoTls);

    Pool::builder().max_size(5).build(manager).unwrap()
}

#[test]
fn test_protocol_simple() {
    let pool = init();
    let mut conn = pool.get().unwrap();
    let mut conn = PostgresConnection::<SimpleProtocol>::new(&mut conn);

    let query = "select * from test_table";
    let results = query_one(&mut conn, query).unwrap();

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
    let pool = init();
    let mut conn = pool.get().unwrap();
    let mut conn = PostgresConnection::<CursorProtocol>::new(&mut conn);

    let query = "select * from test_table";
    let results = query_one(&mut conn, query).unwrap();

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
