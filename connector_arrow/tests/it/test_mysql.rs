use connector_arrow::mysql::MySQLConnection;

fn init() -> MySQLConnection<mysql::Conn> {
    let _ = env_logger::builder().is_test(true).try_init();

    let url = std::env::var("MYSQL_URL").unwrap();
    let conn = mysql::Conn::new(url.as_str()).unwrap();
    MySQLConnection::new(conn)
}

#[test]
fn query_01() {
    let mut conn = init();
    super::tests::query_01(&mut conn);
}
