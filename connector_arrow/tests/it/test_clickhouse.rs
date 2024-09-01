use connector_arrow::{
    api::{Connector, Statement},
    mysql::MySQLConnection,
};
use mysql::prelude::Queryable;

fn init() -> MySQLConnection<mysql::Conn> {
    let _ = env_logger::builder().is_test(true).try_init();

    let url = "mysql://default:@localhost:9004/dummy";
    let mut conn = mysql::Conn::new(url).unwrap();

    let query = "SELECT 'a', 2.3 UNION ALL SELECT 'b', 4.3";

    let res: Vec<(String, f64)> = conn.query(query).unwrap();
    dbg!(res);

    let stmt = conn.prep(query).unwrap();
    let res_prepared: Vec<(String, f64)> = conn.exec(stmt, ()).unwrap();
    dbg!(res_prepared);

    MySQLConnection::new(conn)
}

#[test]
fn query_01() {
    let mut conn = init();
    let mut stmt = conn
        .query(
            r#"
            SELECT 'a', 2.3 UNION ALL SELECT 'b', 4.3
            "#,
        )
        .unwrap();
    let reader = stmt.start([]).unwrap();
    let res = reader.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    dbg!(res);
    assert!(false)
}
