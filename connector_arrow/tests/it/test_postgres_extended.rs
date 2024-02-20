use connector_arrow::postgres::{PostgresConnection, ProtocolExtended};

fn init() -> postgres::Client {
    let _ = env_logger::builder().is_test(true).try_init();

    let dburl = std::env::var("POSTGRES_URL").unwrap();
    postgres::Client::connect(&dburl, postgres::NoTls).unwrap()
}

fn wrap_conn(client: &mut postgres::Client) -> PostgresConnection<ProtocolExtended> {
    PostgresConnection::new(client)
}

#[test]
fn query_01() {
    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    super::tests::query_01(&mut conn);
}

#[test]
fn roundtrip_empty() {
    let table_name = "extended::roundtrip_empty";

    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    let column_spec = super::generator::spec_empty();
    super::tests::roundtrip(&mut conn, table_name, column_spec);
}

#[test]
fn roundtrip_null_bool() {
    let table_name = "extended::roundtrip_null_bool";

    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    let column_spec = super::generator::spec_null_bool();
    super::tests::roundtrip(&mut conn, table_name, column_spec);
}

#[test]
fn schema_get() {
    let table_name = "extended::schema_get";

    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    let column_spec = super::generator::spec_all_types();
    super::tests::schema_get(&mut conn, table_name, column_spec);
}

#[test]
fn schema_edit() {
    let table_name = "extended::schema_edit";

    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    let column_spec = super::generator::spec_all_types();
    super::tests::schema_edit(&mut conn, table_name, column_spec);
}
