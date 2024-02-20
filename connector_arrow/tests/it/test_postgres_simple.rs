use connector_arrow::postgres::{PostgresConnection, ProtocolSimple};
use rstest::*;

use super::spec;

fn init() -> postgres::Client {
    let _ = env_logger::builder().is_test(true).try_init();

    let dburl = std::env::var("POSTGRES_URL").unwrap();
    postgres::Client::connect(&dburl, postgres::NoTls).unwrap()
}

fn wrap_conn(client: &mut postgres::Client) -> PostgresConnection<ProtocolSimple> {
    PostgresConnection::new(client)
}

#[test]
fn query_01() {
    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    super::tests::query_01(&mut conn);
}

#[rstest]
#[case::empty("simple::roundtrip::empty", spec::empty())]
#[case::null_bool("simple::roundtrip::null_bool", spec::null_bool())]
#[case::numeric("simple::roundtrip::numeric", spec::numeric())]
fn roundtrip(#[case] table_name: &str, #[case] spec: spec::ArrowGenSpec) {
    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    super::tests::roundtrip(&mut conn, table_name, spec);
}

#[test]
fn schema_get() {
    let table_name = "simple::schema_get";

    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    let column_spec = super::spec::all_types();
    super::tests::schema_get(&mut conn, table_name, column_spec);
}

#[test]
fn schema_edit() {
    let table_name = "simple::schema_edit";

    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    let column_spec = super::spec::all_types();
    super::tests::schema_edit(&mut conn, table_name, column_spec);
}
