use connector_arrow::postgres::{PostgresConnection, ProtocolSimple};
use rstest::*;

use super::spec;

fn init() -> PostgresConnection<ProtocolSimple> {
    let _ = env_logger::builder().is_test(true).try_init();

    let dburl = std::env::var("POSTGRES_URL").unwrap();
    let client = postgres::Client::connect(&dburl, postgres::NoTls).unwrap();
    PostgresConnection::new(client)
}

#[test]
fn query_01() {
    let mut conn = init();
    super::tests::query_01(&mut conn);
}

#[test]
fn query_02() {
    let mut conn = init();
    super::tests::query_02(&mut conn);
}

#[rstest]
#[case::empty("simple::roundtrip::empty", spec::empty())]
#[case::null_bool("simple::roundtrip::null_bool", spec::null_bool())]
#[case::int("simple::roundtrip::int", spec::int())]
#[case::uint("simple::roundtrip::uint", spec::uint())]
#[case::float("simple::roundtrip::float", spec::float())]
#[case::decimal("simple::roundtrip::decimal", spec::decimal())]
fn roundtrip(#[case] table_name: &str, #[case] spec: spec::ArrowGenSpec) {
    let mut conn = init();
    super::tests::roundtrip(&mut conn, table_name, spec);
}

#[test]
fn schema_get() {
    let table_name = "simple::schema_get";

    let mut conn = init();
    let column_spec = super::spec::all_types();
    super::tests::schema_get(&mut conn, table_name, column_spec);
}

#[test]
fn schema_edit() {
    let table_name = "simple::schema_edit";

    let mut conn = init();
    let column_spec = super::spec::all_types();
    super::tests::schema_edit(&mut conn, table_name, column_spec);
}
#[test]
fn ident_escaping() {
    let table_name = "simple::ident_escaping";

    let mut conn = init();
    super::tests::ident_escaping(&mut conn, table_name);
}
