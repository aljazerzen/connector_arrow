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
#[case::empty("roundtrip::empty", spec::empty())]
#[case::null_bool("roundtrip::null_bool", spec::null_bool())]
#[case::int("roundtrip::int", spec::int())]
#[case::uint("roundtrip::uint", spec::uint())]
#[case::float("roundtrip::float", spec::float())]
#[case::decimal("roundtrip::decimal", spec::decimal())]
#[case::utf8("roundtrip::utf8", spec::utf8())]
#[case::binary("roundtrip::binary", spec::binary())]
fn roundtrip(#[case] table_name: &str, #[case] spec: spec::ArrowGenSpec) {
    let mut conn = init();
    let table_name = format!("simple::{table_name}");
    super::tests::roundtrip(&mut conn, &table_name, spec);
}

#[test]
fn schema_get() {
    let table_name = "simple::schema_get";

    let mut conn = init();
    let column_spec = super::spec::basic_types();
    super::tests::schema_get(&mut conn, table_name, column_spec);
}

#[test]
fn schema_edit() {
    let table_name = "simple::schema_edit";

    let mut conn = init();
    let column_spec = super::spec::basic_types();
    super::tests::schema_edit(&mut conn, table_name, column_spec);
}

#[test]
fn ident_escaping() {
    let table_name = "simple::ident_escaping";

    let mut conn = init();
    super::tests::ident_escaping(&mut conn, table_name);
}
