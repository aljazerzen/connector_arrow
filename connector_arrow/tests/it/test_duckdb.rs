use super::spec;
use rstest::*;

fn init() -> connector_arrow::duckdb::DuckDBConnection {
    let _ = env_logger::builder().is_test(true).try_init();

    let conn = duckdb::Connection::open_in_memory().unwrap();
    connector_arrow::duckdb::DuckDBConnection::new(conn)
}

#[test]
fn query_01() {
    let mut conn = init();
    super::tests::query_01(&mut conn);
}

#[test]
#[ignore]
fn query_02() {
    let mut conn = init();
    super::tests::query_02(&mut conn);
}

#[test]
fn query_03() {
    let mut conn = init();
    super::tests::query_03(&mut conn);
}

#[rstest]
#[case::empty("roundtrip::empty", spec::empty())]
#[case::null_bool("roundtrip::null_bool", spec::null_bool())]
#[case::int("roundtrip::int", spec::int())]
#[case::uint("roundtrip::uint", spec::uint())]
#[case::float("roundtrip::float", spec::float())]
// #[case::decimal("roundtrip::decimal", spec::decimal())]
#[case::timestamp("roundtrip::timestamp", spec::timestamp())]
// #[case::date("roundtrip::date", spec::date())]
// #[case::time("roundtrip::time", spec::time())]
// #[case::duration("roundtrip::duration", spec::duration())]
// #[case::interval("roundtrip::interval", spec::interval())]
#[case::utf8("roundtrip::utf8", spec::utf8_large())]
#[case::binary("roundtrip::binary", spec::binary_large())]
fn roundtrip(#[case] table_name: &str, #[case] spec: spec::ArrowGenSpec) {
    let mut conn = init();
    super::tests::roundtrip(&mut conn, table_name, spec, '"', false);
}

#[test]
fn schema_get() {
    let table_name = "schema_get";

    let mut conn = init();
    super::tests::schema_get(&mut conn, table_name, spec::basic_types());
}

#[test]
fn schema_edit() {
    let table_name = "schema_edit";

    let mut conn = init();
    super::tests::schema_edit(&mut conn, table_name, spec::basic_types());
}

#[test]
fn ident_escaping() {
    let table_name = "simple::ident_escaping";

    let mut conn = init();
    super::tests::ident_escaping(&mut conn, table_name);
}
