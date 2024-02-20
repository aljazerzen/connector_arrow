use super::spec;
use rstest::*;

fn init() -> connector_arrow::sqlite::SQLiteConnection {
    let _ = env_logger::builder().is_test(true).try_init();

    let conn = rusqlite::Connection::open_in_memory().unwrap();
    connector_arrow::sqlite::SQLiteConnection::new(conn)
}

#[test]
fn query_01() {
    let mut conn = init();
    super::tests::query_01(&mut conn);
}

#[rstest]
// #[case::empty("roundtrip::empty", spec::empty())]
#[case::null_bool("roundtrip::null_bool", spec::null_bool())]
#[case::numeric("roundtrip::numeric", spec::numeric())]
fn roundtrip(#[case] table_name: &str, #[case] spec: spec::ArrowGenSpec) {
    let mut conn = init();
    super::tests::roundtrip(&mut conn, table_name, spec);
}

#[test]
#[ignore] // cannot introspect the Null column
fn schema_get() {
    let table_name = "schema_get";

    let mut conn = init();
    let column_spec = super::spec::all_types();
    super::tests::schema_get(&mut conn, table_name, column_spec);
}

#[test]
fn schema_edit() {
    let table_name = "schema_edit";

    let mut conn = init();
    let column_spec = super::spec::all_types();
    super::tests::schema_edit(&mut conn, table_name, column_spec);
}
