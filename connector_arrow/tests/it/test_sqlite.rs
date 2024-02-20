fn init() -> rusqlite::Connection {
    let _ = env_logger::builder().is_test(true).try_init();

    rusqlite::Connection::open_in_memory().unwrap()
}

#[test]
fn query_01() {
    let mut conn = init();
    super::tests::query_01(&mut conn);
}

#[test]
#[ignore] // SQLite cannot infer schema from an empty response, as there is no rows to infer from
fn roundtrip_empty() {
    let table_name = "roundtrip_empty";

    let mut conn = init();
    let column_spec = super::generator::spec_empty();
    super::tests::roundtrip(&mut conn, table_name, column_spec);
}

#[test]
fn roundtrip_null_bool() {
    let table_name = "roundtrip_null_bool";

    let mut conn = init();
    let column_spec = super::generator::spec_null_bool();
    super::tests::roundtrip(&mut conn, table_name, column_spec);
}

#[test]
fn roundtrip_numeric() {
    let table_name = "roundtrip_numeric";

    let mut conn = init();
    let column_spec = super::generator::spec_numeric();
    super::tests::roundtrip(&mut conn, table_name, column_spec);
}

#[test]
#[ignore]
fn roundtrip_timestamp() {
    let table_name = "roundtrip_timestamp";

    let mut conn = init();
    let column_spec = super::generator::spec_timestamp();
    super::tests::roundtrip(&mut conn, table_name, column_spec);
}

#[test]
#[ignore]
fn roundtrip_date() {
    let table_name = "roundtrip_date";

    let mut conn = init();
    let column_spec = super::generator::spec_date();
    super::tests::roundtrip(&mut conn, table_name, column_spec);
}

#[test]
#[ignore]
fn roundtrip_time() {
    let table_name = "roundtrip_time";

    let mut conn = init();
    let column_spec = super::generator::spec_time();
    super::tests::roundtrip(&mut conn, table_name, column_spec);
}

#[test]
#[ignore]
fn roundtrip_duration() {
    let table_name = "roundtrip_duration";

    let mut conn = init();
    let column_spec = super::generator::spec_duration();
    super::tests::roundtrip(&mut conn, table_name, column_spec);
}

#[test]
#[ignore]
fn roundtrip_interval() {
    let table_name = "roundtrip_interval";

    let mut conn = init();
    let column_spec = super::generator::spec_interval();
    super::tests::roundtrip(&mut conn, table_name, column_spec);
}

#[test]
#[ignore] // cannot introspect the Null column
fn schema_get() {
    let table_name = "schema_get";

    let mut conn = init();
    let column_spec = super::generator::spec_all_types();
    super::tests::schema_get(&mut conn, table_name, column_spec);
}

#[test]
fn schema_edit() {
    let table_name = "schema_edit";

    let mut conn = init();
    let column_spec = super::generator::spec_all_types();
    super::tests::schema_edit(&mut conn, table_name, column_spec);
}
