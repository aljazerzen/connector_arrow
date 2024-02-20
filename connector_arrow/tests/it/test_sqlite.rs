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
fn roundtrip_basic_small() {
    let table_name = "roundtrip_basic_small";
    let file_name = "basic_small.parquet";

    let mut conn = init();
    super::tests::roundtrip_of_parquet(&mut conn, file_name, table_name);
}

#[test]
#[ignore] // SQLite cannot infer schema from an empty response, as there is no rows to infer from
fn roundtrip_empty() {
    let table_name = "roundtrip_empty";
    let file_name = "empty.parquet";

    let mut conn = init();
    super::tests::roundtrip_of_parquet(&mut conn, file_name, table_name);
}

#[test]
fn roundtrip_simple() {
    let table_name = "roundtrip_simple";

    let mut conn = init();
    let column_spec = super::generator::spec_simple();
    super::tests::roundtrip_of_generated(&mut conn, table_name, column_spec);
}

#[test]
fn roundtrip_numeric() {
    let table_name = "roundtrip_numeric";

    let mut conn = init();
    let column_spec = super::generator::spec_numeric();
    super::tests::roundtrip_of_generated(&mut conn, table_name, column_spec);
}

#[test]
#[ignore]
fn roundtrip_timestamp() {
    let table_name = "roundtrip_timestamp";

    let mut conn = init();
    let column_spec = super::generator::spec_timestamp();
    super::tests::roundtrip_of_generated(&mut conn, table_name, column_spec);
}

#[test]
#[ignore]
fn roundtrip_date() {
    let table_name = "roundtrip_date";

    let mut conn = init();
    let column_spec = super::generator::spec_date();
    super::tests::roundtrip_of_generated(&mut conn, table_name, column_spec);
}

#[test]
#[ignore]
fn roundtrip_time() {
    let table_name = "roundtrip_time";

    let mut conn = init();
    let column_spec = super::generator::spec_time();
    super::tests::roundtrip_of_generated(&mut conn, table_name, column_spec);
}

#[test]
#[ignore]
fn roundtrip_duration() {
    let table_name = "roundtrip_duration";

    let mut conn = init();
    let column_spec = super::generator::spec_duration();
    super::tests::roundtrip_of_generated(&mut conn, table_name, column_spec);
}

#[test]
#[ignore]
fn roundtrip_interval() {
    let table_name = "roundtrip_interval";

    let mut conn = init();
    let column_spec = super::generator::spec_interval();
    super::tests::roundtrip_of_generated(&mut conn, table_name, column_spec);
}

#[test]
#[ignore] // cannot introspect the Null column
fn introspection_basic_small() {
    let table_name = "introspection_basic_small";
    let file_name = "basic_small.parquet";

    let mut conn = init();
    super::tests::introspection(&mut conn, file_name, table_name);
}

#[test]
fn schema_edit_01() {
    let table_name = "schema_edit_01";
    let file_name = "basic_small.parquet";

    let mut conn = init();
    super::tests::schema_edit(&mut conn, file_name, table_name);
}
