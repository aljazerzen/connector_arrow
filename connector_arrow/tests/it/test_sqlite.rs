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
fn roundtrip_numeric() {
    let table_name = "roundtrip_number";
    let file_name = "numeric.parquet";

    let mut conn = init();
    super::tests::roundtrip_of_parquet(&mut conn, file_name, table_name);
}

#[test]
#[ignore]
fn roundtrip_temporal() {
    let table_name = "roundtrip_temporal";
    let file_name = "temporal.parquet";

    let mut conn = init();
    super::tests::roundtrip_of_parquet(&mut conn, file_name, table_name);
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
