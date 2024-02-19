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
fn roundtrip_basic_small() {
    let table_name = "extended::roundtrip_basic_small";
    let file_name = "basic_small.parquet";

    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    super::tests::roundtrip_of_parquet(&mut conn, file_name, table_name);
}

#[test]
fn roundtrip_empty() {
    let table_name = "extended::roundtrip_empty";
    let file_name = "empty.parquet";

    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    super::tests::roundtrip_of_parquet(&mut conn, file_name, table_name);
}

#[test]
fn introspection_basic_small() {
    let table_name = "extended::introspection_basic_small";
    let file_name = "basic_small.parquet";

    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    super::tests::introspection(&mut conn, file_name, table_name);
}

#[test]
fn schema_edit_01() {
    let table_name = "extended::schema_edit_01";
    let file_name = "basic_small.parquet";

    let mut client = init();
    let mut conn = wrap_conn(&mut client);
    super::tests::schema_edit(&mut conn, file_name, table_name);
}

#[test]
#[ignore]
fn streaming() {
    let mut client = init();
    let mut conn = wrap_conn(&mut client);

    super::tests::streaming(&mut conn);

    // This should be quick and not load the full result.
    // ... but I guess not - it takes a long time.
    // ... I don't know why. Maybe my impl is wrong, but I cannot find a reason why.
    // ... Maybe it is the postgres that hangs before returning the first result batch?
    // ... Maybe it tries to return the full result and not in batches?
}
