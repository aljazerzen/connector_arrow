use connector_arrow::postgres::{PostgresConnection, ProtocolExtended};
use rstest::*;

use super::spec;

fn init() -> PostgresConnection<ProtocolExtended> {
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
#[case::empty("extended::roundtrip::empty", spec::empty())]
#[case::null_bool("extended::roundtrip::null_bool", spec::null_bool())]
#[case::int("extended::roundtrip::int", spec::int())]
#[case::uint("extended::roundtrip::uint", spec::uint())]
#[case::float("extended::roundtrip::float", spec::float())]
#[case::decimal("extended::roundtrip::decimal", spec::decimal())]
fn roundtrip(#[case] table_name: &str, #[case] spec: spec::ArrowGenSpec) {
    let mut conn = init();
    super::tests::roundtrip(&mut conn, table_name, spec);
}
