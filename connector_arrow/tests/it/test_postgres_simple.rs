use connector_arrow::postgres::{PostgresConnection, ProtocolSimple};
use rstest::*;

use crate::spec;
use crate::test_postgres_common::literals_cases;
use crate::util::QueryOfSingleLiteral;

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
#[case::timestamp("roundtrip::timestamp", spec::timestamp())]
#[case::date("roundtrip::date", spec::date())]
#[case::time("roundtrip::time", spec::time())]
#[case::duration("roundtrip::duration", spec::duration())]
// #[case::interval("roundtrip::interval", spec::interval())]
#[case::utf8("roundtrip::utf8", spec::utf8_large())]
#[case::binary("roundtrip::binary", spec::binary_large())]
fn roundtrip(#[case] table_name: &str, #[case] spec: spec::ArrowGenSpec) {
    let mut conn = init();
    let table_name = format!("simple::{table_name}");
    super::tests::roundtrip(&mut conn, &table_name, spec, '"', false);
}

#[rstest]
#[case::bool(literals_cases::bool())]
#[case::int(literals_cases::int())]
#[case::float(literals_cases::float())]
#[case::decimal(literals_cases::decimal())]
// #[case::timestamp(literals_cases::timestamp())]
// #[case::date(literals_cases::date())]
// #[case::time(literals_cases::time())]
// #[case::interval(literals_cases::interval())]
#[case::text(literals_cases::text())]
// #[case::binary(literals_cases::binary())]
fn query_literals(#[case] queries: Vec<QueryOfSingleLiteral>) {
    let mut conn = init();
    crate::util::query_literals(&mut conn, queries)
}
