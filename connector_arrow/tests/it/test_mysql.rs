use connector_arrow::mysql::MySQLConnection;
use rstest::*;

use crate::{spec, util::QueryOfSingleLiteral};

fn init() -> MySQLConnection<mysql::Conn> {
    let _ = env_logger::builder().is_test(true).try_init();

    let url = std::env::var("MYSQL_URL").unwrap();
    let conn = mysql::Conn::new(url.as_str()).unwrap();
    MySQLConnection::new(conn)
}

#[test]
fn query_01() {
    let mut conn = init();
    super::tests::query_01(&mut conn);
}

#[test]
fn schema_get() {
    let table_name = "schema_get";

    let mut conn = init();
    let column_spec = super::spec::basic_types();
    super::tests::schema_get(&mut conn, table_name, column_spec);
}

#[test]
fn schema_edit() {
    let table_name = "schema_edit";

    let mut conn = init();
    let column_spec = super::spec::basic_types();
    super::tests::schema_edit(&mut conn, table_name, column_spec);
}

#[test]
fn ident_escaping() {
    // https://github.com/blackbeam/rust_mysql_common/issues/129
    let table_name = "ident_escaping";

    let mut conn = init();
    super::tests::ident_escaping(&mut conn, table_name);
}

#[rstest]
#[case::empty("roundtrip__empty", spec::empty())]
#[case::null_bool("roundtrip__null_bool", spec::null_bool())]
#[case::int("roundtrip__int", spec::int())]
#[case::uint("roundtrip__uint", spec::uint())]
#[case::float("roundtrip__float", spec::float())]
#[case::decimal("roundtrip__decimal", spec::decimal())]
// #[case::timestamp("roundtrip__timestamp", spec::timestamp())]
// #[case::date("roundtrip__date", spec::date())]
// #[case::time("roundtrip__time", spec::time())]
// #[case::duration("roundtrip__duration", spec::duration())]
// #[case::interval("roundtrip__interval", spec::interval())]
#[case::utf8("roundtrip__utf8", spec::utf8())]
#[case::binary("roundtrip__binary", spec::binary())]
fn roundtrip(#[case] table_name: &str, #[case] spec: spec::ArrowGenSpec) {
    let mut conn = init();
    super::tests::roundtrip(&mut conn, table_name, spec, '`', true);
}

#[rstest]
#[case::strings(literals_cases::strings())]
#[case::decimals(literals_cases::decimals())]
fn query_literals(#[case] queries: Vec<QueryOfSingleLiteral>) {
    let mut conn = init();
    crate::util::query_literals(&mut conn, queries)
}

/// These tests cases are used to test of querying of Postgres-native types
/// that cannot be obtained by converting Arrow into PostgreSQL.
#[allow(dead_code)]
mod literals_cases {
    use crate::util::QueryOfSingleLiteral;

    pub fn strings() -> Vec<QueryOfSingleLiteral> {
        vec![("char", "'hello'", "hello".to_string()).into()]
    }

    pub fn decimals() -> Vec<QueryOfSingleLiteral> {
        vec![
            ("DECIMAL", "1000.33333", "1000".to_string()).into(),
            ("DECIMAL(10, 2)", "1000.33333", "1000.33".to_string()).into(),
        ]
    }
}
