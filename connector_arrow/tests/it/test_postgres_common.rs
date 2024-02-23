use connector_arrow::postgres::{PostgresConnection, ProtocolSimple};

fn init() -> PostgresConnection<ProtocolSimple> {
    let _ = env_logger::builder().is_test(true).try_init();

    let dburl = std::env::var("POSTGRES_URL").unwrap();
    let client = postgres::Client::connect(&dburl, postgres::NoTls).unwrap();
    PostgresConnection::new(client)
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

pub mod literals_cases {
    use crate::util::QueryOfSingleLiteral;

    pub fn bool() -> Vec<QueryOfSingleLiteral> {
        vec![
            ("boolean", "false", false).into(),
            ("boolean", "true", true).into(),
        ]
    }

    pub fn int() -> Vec<QueryOfSingleLiteral> {
        vec![
            ("smallint", "-32768", -32768_i16).into(),
            ("smallint", "32767", 32767_i16).into(),
            ("integer", "-2147483648", -2147483648_i32).into(),
            ("integer", "2147483647", 2147483647_i32).into(),
            ("bigint", "-9223372036854775808", -9223372036854775808_i64).into(),
            ("bigint", "9223372036854775807", 9223372036854775807_i64).into(),
        ]
    }

    pub fn float() -> Vec<QueryOfSingleLiteral> {
        vec![
            ("real", f32::MIN, f32::MIN).into(),
            ("real", f32::MIN_POSITIVE, f32::MIN_POSITIVE).into(),
            ("real", f32::MAX, f32::MAX).into(),
            ("real", "'Infinity'", f32::INFINITY).into(),
            ("real", "'-Infinity'", f32::NEG_INFINITY).into(),
            ("real", "'NaN'", f32::NAN).into(),
            ("double precision", f64::MIN, f64::MIN).into(),
            ("double precision", f64::MIN_POSITIVE, f64::MIN_POSITIVE).into(),
            ("double precision", f64::MAX, f64::MAX).into(),
            ("double precision", "'Infinity'", f64::INFINITY).into(),
            ("double precision", "'-Infinity'", f64::NEG_INFINITY).into(),
            ("double precision", "'NaN'", f64::NAN).into(),
        ]
    }

    pub fn decimal() -> Vec<QueryOfSingleLiteral> {
        let precision_272 = "100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234.44100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234100234";
        vec![
            ("numeric", "100234.44", "100234.44".to_string()).into(),
            ("numeric", "-100234", "-100234".to_string()).into(),
            ("numeric", "0100234.4400", "100234.4400".to_string()).into(),
            ("numeric", precision_272, precision_272.to_string()).into(),
            ("numeric", "'Infinity'", "Infinity".to_string()).into(),
            ("numeric", "'-Infinity'", "-Infinity".to_string()).into(),
            ("numeric", "'NaN'", "NaN".to_string()).into(),
        ]
    }

    pub fn timestamp() -> Vec<QueryOfSingleLiteral> {
        vec![
            (
                "timestamp",
                "TIMESTAMP '2024-02-23 15:18:36'",
                1708701516000000_i64,
            )
                .into(),
            (
                "timestamp without time zone",
                "TIMESTAMP '2024-02-23 15:18:36'",
                1708701516000000_i64,
            )
                .into(),
            (
                "timestamp with time zone",
                "TIMESTAMPTZ '2024-02-23 16:18:36 +01:00'",
                1708701516000000_i64,
            )
                .into(),
            (
                "timestamptz",
                "TIMESTAMP with time zone '2024-02-23 17:18:36 CEST'",
                1708701516000000_i64,
            )
                .into(),
            (
                "timestamp",
                "TIMESTAMP '4713-01-01 00:00:00 BC'",
                -210863520000000000_i64,
            )
                .into(),
            (
                "timestamp",
                "TIMESTAMP '294246-01-01 00:00:00'", // max possible year in pg is 294276
                9223339708800000000_i64,
            )
                .into(),
        ]
    }

    // TODO:
    // bit [ (n) ]
    // bit varying [ (n) ]
    // bytea
    //
    // character [ (n) ]
    // character varying [ (n) ]
    // text
    //
    // timestamp [ (p) ] [ without time zone ]
    // timestamp [ (p) ] with time zone
    // date
    // time [ (p) ] [ without time zone ]
    // time [ (p) ] with time zone
    // interval [ fields ] [ (p) ]
    //
    // point
    // box
    // circle
    // line
    // lseg
    // polygon
    // path
    //
    // cidr
    // inet
    // macaddr
    // macaddr8
    // money
    //
    // json
    // jsonb
    // xml
    // uuid
    //
    // tsquery
    // tsvector
    //
    // pg_lsn
    // pg_snapshot
    // txid_snapshot
    //
}
