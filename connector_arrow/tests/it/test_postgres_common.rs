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

/// These tests cases are used to test of querying of Postgres-native types
/// that cannot be obtained by converting Arrow into PostgreSQL.
pub mod literals_cases {
    use arrow::datatypes::{DataType, IntervalMonthDayNano, IntervalUnit, TimeUnit};

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

    pub fn date() -> Vec<QueryOfSingleLiteral> {
        vec![
            ("date", "DATE '2024-02-23'", (DataType::Date32, 19776_i32)).into(),
            ("date", "'4713-01-01 BC'", (DataType::Date32, -2440550_i32)).into(),
            ("date", "'5874897-1-1'", (DataType::Date32, 2145042541_i32)).into(),
        ]
    }

    pub fn time() -> Vec<QueryOfSingleLiteral> {
        vec![
            (
                "time",
                "'17:18:36'",
                (DataType::Time64(TimeUnit::Microsecond), 62316000000_i64),
            )
                .into(),
            (
                "time without time zone",
                "'17:18:36.789'",
                (DataType::Time64(TimeUnit::Microsecond), 62316789000_i64),
            )
                .into(),
            // timetz not supported by postgres crate
            // (
            //     "time with time zone",
            //     "'17:18:36.789+01:00'",
            //     (DataType::Time64(TimeUnit::Microsecond), 65916789000_i64),
            // )
            //     .into(),
            // (
            //     "time with time zone",
            //     "'17:18:36.789 CEST'",
            //     (DataType::Time64(TimeUnit::Microsecond), 65916789000_i64),
            // )
            //     .into(),
        ]
    }

    pub fn interval() -> Vec<QueryOfSingleLiteral> {
        vec![
            (
                "interval",
                "'P12M3DT4H5M6S'",
                (
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    IntervalMonthDayNano {
                        months: 12,
                        days: 3,
                        nanoseconds: 0x00000d6001e7f400_i64,
                    },
                ),
            )
                .into(),
            (
                "interval",
                "'P-1Y-2M3DT-4H-5M-6S'",
                (
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    IntervalMonthDayNano {
                        months: -14,
                        days: 3,
                        nanoseconds: -14706000000000i64,
                    },
                ),
            )
                .into(),
        ]
    }

    pub fn binary() -> Vec<QueryOfSingleLiteral> {
        vec![
            ("bytea", "'\\xDEADBEEF'", vec![0xDE, 0xAD, 0xBE, 0xEF]).into(),
            ("bit(4)", "B'1011'", vec![0b10110000]).into(),
            ("bit varying(6)", "B'1011'", vec![0b10110000]).into(),
        ]
    }

    pub fn text() -> Vec<QueryOfSingleLiteral> {
        vec![
            ("text", "'ok'", "ok".to_string()).into(),
            ("char(6)", "'hello'", "hello ".to_string()).into(),
            ("varchar(6)", "'world'", "world".to_string()).into(),
            ("bpchar", "' nope  '", " nope  ".to_string()).into(),
        ]
    }

    pub fn network_addr() -> Vec<QueryOfSingleLiteral> {
        vec![
            (
                "cidr",
                "'192.168/25'",
                vec![
                    02, // family: IPv4
                    25, // netmask
                    01, // is cidr
                    04, // length
                    192, 168, 0, 0,
                ],
            )
                .into(),
            (
                "cidr",
                "'2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128'",
                vec![
                    03,  // family: IPv6
                    128, // netmask
                    01,  // is cidr
                    16,  // length
                    0x20, 0x01, 0x04, 0xf8, 0x00, 0x03, 0x00, 0xba, 0x02, 0xe0, 0x81, 0xff, 0xfe,
                    0x22, 0xd1, 0xf1,
                ],
            )
                .into(),
            (
                "inet",
                "'192.168.0.1/24'",
                vec![
                    02, // family: IPv4
                    24, // netmask
                    00, // is cidr
                    04, // length
                    192, 168, 0, 1,
                ],
            )
                .into(),
            (
                "macaddr",
                "'08:00:2b:01:02:03'",
                vec![0x08, 0x00, 0x2b, 0x01, 0x02, 0x03],
            )
                .into(),
            (
                "macaddr8",
                "'08:00:2b:01:02:03:04:05'",
                vec![0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05],
            )
                .into(),
        ]
    }

    // TODO:
    // point
    // box
    // circle
    // line
    // lseg
    // polygon
    // path
    //
    // money
    //
    // json
    // jsonb
    // xml
    // uuid
    //
    // tsquery
    // tsvector
    // pg_lsn
    // pg_snapshot
    // txid_snapshot
    //
}
