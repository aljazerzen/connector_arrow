use std::sync::Arc;

use connector_arrow::tiberius::TiberiusConnection;
use rstest::rstest;
use tiberius::{AuthMethod, Client, Config};
use tokio::{net::TcpStream, runtime};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::{spec, util::QueryOfSingleLiteral};

fn init() -> TiberiusConnection<Compat<TcpStream>> {
    let _ = env_logger::builder().is_test(true).try_init();

    let rt = Arc::new(
        runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    );

    let url = std::env::var("TIBERIUS_URL").unwrap();
    let url = url::Url::parse(&url).unwrap();

    let mut config = Config::new();
    config.host(url.host().unwrap());
    config.port(url.port().unwrap());
    config.authentication(AuthMethod::sql_server(
        url.username(),
        url.password().unwrap(),
    ));

    let addr = (url.host_str().unwrap(), url.port().unwrap());
    let tcp = rt.block_on(TcpStream::connect(addr)).unwrap();
    tcp.set_nodelay(true).unwrap();

    let client = Client::connect(config, tcp.compat_write());
    let client = rt.block_on(client).unwrap();

    TiberiusConnection::new(rt, client)
}

#[test]
fn query_01() {
    let mut conn = init();
    super::tests::query_01(&mut conn);
}

#[test]
#[ignore]
fn query_02() {
    let mut conn = init();
    super::tests::query_02(&mut conn);
}

#[test]
#[ignore]
fn query_03() {
    let mut conn = init();
    super::tests::query_03(&mut conn);
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
#[ignore]
fn ident_escaping() {
    let table_name = "simple::ident_escaping";

    let mut conn = init();
    super::tests::ident_escaping(&mut conn, table_name);
}

#[rstest]
#[case::empty("roundtrip::empty", spec::empty())]
#[case::null_bool("roundtrip::null_bool", spec::null_bool())]
#[case::int("roundtrip::int", spec::int())]
#[case::uint("roundtrip::uint", spec::uint())]
#[case::float("roundtrip::float", spec::float())]
#[case::decimal("roundtrip::decimal", spec::decimal())]
#[case::timestamp("roundtrip::timestamp", spec::timestamp())]
// #[case::date("roundtrip::date", spec::date())]
// #[case::time("roundtrip::time", spec::time())]
// #[case::duration("roundtrip::duration", spec::duration())]
// #[case::interval("roundtrip::interval", spec::interval())]
#[case::utf8("roundtrip::utf8", spec::utf8_large())]
// #[case::binary("roundtrip::binary", spec::binary_large())]
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

/// These tests cases are used to test of querying of Postgres-native types
/// that cannot be obtained by converting Arrow into PostgreSQL.
#[allow(dead_code)]
mod literals_cases {
    use arrow::datatypes::{DataType, IntervalMonthDayNano, IntervalUnit, TimeUnit};

    use crate::util::QueryOfSingleLiteral;

    pub fn bool() -> Vec<QueryOfSingleLiteral> {
        vec![("bit", "0", false).into(), ("bit", "1", true).into()]
    }

    pub fn int() -> Vec<QueryOfSingleLiteral> {
        vec![
            ("tinyint", "0", 0_u8).into(),
            ("tinyint", "255", 255_u8).into(),
            ("smallint", "-32768", -32768_i16).into(),
            ("smallint", "32767", 32767_i16).into(),
            ("int", "-2147483648", -2147483648_i32).into(),
            ("int", "2147483647", 2147483647_i32).into(),
            ("bigint", "-9223372036854775808", -9223372036854775808_i64).into(),
            ("bigint", "9223372036854775807", 9223372036854775807_i64).into(),
        ]
    }

    pub fn float() -> Vec<QueryOfSingleLiteral> {
        vec![
            (
                "real",
                "-34000000000000000000000000000000000000",
                -34000000000000000000000000000000000000_f32,
            )
                .into(),
            (
                "real",
                "-0.00000000000000000000000000000000000118",
                -0.00000000000000000000000000000000000118_f32,
            )
                .into(),
            ("real", "0", 0_f32).into(),
            (
                "real",
                "0.00000000000000000000000000000000000118",
                0.00000000000000000000000000000000000118_f32,
            )
                .into(),
            (
                "real",
                "34000000000000000000000000000000000000",
                34000000000000000000000000000000000000_f32,
            )
                .into(),
            (
                "float",
                "-34000000000000000000000000000000000000",
                -34000000000000000000000000000000000000_f64,
            )
                .into(),
            (
                "float",
                "-0.00000000000000000000000000000000000118",
                -0.00000000000000000000000000000000000118_f64,
            )
                .into(),
            (
                "float",
                "34000000000000000000000000000000000000",
                34000000000000000000000000000000000000_f64,
            )
                .into(),
        ]
    }

    pub fn decimal() -> Vec<QueryOfSingleLiteral> {
        let precision_38 = "10023410023410023410023410023410023410";
        vec![
            ("numeric(8, 2)", "100234.44", "100234.44".to_string()).into(),
            ("numeric(6, 0)", "-100234", "-100234".to_string()).into(),
            ("numeric(11, 4)", "0100234.4400", "100234.4400".to_string()).into(),
            ("numeric(38, 0)", precision_38, precision_38.to_string()).into(),
            ("numeric(11, 4)", "-100234.4400", "-100234.4400".to_string()).into(),
            ("numeric(3, 2)", "-0.2", "-0.20".to_string()).into(),
            ("numeric(3, 3)", "-0.2", "-0.200".to_string()).into(),
            ("numeric(3, 2)", "0.2", "0.20".to_string()).into(),
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
            // ("text", "'ok'", "ok".to_string()).into(),
            // ("ntext", "'ok'", "ok".to_string()).into(),
            ("char(6)", "'hello'", "hello ".to_string()).into(),
            ("nchar(6)", "'hello'", "hello ".to_string()).into(),
            ("varchar(6)", "'world'", "world".to_string()).into(),
            ("nvarchar(6)", "'world'", "world".to_string()).into(),
        ]
    }

    pub fn network_addr() -> Vec<QueryOfSingleLiteral> {
        vec![
            (
                "cidr",
                "'192.168/25'",
                vec![
                    2, // family: IPv4
                    25, // netmask
                    1, // is cidr
                    4, // length
                    192, 168, 0, 0,
                ],
            )
                .into(),
            (
                "cidr",
                "'2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128'",
                vec![
                    3,  // family: IPv6
                    128, // netmask
                    1,  // is cidr
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
                    2, // family: IPv4
                    24, // netmask
                    0, // is cidr
                    4, // length
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
