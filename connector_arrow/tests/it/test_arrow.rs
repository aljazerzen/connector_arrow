use arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches};
use connector_arrow::{
    constants::RECORD_BATCH_SIZE,
    destinations::arrow::{ArrowDestination, ArrowTypeSystem},
    prelude::*,
    sources::{
        dummy::{DummySource, DummyTypeSystem},
        postgres::{rewrite_tls_args, BinaryProtocol, PostgresSource},
    },
    sql::CXQuery,
    transports::{DummyArrowTransport, PostgresArrowTransport},
    typesystem::Schema,
};
use insta::assert_display_snapshot;
use postgres::NoTls;
use std::env;
use url::Url;

#[test]
fn arrow_destination_col_major() {
    let mut destination = ArrowDestination::new();
    let schema = Schema {
        names: vec!["a".into(), "b".into(), "c".into()],
        types: vec![
            ArrowTypeSystem::Int64(false),
            ArrowTypeSystem::Float64(true),
            ArrowTypeSystem::LargeUtf8(true),
        ],
    };
    destination.set_schema(schema).unwrap();
    {
        let mut writer = destination.get_writer(DataOrder::ColumnMajor).unwrap();

        writer.prepare_for_batch(2).unwrap();
        writer.consume(1_i64).unwrap();
        writer.consume(2_i64).unwrap();

        writer.consume(Some(0.5_f64)).unwrap();
        writer.consume(None as Option<f64>).unwrap();

        writer.consume(Some("hello".to_string())).unwrap();
        writer.consume(None as Option<String>).unwrap();

        writer.finalize().unwrap();
    }

    let results = destination.finish().unwrap();
    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +---+-----+-------+
    | a | b   | c     |
    +---+-----+-------+
    | 1 | 0.5 | hello |
    | 2 |     |       |
    +---+-----+-------+
    "###);
}

#[test]
fn test_arrow() {
    let schema = [
        DummyTypeSystem::I64(true),
        DummyTypeSystem::F64(true),
        DummyTypeSystem::Bool(false),
        DummyTypeSystem::String(true),
        DummyTypeSystem::F64(false),
    ];
    let nrows = [4, 7];
    let ncols = schema.len();
    let queries: Vec<CXQuery> = nrows
        .iter()
        .map(|v| CXQuery::naked(format!("{},{}", v, ncols)))
        .collect();
    let mut destination = ArrowDestination::new();

    let dispatcher = Dispatcher::<_, _, DummyArrowTransport>::new(
        DummySource::new(&["a", "b", "c", "d", "e"], &schema),
        &mut destination,
        &queries,
    );
    dispatcher.run().expect("run dispatcher");

    let mut results: Vec<RecordBatch> = destination.finish().unwrap();
    results.sort_by_key(|r| r.num_rows());
    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +---+-----+-------+---+-----+
    | a | b   | c     | d | e   |
    +---+-----+-------+---+-----+
    | 0 | 0.0 | true  | 0 | 0.0 |
    | 1 | 1.0 | false | 1 | 1.0 |
    | 2 | 2.0 | true  | 2 | 2.0 |
    | 3 | 3.0 | false | 3 | 3.0 |
    | 0 | 0.0 | true  | 0 | 0.0 |
    | 1 | 1.0 | false | 1 | 1.0 |
    | 2 | 2.0 | true  | 2 | 2.0 |
    | 3 | 3.0 | false | 3 | 3.0 |
    | 4 | 4.0 | true  | 4 | 4.0 |
    | 5 | 5.0 | false | 5 | 5.0 |
    | 6 | 6.0 | true  | 6 | 6.0 |
    +---+-----+-------+---+-----+
    "###);
}

#[test]
fn test_arrow_large() {
    let schema = [
        DummyTypeSystem::I64(true),
        DummyTypeSystem::F64(true),
        DummyTypeSystem::Bool(false),
        DummyTypeSystem::String(true),
        DummyTypeSystem::F64(false),
    ];
    let nrows = [RECORD_BATCH_SIZE * 2 + 1, RECORD_BATCH_SIZE * 2 - 1];
    let ncols = schema.len();
    let queries = [
        CXQuery::naked(format!("{},{}", nrows[0], ncols)),
        CXQuery::naked(format!("{},{}", nrows[1], ncols)),
    ];
    let mut destination = ArrowDestination::new();

    let dispatcher = Dispatcher::<_, _, DummyArrowTransport>::new(
        DummySource::new(&["a", "b", "c", "d", "e"], &schema),
        &mut destination,
        &queries,
    );
    dispatcher.run().expect("run dispatcher");

    let records: Vec<RecordBatch> = destination.finish().unwrap();
    let mut rsizes = vec![];
    for r in records {
        rsizes.push(r.num_rows());
    }
    rsizes.sort();
    rsizes.reverse();
    assert_eq!(&nrows, rsizes.as_slice());
}

#[test]
fn test_postgres_arrow() {
    let _ = env_logger::builder().is_test(true).try_init();

    let dburl = env::var("POSTGRES_URL").unwrap();

    let queries = [
        CXQuery::naked("select * from test_table where test_int < 2"),
        CXQuery::naked("select * from test_table where test_int >= 2"),
    ];
    let url = Url::parse(dburl.as_str()).unwrap();
    let (config, _tls) = rewrite_tls_args(&url).unwrap();
    let builder = PostgresSource::<BinaryProtocol, NoTls>::new(config, NoTls, 2).unwrap();
    let mut destination = ArrowDestination::new();
    let dispatcher = Dispatcher::<_, _, PostgresArrowTransport<BinaryProtocol, NoTls>>::new(
        builder,
        &mut destination,
        &queries,
    );

    dispatcher.run().expect("run dispatcher");

    let mut results: Vec<RecordBatch> = destination.finish().unwrap();
    results.sort_by_key(|r| r.num_rows());
    assert_display_snapshot!(pretty_format_batches(&results).unwrap(), @r###"
    +----------+--------------+----------+------------+-----------+
    | test_int | test_nullint | test_str | test_float | test_bool |
    +----------+--------------+----------+------------+-----------+
    | 1        | 3            | str1     |            | true      |
    | 0        | 5            | a        | 3.1        |           |
    | 2        |              | str2     | 2.2        | false     |
    | 3        | 7            | b        | 3.0        | false     |
    | 4        | 9            | c        | 7.8        |           |
    | 1314     | 2            |          | -10.0      | true      |
    +----------+--------------+----------+------------+-----------+
    "###);
}
