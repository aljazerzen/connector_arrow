use arrow::array::Int64Array;
use connector_arrow::prelude::*;
use connector_arrow::typesystem::Schema;
use connector_arrow::{
    destinations::arrow::{ArrowDestination, ArrowTypeSystem},
    sources::csv::{CSVSource, CSVTypeSystem},
    sql::CXQuery,
    transports::CSVArrowTransport,
};

#[test]
fn no_file() {
    let mut source = CSVSource::new(None);
    let query = CXQuery::naked("./a_fake_file.csv");
    let mut reader = source.reader(&query, DataOrder::RowMajor).unwrap();
    reader.fetch_until_schema().unwrap_err();
}

#[test]
#[ignore] // TODO: panic with division by zero
fn empty_file() {
    let mut source = CSVSource::new(None);
    let mut p = source
        .reader(
            &CXQuery::naked("./tests/data/empty.csv"),
            DataOrder::RowMajor,
        )
        .unwrap();

    let schema = p.fetch_until_schema().unwrap();

    let mut parser = p.value_stream(&schema).unwrap();

    parser.next_batch().unwrap();

    Produce::<i64>::produce(&mut parser).unwrap_err();
}

#[test]
fn load_and_parse() {
    #[derive(Debug, PartialEq)]
    enum Value {
        City(String),
        State(String),
        Population(i64),
        Longitude(f64),
        Latitude(f64),
    }

    let mut source = CSVSource::new(Some(&[
        CSVTypeSystem::String(false),
        CSVTypeSystem::String(false),
        CSVTypeSystem::I64(false),
        CSVTypeSystem::F64(false),
        CSVTypeSystem::F64(false),
    ]));

    let mut reader = source
        .reader(
            &CXQuery::naked("./tests/data/uspop_0.csv"),
            DataOrder::RowMajor,
        )
        .unwrap();

    let schema = reader.fetch_until_schema().unwrap();

    let mut results: Vec<Value> = Vec::new();
    let mut parser = reader.value_stream(&schema).unwrap();
    while let Some(n) = parser.next_batch().unwrap() {
        for _i in 0..n {
            results.push(Value::City(parser.produce().expect("parse city")));
            results.push(Value::State(parser.produce().expect("parse state")));
            results.push(Value::Population(
                parser.produce().expect("parse population"),
            ));
            results.push(Value::Longitude(parser.produce().expect("parse longitude")));
            results.push(Value::Latitude(parser.produce().expect("parse latitude")));
        }
    }
    assert_eq!(
        vec![
            Value::City(String::from("Kenai")),
            Value::State(String::from("AK")),
            Value::Population(7610),
            Value::Longitude(60.5544444),
            Value::Latitude(-151.2583333),
            Value::City(String::from("Selma")),
            Value::State(String::from("AL")),
            Value::Population(18980),
            Value::Longitude(32.4072222),
            Value::Latitude(-87.0211111),
            Value::City(String::from("El Mirage")),
            Value::State(String::from("AZ")),
            Value::Population(32308),
            Value::Longitude(33.6130556),
            Value::Latitude(-112.3238889)
        ],
        results
    );
}

#[test]
fn test_csv() {
    let schema = [CSVTypeSystem::I64(false); 5];
    let files = [
        CXQuery::naked("./tests/data/uint_0.csv"),
        CXQuery::naked("./tests/data/uint_1.csv"),
    ];
    let source = CSVSource::new(Some(&schema));

    let mut destination = ArrowDestination::new();
    let dispatcher = Dispatcher::<_, _, CSVArrowTransport>::new(source, &mut destination, &files);

    dispatcher.run().expect("run dispatcher");

    let result = destination.finish().unwrap();

    println!("result len: {}", result.len());
    assert!(result.len() == 2);

    for rb in result {
        for i in 0..5 {
            let col = rb.column(i).as_any().downcast_ref::<Int64Array>().unwrap();
            assert!(
                col.eq(&Int64Array::from_iter_values(
                    (4i64..=10).map(|v| v * 5 + i as i64),
                )) || col.eq(&Int64Array::from_iter_values(
                    (0i64..4).map(|v| v * 5 + i as i64),
                ))
            );
        }
    }
}

#[test]
fn test_csv_infer_schema() {
    let files = [CXQuery::naked("./tests/data/infer_0.csv")];
    let source = CSVSource::new(None);

    let mut writer = ArrowDestination::new();
    let dispatcher = Dispatcher::<_, _, CSVArrowTransport>::new(source, &mut writer, &files);

    dispatcher.run().expect("run dispatcher");

    let expected_schema = Schema {
        names: vec![
            "c0".into(),
            "c1".into(),
            "c2".into(),
            "c3".into(),
            "c4".into(),
            "c5".into(),
            "c6".into(),
            "c7".into(),
        ],
        types: vec![
            ArrowTypeSystem::Int64(false),
            ArrowTypeSystem::Float64(false),
            ArrowTypeSystem::Boolean(true),
            ArrowTypeSystem::LargeUtf8(true),
            ArrowTypeSystem::Float64(false),
            ArrowTypeSystem::LargeUtf8(true),
            ArrowTypeSystem::LargeUtf8(false),
            ArrowTypeSystem::LargeUtf8(true),
        ],
    };

    assert_eq!(&expected_schema, writer.schema());
}
