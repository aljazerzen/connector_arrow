use connector_arrow::prelude::*;
use connector_arrow::sources::oracle::OracleSource;
use connector_arrow::sql::CXQuery;
use std::env;

#[test]
#[ignore]
fn test_types() {
    let _ = env_logger::builder().is_test(true).try_init();
    let dburl = env::var("ORACLE_URL").unwrap();
    let mut source = OracleSource::new(&dburl, 1).unwrap();
    #[derive(Debug, PartialEq)]
    struct Row(i64, i64, f64, f64, String, String, String, String);

    let query = CXQuery::naked("select * from admin.test_table");
    let mut partition = source.reader(&query, DataOrder::RowMajor).unwrap();

    let schema = partition.fetch_schema().unwrap();

    let mut parser = partition.parser(&schema).unwrap();

    let mut rows: Vec<Row> = Vec::new();
    loop {
        let (n, is_last) = parser.fetch_next().unwrap();
        for _i in 0..n {
            rows.push(Row(
                parser.produce().unwrap(),
                parser.produce().unwrap(),
                parser.produce().unwrap(),
                parser.produce().unwrap(),
                parser.produce().unwrap(),
                parser.produce().unwrap(),
                parser.produce().unwrap(),
                parser.produce().unwrap(),
            ));
        }
        if is_last {
            break;
        }
    }

    assert_eq!(
        vec![
            Row(
                1,
                1,
                1.1,
                1.1,
                "varchar1".to_string(),
                "char1".to_string(),
                "nvarchar1".to_string(),
                "nchar1".to_string()
            ),
            Row(
                2,
                2,
                2.2,
                2.2,
                "varchar2".to_string(),
                "char2".to_string(),
                "nvarchar2".to_string(),
                "nchar2".to_string()
            ),
            Row(
                3,
                3,
                3.3,
                3.3,
                "varchar3".to_string(),
                "char3".to_string(),
                "nvarchar3".to_string(),
                "nchar3".to_string()
            )
        ],
        rows
    );
}
