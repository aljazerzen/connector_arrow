use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use itertools::Itertools;
use std::sync::Arc;

use connector_arrow::api::{Append, ArrowValue, Connector, ResultReader, SchemaEdit, Statement};
use connector_arrow::util::transport::transport;
use connector_arrow::util::ArrowRowWriter;
use connector_arrow::{ConnectorError, TableCreateError, TableDropError};

pub fn coerce_type<C: Connector>(ty: &DataType) -> Option<DataType> {
    let db_ty = C::type_arrow_into_db(ty).unwrap();
    let roundtrip_ty =
        C::type_db_into_arrow(&db_ty).unwrap_or_else(|| panic!("cannot query type {}", db_ty));
    if *ty != roundtrip_ty {
        Some(roundtrip_ty)
    } else {
        None
    }
}

pub fn load_into_table<C>(
    conn: &mut C,
    schema: SchemaRef,
    batches: &[RecordBatch],
    table_name: &str,
) -> Result<(), ConnectorError>
where
    C: Connector + SchemaEdit,
{
    // table drop
    match conn.table_drop(table_name) {
        Ok(_) | Err(TableDropError::TableNonexistent) => (),
        Err(TableDropError::Connector(e)) => return Err(e),
    }

    // table create
    match conn.table_create(table_name, schema.clone()) {
        Ok(_) => (),
        Err(TableCreateError::TableExists) => {
            panic!("table was just deleted, how can it exist now?")
        }
        Err(TableCreateError::Connector(e)) => return Err(e),
    }

    // write into table
    {
        let mut appender = conn.append(&table_name).unwrap();
        for batch in batches {
            appender.append(batch.clone()).unwrap();
        }
        appender.finish().unwrap();
    }

    Ok(())
}

pub fn query_table<C: Connector>(
    conn: &mut C,
    table_name: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), ConnectorError> {
    let mut stmt = conn
        .query(&format!("SELECT * FROM \"{table_name}\""))
        .unwrap();
    let mut reader = stmt.start([])?;

    let schema = reader.get_schema()?;

    let batches = reader.collect::<Result<Vec<_>, ConnectorError>>()?;
    Ok((schema, batches))
}

pub fn query_literals<C: Connector>(conn: &mut C, queries: Vec<QueryOfSingleLiteral>) {
    let mut sql_selects = Vec::new();
    let mut expected_fields = Vec::new();
    let mut expected_arrays = Vec::new();

    for (index, query) in queries.into_iter().enumerate() {
        let field_name = format!("f_{}", index);

        let dt = C::type_db_into_arrow(&query.db_ty).unwrap_or_else(|| {
            panic!(
                "test failed: database type {} cannot be converted to arrow",
                query.db_ty
            )
        });
        expected_arrays.push(new_singleton_array(&dt, query.value));
        let field = Field::new(&field_name, dt, true);
        expected_fields.push(field);

        sql_selects.push(format!(
            "CAST({} AS {}) as {field_name}",
            query.value_sql, query.db_ty
        ));
    }

    let schema = Arc::new(Schema::new(expected_fields));
    let expected = RecordBatch::try_new(schema, expected_arrays).unwrap();

    let sql = format!("SELECT {};", sql_selects.join(", "));
    let batches = connector_arrow::query(conn, &sql).unwrap_or_else(|e| {
        panic!(
            "error while executing the following query:\n{}\nerror: {}",
            sql, e
        )
    });
    let batch = batches.into_iter().exactly_one().unwrap();

    similar_asserts::assert_eq!(
        arrow::util::pretty::pretty_format_batches(&vec![expected])
            .unwrap()
            .to_string(),
        arrow::util::pretty::pretty_format_batches(&vec![batch])
            .unwrap()
            .to_string(),
    );
}

pub fn query_literals_binary<C: Connector>(conn: &mut C, queries: Vec<QueryOfSingleLiteral>) {
    let mut sql_selects = Vec::new();
    let mut expected_fields = Vec::new();
    let mut expected_arrays = Vec::new();

    for (index, query) in queries.into_iter().enumerate() {
        let field_name = format!("f_{}", index);

        if let Some(dt) = C::type_db_into_arrow(&query.db_ty) {
            panic!(
                "test failed: database type {} is expected to fallback to binary, but got {:?}",
                query.db_ty, dt
            )
        }
        let dt = DataType::Binary;

        expected_arrays.push(new_singleton_array(&dt, query.value));
        let field = Field::new(&field_name, dt, true);
        expected_fields.push(field);

        sql_selects.push(format!(
            "CAST({} AS {}) as {field_name}",
            query.value_sql, query.db_ty
        ));
    }

    let schema = Arc::new(Schema::new(expected_fields));
    let expected = RecordBatch::try_new(schema, expected_arrays).unwrap();

    let sql = format!("SELECT {};", sql_selects.join(", "));
    let batches = connector_arrow::query(conn, &sql).unwrap_or_else(|e| {
        panic!(
            "error while executing the following query:\n{}\nerror: {}",
            sql, e
        )
    });
    let batch = batches.into_iter().exactly_one().unwrap();

    similar_asserts::assert_eq!(
        arrow::util::pretty::pretty_format_batches(&vec![expected])
            .unwrap()
            .to_string(),
        arrow::util::pretty::pretty_format_batches(&vec![batch])
            .unwrap()
            .to_string(),
    );
}

fn new_singleton_array(data_type: &DataType, value: Box<dyn ArrowValue>) -> ArrayRef {
    let schema = Arc::new(Schema::new(vec![Field::new("", data_type.clone(), true)]));

    let mut writer = ArrowRowWriter::new(schema.clone(), 1);
    writer.prepare_for_batch(1).unwrap();

    transport(schema.field(0), value.as_ref(), &mut writer).unwrap();

    let batch = writer.finish().unwrap().into_iter().exactly_one().unwrap();
    batch.column(0).clone()
}

pub struct QueryOfSingleLiteral {
    db_ty: String,
    value_sql: String,
    value: Box<dyn ArrowValue>,
}

impl<S1: ToString, S2: ToString, V: ArrowValue> From<(S1, S2, V)> for QueryOfSingleLiteral {
    fn from(value: (S1, S2, V)) -> Self {
        QueryOfSingleLiteral {
            db_ty: value.0.to_string(),
            value_sql: value.1.to_string(),
            value: Box::new(value.2),
        }
    }
}
