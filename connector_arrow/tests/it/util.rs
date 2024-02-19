use std::{fs::File, path::Path, sync::Arc};

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use connector_arrow::api::{Append, Connection, ResultReader, SchemaEdit, Statement};
use connector_arrow::ConnectorError;

#[track_caller]
pub fn load_parquet_if_not_exists<C>(
    conn: &mut C,
    file_path: &Path,
    table_name: &str,
) -> (SchemaRef, Vec<RecordBatch>)
where
    C: Connection + SchemaEdit,
{
    // read from file
    let (schema, arrow_file) = {
        let file = File::open(file_path).unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        let schema = builder.schema().clone();

        let reader = builder.build().unwrap();
        let batches = reader.collect::<Result<Vec<_>, ArrowError>>().unwrap();
        (schema, batches)
    };

    // table create
    match conn.table_create(table_name, schema.clone()) {
        Ok(_) => (),
        Err(connector_arrow::TableCreateError::TableExists) => return (schema, arrow_file),
        Err(e) => panic!("{}", e),
    }

    // write into table
    {
        let mut appender = conn.append(&table_name).unwrap();
        for batch in arrow_file.clone() {
            appender.append(batch).unwrap();
        }
        appender.finish().unwrap();
    }

    (schema, arrow_file)
}

// #[track_caller]
pub fn roundtrip_of_parquet<C, F>(conn: &mut C, file_path: &Path, table_name: &str, coerce_ty: F)
where
    C: Connection + SchemaEdit,
    F: Fn(&DataType) -> Option<DataType>,
{
    let (schema_file, arrow_file) = load_parquet_if_not_exists(conn, file_path, table_name);

    // read from table
    let (schema_query, arrow_query) = {
        let mut stmt = conn
            .query(&format!("SELECT * FROM \"{table_name}\""))
            .unwrap();
        let mut reader = stmt.start(()).unwrap();

        let schema = reader.get_schema().unwrap();

        let batches = reader.collect::<Result<Vec<_>, ConnectorError>>().unwrap();
        (schema, batches)
    };

    // table drop
    conn.table_drop(&table_name).unwrap();

    let schema_file_coerced = cast_schema(&schema_file, &coerce_ty);
    similar_asserts::assert_eq!(&schema_file_coerced, &schema_query);

    let arrow_file_coerced = cast_batches(&arrow_file, coerce_ty);
    similar_asserts::assert_eq!(&arrow_file_coerced, &arrow_query);
}

pub fn cast_batches<F>(batches: &[RecordBatch], coerce_ty: F) -> Vec<RecordBatch>
where
    F: Fn(&DataType) -> Option<DataType>,
{
    let arrow_file = batches
        .iter()
        .map(|batch| {
            let new_schema = cast_schema(&batch.schema(), &coerce_ty);

            let new_columns = batch
                .columns()
                .iter()
                .map(|col_array| match coerce_ty(col_array.data_type()) {
                    Some(new_ty) => arrow::compute::cast(&col_array, &new_ty).unwrap(),
                    None => col_array.clone(),
                })
                .collect_vec();

            RecordBatch::try_new(new_schema, new_columns).unwrap()
        })
        .collect_vec();
    arrow_file
}

pub fn cast_schema<F>(schema: &Schema, coerce_ty: &F) -> SchemaRef
where
    F: Fn(&DataType) -> Option<DataType>,
{
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| match coerce_ty(f.data_type()) {
                Some(new_ty) => Field::new(f.name(), new_ty, f.is_nullable()),
                None => Field::clone(f),
            })
            .collect_vec(),
    ))
}
