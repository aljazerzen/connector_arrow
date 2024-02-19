use std::{fs::File, path::Path};

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use connector_arrow::api::{Append, Connection, ResultReader, SchemaEdit, Statement};
use connector_arrow::util::coerce;
use connector_arrow::{ConnectorError, TableCreateError, TableDropError};

pub fn read_parquet(file_path: &Path) -> Result<(SchemaRef, Vec<RecordBatch>), ArrowError> {
    // read from file
    let file = File::open(file_path)?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let schema = builder.schema().clone();

    let reader = builder.build()?;
    let batches = reader.collect::<Result<Vec<_>, ArrowError>>()?;
    Ok((schema, batches))
}

pub fn load_parquet_into_table<C>(
    conn: &mut C,
    file_path: &Path,
    table_name: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), ConnectorError>
where
    C: Connection + SchemaEdit,
{
    let (schema_file, batches_file) = read_parquet(file_path)?;

    // table drop
    match conn.table_drop(table_name) {
        Ok(_) | Err(TableDropError::TableNonexistent) => (),
        Err(TableDropError::Connector(e)) => return Err(e),
    }

    // table create
    match conn.table_create(table_name, schema_file.clone()) {
        Ok(_) => (),
        Err(TableCreateError::TableExists) => {
            panic!("table was just deleted, how can it exist now?")
        }
        Err(TableCreateError::Connector(e)) => return Err(e),
    }

    // write into table
    {
        let mut appender = conn.append(&table_name).unwrap();
        for batch in batches_file.clone() {
            appender.append(batch).unwrap();
        }
        appender.finish().unwrap();
    }

    let schema_coerced = coerce::coerce_schema(schema_file, &C::coerce_type);
    let batches_coerced = coerce::coerce_batches(&batches_file, C::coerce_type).unwrap();
    Ok((schema_coerced, batches_coerced))
}

pub fn query_table<C: Connection>(
    conn: &mut C,
    table_name: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), ConnectorError> {
    let mut stmt = conn
        .query(&format!("SELECT * FROM \"{table_name}\""))
        .unwrap();
    let mut reader = stmt.start(())?;

    let schema = reader.get_schema()?;

    let batches = reader.collect::<Result<Vec<_>, ConnectorError>>()?;
    Ok((schema, batches))
}
