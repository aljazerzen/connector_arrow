use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use connector_arrow::api::{Append, Connector, ResultReader, SchemaEdit, Statement};
use connector_arrow::{ConnectorError, TableCreateError, TableDropError};

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
    let mut reader = stmt.start(())?;

    let schema = reader.get_schema()?;

    let batches = reader.collect::<Result<Vec<_>, ConnectorError>>()?;
    Ok((schema, batches))
}
