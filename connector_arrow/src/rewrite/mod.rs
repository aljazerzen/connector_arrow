mod consumer_row;
mod data_store;
pub mod duckdb;
mod errors;
pub mod sqlite;
mod transport;

use arrow::record_batch::RecordBatch;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use self::data_store::{
    DataStore, DataStoreConnection, DataStoreTask, ResultReader, RowReader, RowsReader,
};
use self::errors::ConnectorError;

pub fn query_many<S: DataStore>(
    store: &S,
    queries: &[&str],
) -> Result<Vec<Vec<RecordBatch>>, ConnectorError>
where
    S: DataStore,
{
    queries
        .into_par_iter()
        .map(|query| query_one(store, query))
        .collect()
}

pub fn query_one<S: DataStore>(store: &S, query: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
    log::debug!("query: {query}");

    // open a new connection
    let mut connection = store.new_connection()?;

    // create a new task
    let mut task = connection.prepare_task(query)?;

    // start reading
    let mut reader = task.start(())?;
    let Some(schema) = reader.read_until_schema()? else {
        panic!("cannot read schema, probably empty result");
    };

    log::debug!("got schema: {schema:?}");

    // try to read as arrow record batches directly
    let reader = match reader.try_into_batch() {
        Ok(batch_reader) => {
            let res = batch_reader.into_iter().collect::<Vec<_>>();

            return Ok(res);
        }
        Err(r) => r,
    };

    // fallback to reading row-by-row
    let _reader = match reader.try_into_rows() {
        Ok(mut rows_reader) => {
            let mut consumer = consumer_row::ArrowRowWriter::new(schema.clone(), 1024)?;

            log::debug!("reading rows");
            while let Some(mut row_reader) = rows_reader.next_row()? {
                consumer.prepare_for_batch(1)?;

                log::debug!("reading row");
                for field in &schema.fields {
                    log::debug!("reading cell");
                    let cell_ref = row_reader.next_cell();
                    log::debug!("transporting cell");

                    transport::transport(field, &cell_ref, &mut consumer);
                }
            }

            return consumer.finish();
        }
        Err(r) => r,
    };

    // cannot read as arrow or as rows, the reader is not implemented correctly
    panic!("none of the readers are implemented for")
}
