mod consumer_row;
mod data_store;
pub mod sqlite;
mod transport;

use std::sync::Arc;

use arrow::{error::ArrowError, record_batch::RecordBatch};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use self::data_store::{
    DataStore, DataStoreConnection, DataStoreTask, ResultReader, RowReader, RowsReader,
};

pub fn query_many<S: DataStore>(
    store: &S,
    queries: &[&str],
) -> Result<Vec<Vec<RecordBatch>>, S::Error>
where
    S: DataStore,
{
    queries
        .into_par_iter()
        .map(|query| query_one(store, query))
        .collect()
}

pub fn query_one<S: DataStore>(store: &S, query: &str) -> Result<Vec<RecordBatch>, S::Error> {
    log::debug!("query: {query}");

    let mut connection = store.new_connection()?;

    let mut task = connection.prepare_task(query)?;

    let mut reader = task.start(())?;

    let Some(schema) = reader.read_until_schema()? else {
        panic!("cannot read schema, probably empty result");
    };

    log::debug!("got schema: {schema:?}");

    let schema = Arc::new(schema);

    let reader = match reader.try_into_batch() {
        Ok(batch_reader) => {
            let res = batch_reader
                .into_iter()
                .collect::<Result<Vec<_>, ArrowError>>()
                .unwrap();

            return Ok(res);
        }
        Err(r) => r,
    };

    let _reader = match reader.try_into_rows() {
        Ok(mut rows_reader) => {
            let mut consumer = consumer_row::ArrowRowWriter::new(schema.clone(), 1024).unwrap();

            log::debug!("reading rows");
            while let Some(mut row_reader) = rows_reader.next_row()? {
                consumer.prepare_for_batch(1).unwrap();

                log::debug!("reading row");
                for field in &schema.fields {
                    log::debug!("reading cell");
                    let cell_ref = row_reader.next_cell();
                    log::debug!("transporting cell");

                    transport::transport(field, &cell_ref, &mut consumer);
                }
            }

            return Ok(consumer.finish().unwrap());
        }
        Err(r) => r,
    };

    panic!("none of the readers are implemented for")
}
