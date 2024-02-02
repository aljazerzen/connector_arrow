mod data_store;
pub mod sqlite;
mod transport;

use rayon::iter::{IntoParallelIterator, ParallelIterator};

use self::data_store::{
    DataStore, DataStoreConnection, DataStoreTask, ResultReader, RowReader, RowsReader,
};

pub fn query_many<S: DataStore>(store: &S, queries: &[&str]) -> Result<(), S::Error>
where
    S: DataStore,
{
    queries
        .into_par_iter()
        .try_for_each(|query| query_one(store, query))?;

    Ok(())
}

pub fn query_one<S: DataStore>(store: &S, query: &str) -> Result<(), S::Error> {
    log::debug!("query: {query}");

    let mut connection = store.new_connection()?;

    let mut task = connection.prepare_task(query)?;

    let mut reader = task.start(())?;

    let Some(schema) = reader.read_until_schema()? else {
        panic!("cannot read schema, probably empty result");
    };

    log::debug!("got schema: {schema:?}");

    let mut consumer = transport::print::PrintConsumer();

    match reader.try_into_batch() {
        Ok(_batch_reader) => todo!(),
        Err(reader) => {
            if let Ok(mut rows_reader) = reader.try_into_rows() {
                log::debug!("reading rows");
                while let Some(mut row_reader) = rows_reader.next_row()? {
                    log::debug!("reading row");
                    for field in &schema.fields {
                        log::debug!("reading cell");
                        let cell_ref = row_reader.next_cell();
                        log::debug!("transporting cell");

                        transport::transport(field, &cell_ref, &mut consumer);
                    }
                }
            } else {
                panic!("none of the readers are implemented")
            }
        }
    }

    Ok(())
}
