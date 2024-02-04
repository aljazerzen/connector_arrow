mod api;
pub mod duckdb;
mod errors;
pub mod sqlite;
pub mod util;

use arrow::record_batch::RecordBatch;

use self::api::{Connection, DataStore, Statement};
use self::errors::ConnectorError;

/// Open a connection, execute a single query and return the connection back into the pool.
pub fn query_one<S: DataStore>(store: &S, query: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
    log::debug!("query: {query}");

    // open a new connection
    let mut connection = store.new_connection()?;

    // create a new task
    let mut task = connection.prepare_task(query)?;

    // start reading
    let reader = task.start(())?;

    // try to read as arrow record batches directly
    let batches = reader.collect::<Result<_, _>>()?;
    Ok(batches)
}
