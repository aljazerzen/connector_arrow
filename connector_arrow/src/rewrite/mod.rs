mod api;
pub mod duckdb;
mod errors;
pub mod sqlite;
pub mod util;

use arrow::record_batch::RecordBatch;

use self::api::{Connection, Statement};
use self::errors::ConnectorError;

/// Open a connection, execute a single query and return the connection back into the pool.
pub fn query_one<C: Connection>(
    conn: &mut C,
    query: &str,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    log::debug!("query: {query}");

    // prepare statement
    let mut stmt = conn.prepare(query)?;

    // start reading
    let reader = stmt.start(())?;

    // collect results
    let batches = reader.collect::<Result<_, _>>()?;
    Ok(batches)
}
