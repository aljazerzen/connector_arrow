pub mod api;
mod errors;
pub mod util;

#[cfg(feature = "src_duckdb")]
pub mod duckdb;
#[cfg(feature = "src_postgres")]
pub mod postgres;
#[cfg(feature = "src_sqlite")]
pub mod sqlite;

pub use errors::ConnectorError;

use arrow::record_batch::RecordBatch;

use self::api::{Connection, Statement};

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
