//! An database client for many databases, exposing an interface that produces Apache Arrow.
//!
//! The API provided by each data source is described in [api] module.
//!
//! Example for SQLite:
//! ```
//! use connector_arrow::{Connection, Statement, ConnectorError, arrow};
//! use connector_arrow::arrow::record_batch::RecordBatch;
//!
//! let mut conn = rusqlite::Connection::open_in_memory();
//!
//! let mut stmt = Connection::prepare(&mut conn, "SELECT 1 as a");
//!
//! let reader = stmt.start(());
//!
//! // reader implements Iterator<Item = Result<RecordBatch, _>>
//! let batches: Vec<RecordBatch> = reader.collect::<Result<_, ConnectorError>>().unwrap();
//! ```
//!
//! For a list of supported databases, refer to [creates page](https://creates.io/crate/connector_arrow).

pub mod api;
mod errors;
pub mod util;

#[cfg(feature = "src_duckdb")]
pub mod duckdb;
#[cfg(feature = "src_postgres")]
pub mod postgres;
#[cfg(feature = "src_sqlite")]
pub mod sqlite;

pub use arrow;
pub use errors::*;

use arrow::record_batch::RecordBatch;

use self::api::{Connection, Statement};

/// Open a connection, execute a single query and return the connection back into the pool.
pub fn query_one<C: Connection>(
    conn: &mut C,
    query: &str,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    log::debug!("query: {query}");

    // prepare statement
    let mut stmt = conn.query(query)?;

    // start reading
    let reader = stmt.start(())?;

    // collect results
    let batches = reader.collect::<Result<_, _>>()?;
    Ok(batches)
}
