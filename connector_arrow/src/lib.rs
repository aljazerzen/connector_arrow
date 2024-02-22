//! A flexible database client that converts data into Apache Arrow format
//! across various databases.
//!
//! The API provided by each data source is described in [api] module.
//!
//! Capabilities:
//! - **Query**: Query databases and retrieve results in Apache Arrow format.
//! - **Query Parameters**: Utilize Arrow type system for query parameters.
//! - **Temporal and Container Types**: Correctly handles temporal and container types.
//! - **Schema Introspection**: Query the database for schema of specific tables.
//! - **Schema Migration**: Basic schema migration commands.
//! - **Append**: Write [arrow::record_batch::RecordBatch] into database tables.
//!
//! Example for SQLite:
//! ```
//! # use connector_arrow::api::{Connector, Statement, ResultReader};
//! # use connector_arrow::arrow::record_batch::RecordBatch;
//! # use connector_arrow::arrow::datatypes::SchemaRef;
//! # use connector_arrow::sqlite::SQLiteConnection;
//!
//! # fn main() -> Result<(), connector_arrow::ConnectorError> {
//! // a regular rusqlite connection
//! let conn = rusqlite::Connection::open_in_memory()?;
//!
//! // wrap into connector_arrow connection
//! let mut conn = SQLiteConnection::new(conn);
//!
//! let mut stmt = conn.query("SELECT 1 as a")?;
//!
//! let mut reader = stmt.start(&[])?;
//!
//! let schema: SchemaRef = reader.get_schema()?;
//!
//! // reader implements Iterator<Item = Result<RecordBatch, _>>
//! let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
//! # Ok(())
//! # }
//! ```
//!
//! For a list of supported databases, refer to the [crates.io page](https://crates.io/crates/connector_arrow).

pub mod api;
mod errors;
mod params;
pub mod types;
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

use self::api::{Connector, Statement};

/// Open a connection, execute a single query and return the connection back into the pool.
pub fn query_one<C: Connector>(
    conn: &mut C,
    query: &str,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    log::debug!("query: {query}");

    // prepare statement
    let mut stmt = conn.query(query)?;

    // start reading
    let reader = stmt.start(&[])?;

    // collect results
    let batches = reader.collect::<Result<_, _>>()?;
    Ok(batches)
}
