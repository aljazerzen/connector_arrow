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
//! use connector_arrow::api::{Connector, Statement, ResultReader};
//! use connector_arrow::arrow;
//!
//! # fn main() -> Result<(), connector_arrow::ConnectorError> {
//! // a regular rusqlite connection
//! let conn = rusqlite::Connection::open_in_memory()?;
//!
//! // wrap into connector_arrow connection
//! let mut conn = connector_arrow::sqlite::SQLiteConnection::new(conn);
//!
//! let mut stmt = conn.query("SELECT 1 as a")?;
//!
//! let mut reader = stmt.start([])?;
//!
//! let schema: arrow::datatypes::SchemaRef = reader.get_schema()?;
//!
//! // reader implements Iterator<Item = Result<RecordBatch, _>>
//! let batches: Vec<arrow::record_batch::RecordBatch> = reader.collect::<Result<_, _>>()?;
//! # Ok(())
//! # }
//! ```
//!
//! For a list of supported databases, refer to the [crates.io page](https://crates.io/crates/connector_arrow).
//!
//! ## Transitive dependency on arrow
//!
//! If you depend on `connector_arrow`, it is recommended not to depend on `arrow`
//! directly, but use re-export from this crate instead. This advice is only relevant
//! if your crate does not need any additional `arrow` features.
//!
//! ```
//! use connector_arrow::arrow;
//! ```
//!
//! If you do depend on `arrow` directly, you have to make sure to use exactly the
//! same version as is used by `connector_arrow`, otherwise types from `arrow` and
//! `connector_arrow` will not be interchangeable and might lead to type errors.
//!
//! This situation is made much worse by unusually high cadence of major version
//! releases of arrow-rs, even without breaking changes.

pub mod api;
mod errors;
mod params;
pub mod types;
pub mod util;

#[cfg(feature = "src_duckdb")]
pub mod duckdb;
#[cfg(feature = "src_mysql")]
pub mod mysql;
#[cfg(feature = "src_postgres")]
pub mod postgres;
#[cfg(feature = "src_sqlite")]
pub mod sqlite;
#[cfg(feature = "src_tiberius")]
pub mod tiberius;

pub use arrow;
pub use errors::*;

use arrow::record_batch::RecordBatch;

use self::api::{Connector, Statement};

/// Open a connection, execute a single query and return the results.
pub fn query<C: Connector>(conn: &mut C, query: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
    log::debug!("query: {query}");

    // prepare statement
    let mut stmt = conn.query(query)?;

    // start reading
    let reader = stmt.start([])?;

    // collect results
    let batches = reader.collect::<Result<_, _>>()?;
    Ok(batches)
}
