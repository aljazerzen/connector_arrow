//! This module contains transport definitions for the sources and destinations implemented in ConnectorX.

#[cfg(feature = "src_bigquery")]
mod bigquery_arrow;
#[cfg(feature = "src_csv")]
mod csv_arrow;
#[cfg(feature = "src_dummy")]
mod dummy_arrow;
#[cfg(feature = "src_mssql")]
mod mssql_arrow;
#[cfg(feature = "src_mysql")]
mod mysql_arrow;
#[cfg(feature = "src_oracle")]
mod oracle_arrow;
#[cfg(feature = "src_postgres")]
mod postgres_arrow;
#[cfg(feature = "src_sqlite")]
mod sqlite_arrow;

#[cfg(feature = "src_bigquery")]
pub use bigquery_arrow::{BigQueryArrowTransport, BigQueryArrowTransportError};
#[cfg(feature = "src_csv")]
pub use csv_arrow::CSVArrowTransport;
#[cfg(feature = "src_dummy")]
pub use dummy_arrow::DummyArrowTransport;
#[cfg(feature = "src_mssql")]
pub use mssql_arrow::{MsSQLArrowTransport, MsSQLArrowTransportError};
#[cfg(feature = "src_mysql")]
pub use mysql_arrow::{MySQLArrowTransport, MySQLArrowTransportError};
#[cfg(feature = "src_oracle")]
pub use oracle_arrow::{OracleArrowTransport, OracleArrowTransportError};
#[cfg(feature = "src_postgres")]
pub use postgres_arrow::{PostgresArrowTransport, PostgresArrowTransportError};
#[cfg(feature = "src_sqlite")]
pub use sqlite_arrow::{SQLiteArrowTransport, SQLiteArrowTransportError};
