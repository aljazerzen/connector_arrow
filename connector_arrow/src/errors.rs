use std::string::FromUtf8Error;

use thiserror::Error;

/// Errors that can be raised from this library.
#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("Schema of the result cannot be inferred or converted to Arrow schema")]
    CannotConvertSchema,

    #[error("Result data does not match the schema")]
    DataSchemaMismatch(String),

    #[error("Schema is not compatible with Arrow data types")]
    IncompatibleSchema {
        table_name: String,
        message: String,
        hint: Option<String>,
    },

    #[error("When converting values from database representation into into an Arrow types, it fell out of supported range.")]
    DataOutOfRange,

    #[error(transparent)]
    UrlEncoding(#[from] FromUtf8Error),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[cfg(feature = "src_sqlite")]
    #[error(transparent)]
    SQLite(#[from] rusqlite::Error),

    #[cfg(feature = "src_duckdb")]
    #[error(transparent)]
    DuckDB(#[from] duckdb::Error),

    #[cfg(feature = "src_postgres")]
    #[error(transparent)]
    Postgres(#[from] super::postgres::PostgresError),
}

#[derive(Error, Debug)]
pub enum TableCreateError {
    #[error("Table already exists")]
    TableExists,

    #[error(transparent)]
    Connector(#[from] ConnectorError),
}

#[derive(Error, Debug)]
pub enum TableDropError {
    #[error("Table does not exist")]
    TableNonexistent,

    #[error(transparent)]
    Connector(#[from] ConnectorError),
}
