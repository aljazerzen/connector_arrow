use std::string::FromUtf8Error;

use thiserror::Error;

/// Errors that can be raised from this library.
#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    SQLite(#[from] rusqlite::Error),

    #[error(transparent)]
    DuckDB(#[from] duckdb::Error),

    #[error(transparent)]
    Pool(#[from] r2d2::Error),

    #[error(transparent)]
    UrlEncoding(#[from] FromUtf8Error),
}
