//! Provides `connector_arrow` traits for [rusqlite crate](https://docs.rs/rusqlite).

mod append;
mod query;
mod schema;
mod types;

#[doc(hidden)]
pub use append::SQLiteAppender;
#[doc(hidden)]
pub use query::SQLiteStatement;

use crate::api::Connection;
use crate::errors::ConnectorError;
use arrow::datatypes::DataType;

pub struct SQLiteConnection {
    inner: rusqlite::Connection,
}

impl SQLiteConnection {
    pub fn new(inner: rusqlite::Connection) -> Self {
        Self { inner }
    }
}

impl Connection for SQLiteConnection {
    type Stmt<'conn> = SQLiteStatement<'conn> where Self: 'conn;

    type Append<'conn> = SQLiteAppender<'conn> where Self: 'conn;

    fn query(&mut self, query: &str) -> Result<SQLiteStatement, ConnectorError> {
        let stmt = self.inner.prepare(query)?;
        Ok(SQLiteStatement { stmt })
    }

    fn append<'a>(&'a mut self, table: &str) -> Result<Self::Append<'a>, ConnectorError> {
        let transaction = self.inner.transaction()?;

        SQLiteAppender::new(table.to_string(), transaction)
    }

    fn coerce_type(ty: &DataType) -> Option<DataType> {
        match ty {
            DataType::Boolean => Some(DataType::Int64),
            DataType::Int8 => Some(DataType::Int64),
            DataType::Int16 => Some(DataType::Int64),
            DataType::Int32 => Some(DataType::Int64),
            DataType::Int64 => Some(DataType::Int64),
            DataType::UInt8 => Some(DataType::Int64),
            DataType::UInt16 => Some(DataType::Int64),
            DataType::UInt32 => Some(DataType::Int64),
            DataType::UInt64 => Some(DataType::LargeUtf8),
            DataType::Float16 => Some(DataType::Float64),
            DataType::Float32 => Some(DataType::Float64),
            DataType::Float64 => Some(DataType::Float64),
            DataType::Binary => Some(DataType::LargeBinary),
            DataType::FixedSizeBinary(_) => Some(DataType::LargeBinary),
            DataType::LargeBinary => Some(DataType::LargeBinary),
            DataType::Utf8 => Some(DataType::LargeUtf8),
            DataType::LargeUtf8 => Some(DataType::LargeUtf8),
            _ => None,
        }
    }
}
