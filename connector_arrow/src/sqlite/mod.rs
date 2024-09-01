//! Provides `connector_arrow` traits for [rusqlite crate](https://docs.rs/rusqlite).

mod append;
mod query;
mod schema;
mod types;

#[doc(hidden)]
pub use append::SQLiteAppender;
#[doc(hidden)]
pub use query::SQLiteStatement;

use crate::api::Connector;
use crate::errors::ConnectorError;
use arrow::datatypes::DataType;

pub struct SQLiteConnection {
    inner: rusqlite::Connection,
}

impl SQLiteConnection {
    pub fn new(inner: rusqlite::Connection) -> Self {
        Self { inner }
    }

    pub fn unwrap(self) -> rusqlite::Connection {
        self.inner
    }

    pub fn inner_mut(&mut self) -> &mut rusqlite::Connection {
        &mut self.inner
    }
}

impl Connector for SQLiteConnection {
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

    fn type_db_into_arrow(database_ty: &str) -> Option<DataType> {
        match database_ty {
            "NULL" => Some(DataType::Null),
            "INTEGER" => Some(DataType::Int64),
            "REAL" => Some(DataType::Float64),
            "TEXT" => Some(DataType::Utf8),
            "BLOB" => Some(DataType::Binary),
            _ => None,
        }
    }

    fn type_arrow_into_db(ty: &DataType) -> Option<String> {
        let s = match ty {
            DataType::Null => "NULL",
            DataType::Boolean => "INTEGER",

            DataType::Int8 => "INTEGER",
            DataType::Int16 => "INTEGER",
            DataType::Int32 => "INTEGER",
            DataType::Int64 => "INTEGER",

            DataType::UInt8 => "INTEGER",
            DataType::UInt16 => "INTEGER",
            DataType::UInt32 => "INTEGER",
            DataType::UInt64 => "TEXT",

            DataType::Float16 => "REAL",
            DataType::Float32 => "REAL",
            DataType::Float64 => "REAL",

            // temporal types are stored as plain integers
            // - for Timestamp(Second, Some("00:00")), this is convenient,
            //   since it can be used as SQLite's 'unixepoch'
            // - for all others, this is very inconvenient,
            //   but better than losing information in a roundtrip.
            DataType::Timestamp(_, _) => "INTEGER",
            DataType::Date32 => "INTEGER",
            DataType::Date64 => "INTEGER",
            DataType::Time32(_) => "INTEGER",
            DataType::Time64(_) => "INTEGER",
            DataType::Duration(_) => "INTEGER",
            DataType::Interval(_) => return None,

            DataType::Binary => "BLOB",
            DataType::FixedSizeBinary(_) => "BLOB",
            DataType::LargeBinary => "BLOB",

            DataType::Utf8 => "TEXT",
            DataType::LargeUtf8 => "TEXT",

            DataType::Decimal128(_, _) => "TEXT",
            DataType::Decimal256(_, _) => "TEXT",
            _ => return None,
        };
        Some(s.to_string())
    }
}
