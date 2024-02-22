//! Provides `connector_arrow` traits for [duckdb crate](https://docs.rs/duckdb).

mod append;
mod schema;

#[doc(hidden)]
pub use append::DuckDBAppender;

use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

use std::sync::Arc;

use crate::api::{ArrowValue, Connector, ResultReader, Statement};
use crate::errors::ConnectorError;

pub struct DuckDBConnection {
    inner: duckdb::Connection,
}

impl DuckDBConnection {
    pub fn new(inner: duckdb::Connection) -> Self {
        Self { inner }
    }
}

impl Connector for DuckDBConnection {
    type Stmt<'conn> = DuckDBStatement<'conn>
    where
        Self: 'conn;

    type Append<'conn> = DuckDBAppender<'conn> where Self: 'conn;

    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        let stmt = self.inner.prepare(query)?;

        Ok(DuckDBStatement { stmt })
    }
    fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError> {
        Ok(DuckDBAppender {
            inner: self.inner.appender(table_name)?,
        })
    }

    fn coerce_type(ty: &DataType) -> Option<DataType> {
        match ty {
            DataType::Null => Some(DataType::Int64),
            DataType::Float16 => Some(DataType::Float32),

            // timezone cannot be retained in the schema
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Some(DataType::Timestamp(TimeUnit::Microsecond, None))
            }

            // timestamps are all converted to microseconds, so all other units
            // overflow or lose precision, which counts as information loss.
            // this means that we have to store them as just int64.
            DataType::Timestamp(_, _) => Some(DataType::Int64),

            DataType::LargeUtf8 => Some(DataType::Utf8),
            DataType::LargeBinary => Some(DataType::Binary),
            DataType::FixedSizeBinary(_) => Some(DataType::Binary),

            _ => None,
        }
    }
}

#[doc(hidden)]
pub struct DuckDBStatement<'conn> {
    stmt: duckdb::Statement<'conn>,
}

impl<'conn> Statement<'conn> for DuckDBStatement<'conn> {
    type Reader<'stmt> = DuckDBReader<'stmt>
    where
        Self: 'stmt;

    fn start(&mut self, _params: &[&dyn ArrowValue]) -> Result<Self::Reader<'_>, ConnectorError> {
        let arrow = self.stmt.query_arrow([])?;
        Ok(DuckDBReader { arrow })
    }
}

#[doc(hidden)]
pub struct DuckDBReader<'stmt> {
    arrow: duckdb::Arrow<'stmt>,
}

impl<'stmt> ResultReader<'stmt> for DuckDBReader<'stmt> {
    fn get_schema(&mut self) -> Result<Arc<arrow::datatypes::Schema>, ConnectorError> {
        Ok(self.arrow.get_schema())
    }
}

impl<'stmt> Iterator for DuckDBReader<'stmt> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.arrow.next().map(Ok)
    }
}
