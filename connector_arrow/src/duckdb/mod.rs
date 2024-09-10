//! Provides `connector_arrow` traits for [duckdb crate](https://docs.rs/duckdb).

mod append;
mod schema;

#[doc(hidden)]
pub use append::DuckDBAppender;

use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use itertools::Itertools;

use std::sync::Arc;

use crate::api::{Connector, ResultReader, Statement};
use crate::errors::ConnectorError;
use crate::util::{transport, ArrayCellRef};

pub struct DuckDBConnection {
    inner: duckdb::Connection,
}

impl DuckDBConnection {
    pub fn new(inner: duckdb::Connection) -> Self {
        Self { inner }
    }

    pub fn unwrap(self) -> duckdb::Connection {
        self.inner
    }

    pub fn inner_mut(&mut self) -> &mut duckdb::Connection {
        &mut self.inner
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

    fn type_db_into_arrow(database_ty: &str) -> Option<DataType> {
        Some(match database_ty {
            "BOOLEAN" => DataType::Boolean,
            "TINYINT" => DataType::Int8,
            "SMALLINT" => DataType::Int16,
            "INTEGER" => DataType::Int32,
            "BIGINT" => DataType::Int64,
            "UTINYINT" => DataType::UInt8,
            "USMALLINT" => DataType::UInt16,
            "UINTEGER" => DataType::UInt32,
            "UBIGINT" => DataType::UInt64,
            "REAL" => DataType::Float32,
            "DOUBLE" => DataType::Float64,

            "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, None),

            "DATE" => DataType::Date64,

            "BLOB" => DataType::Binary,
            "VARCHAR" => DataType::Utf8,
            _ => return None,
        })
    }

    fn type_arrow_into_db(ty: &DataType) -> Option<String> {
        let s = match ty {
            DataType::Null => "BIGINT",

            DataType::Boolean => "BOOLEAN",
            DataType::Int8 => "TINYINT",
            DataType::Int16 => "SMALLINT",
            DataType::Int32 => "INTEGER",
            DataType::Int64 => "BIGINT",
            DataType::UInt8 => "UTINYINT",
            DataType::UInt16 => "USMALLINT",
            DataType::UInt32 => "UINTEGER",
            DataType::UInt64 => "UBIGINT",
            DataType::Float16 => "REAL",
            DataType::Float32 => "REAL",
            DataType::Float64 => "DOUBLE",

            DataType::Timestamp(TimeUnit::Microsecond, _) => "TIMESTAMP",
            // timestamps are all converted to microseconds, so all other units
            // overflow or lose precision, which counts as information loss.
            // this means that we have to store them as int64.
            DataType::Timestamp(_, _) => "BIGINT",

            DataType::Date32 => "DATE",
            DataType::Date64 => "DATE",
            DataType::Time32(_) => "TIME",
            DataType::Time64(_) => "TIME",
            DataType::Duration(_) => return None,
            DataType::Interval(_) => "INTERVAL",

            DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => "BLOB",
            DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",

            DataType::Decimal128(_, _) => todo!(),
            DataType::Decimal256(_, _) => todo!(),

            _ => return None,
        };
        Some(s.to_string())
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

    fn start_batch<'p>(
        &mut self,
        args: (&RecordBatch, usize),
    ) -> Result<Self::Reader<'_>, ConnectorError> {
        // args
        let arg_cells = ArrayCellRef::vec_from_batch(args.0, args.1);
        let mut args: Vec<duckdb::types::Value> = Vec::with_capacity(arg_cells.len());
        for cell in arg_cells {
            transport::transport(cell.field, &cell, &mut args)?;
        }
        let args = args.iter().map(|x| x as &dyn duckdb::ToSql).collect_vec();

        // query
        let arrow = self.stmt.query_arrow(args.as_slice())?;
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
