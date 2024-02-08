use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};

use crate::errors::{TableCreateError, TableDropError};

use super::errors::ConnectorError;

/// A connection to a data store.
pub trait Connection {
    type Stmt<'conn>: Statement<'conn>
    where
        Self: 'conn;

    type Append<'conn>: Append<'conn>
    where
        Self: 'conn;

    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError>;

    fn get_table_schemas(&mut self) -> Result<Vec<TableSchema>, ConnectorError>;

    fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError>;
}

/// A task that is to be executed in the data store, over a connection.
pub trait Statement<'conn> {
    type Params: Send + Sync + Clone;

    type Reader<'stmt>: ResultReader<'stmt>
    where
        Self: 'stmt;

    /// Start executing.
    /// This will create a reader that can retrieve schema and then the data.
    fn start(&mut self, params: ()) -> Result<Self::Reader<'_>, ConnectorError>;
}

/// Reads result of the query, starting with the schema.
pub trait ResultReader<'stmt>: Iterator<Item = Result<RecordBatch, ConnectorError>> {
    fn get_schema(&mut self) -> Result<Arc<Schema>, ConnectorError>;
}

/// A definition of a relation stored in a data store. Also known as a "table".
#[derive(Debug)]
pub struct TableSchema {
    pub name: String,
    pub schema: Schema,
}

pub trait Append<'conn> {
    // TODO: add ON CONFLICT parameter

    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError>;

    fn finish(&mut self, params: ()) -> Result<(), ConnectorError>;
}

pub trait EditSchema {
    fn table_create(&mut self, name: &str, schema: Arc<Schema>) -> Result<(), TableCreateError>;

    fn table_drop(&mut self, name: &str) -> Result<(), TableDropError>;
}

pub mod unimplemented {
    pub struct Appender;

    impl<'conn> super::Append<'conn> for Appender {
        fn append(
            &mut self,
            _: arrow::record_batch::RecordBatch,
        ) -> Result<(), crate::ConnectorError> {
            unimplemented!()
        }

        fn finish(&mut self, _: ()) -> Result<(), crate::ConnectorError> {
            unimplemented!()
        }
    }
}
