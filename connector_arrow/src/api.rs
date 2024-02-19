//! Database client interface that uses Apache Arrow as data-transfer format and schema definition format.
//!
//! The important traits are:
//! - [Connection], providing [Connection::query] and [Connection::append] functions,
//! - [SchemaGet], for schema introspection,
//! - [SchemaEdit], for basic schema migration commands,

use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::errors::{ConnectorError, TableCreateError, TableDropError};

/// A connection to a data store.
pub trait Connection {
    type Stmt<'conn>: Statement<'conn>
    where
        Self: 'conn;

    type Append<'conn>: Append<'conn>
    where
        Self: 'conn;

    /// Prepare a query to the data store, using data store's preferred query language.
    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError>;

    /// Prepare an appender for the given table.
    fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError>;

    /// Describes how a given type will change during a roundtrip to the database.
    /// None means that type will not change at all.
    fn coerce_type(ty: &DataType) -> Option<DataType>;
}

/// A task that is to be executed in the data store, over a connection.
pub trait Statement<'conn> {
    type Params: Send + Sync + Clone;

    type Reader<'stmt>: ResultReader<'stmt>
    where
        Self: 'stmt;

    /// Start executing.
    /// This will create a reader that can retrieve the result schema and data.
    fn start(&mut self, params: ()) -> Result<Self::Reader<'_>, ConnectorError>;
}

/// Reads result of the query, starting with the schema.
pub trait ResultReader<'stmt>: Iterator<Item = Result<RecordBatch, ConnectorError>> {
    /// Return the schema of the result.
    fn get_schema(&mut self) -> Result<SchemaRef, ConnectorError>;
}

/// Receive [RecordBatch]es that have to be written to a table in the data store.
pub trait Append<'conn> {
    // TODO: add ON CONFLICT parameter

    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError>;

    fn finish(self) -> Result<(), ConnectorError>;
}

/// Schema introspection
pub trait SchemaGet {
    fn table_list(&mut self) -> Result<Vec<String>, ConnectorError>;

    fn table_get(&mut self, name: &str) -> Result<SchemaRef, ConnectorError>;
}

/// Schema migration
pub trait SchemaEdit {
    fn table_create(&mut self, name: &str, schema: SchemaRef) -> Result<(), TableCreateError>;

    fn table_drop(&mut self, name: &str) -> Result<(), TableDropError>;
}

/// Stub types that can be used to signal that certain capability is not implemented
/// for a data source.
pub mod unimplemented {
    pub struct Appender;

    impl<'conn> super::Append<'conn> for Appender {
        fn append(
            &mut self,
            _: arrow::record_batch::RecordBatch,
        ) -> Result<(), crate::ConnectorError> {
            unimplemented!()
        }

        fn finish(self) -> Result<(), crate::ConnectorError> {
            unimplemented!()
        }
    }
}
