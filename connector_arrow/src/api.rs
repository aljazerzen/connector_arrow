//! Database client interface that uses Apache Arrow as data-transfer format and schema definition format.
//!
//! The important traits are:
//! - [Connector], providing [Connector::query] and [Connector::append] functions,
//! - [SchemaGet], for schema introspection,
//! - [SchemaEdit], for basic schema migration commands,

use std::any::Any;

use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::errors::{ConnectorError, TableCreateError, TableDropError};

/// Ability to query data from a data store and append data into the data store.
pub trait Connector {
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

    /// Describes how database types map into the arrow types.
    /// None means that querying this type will return [DataType::Binary] with field
    /// metadata [METADATA_DB_TYPE] set to original type name.
    // I cannot think of a better name. I'd want it to start with `type_`.
    fn type_db_into_arrow(_database_ty: &str) -> Option<DataType>;

    /// Describes how arrow types map into the database types.
    /// None means that appending this type is not supported.
    fn type_arrow_into_db(_ty: &DataType) -> Option<String>;
}

/// A task that is to be executed in the data store, over a connection.
pub trait Statement<'conn> {
    type Reader<'stmt>: ResultReader<'stmt>
    where
        Self: 'stmt;

    /// Execute this statement once.
    /// Returns a reader that can retrieve the result schema and data.
    fn start<'p, I>(&mut self, args: I) -> Result<Self::Reader<'_>, ConnectorError>
    where
        I: IntoIterator<Item = &'p dyn ArrowValue>,
    {
        let args: Vec<_> = args.into_iter().collect();
        let batch = crate::params::vec_to_record_batch(args)?;
        self.start_batch((&batch, 0))
    }

    /// Execute this statement once.
    /// Query arguments are read from record batch, from the specified row.
    /// Returns a reader that can retrieve the result schema and data.
    fn start_batch(
        &mut self,
        args: (&RecordBatch, usize),
    ) -> Result<Self::Reader<'_>, ConnectorError>;
}

/// Reads result of the query, starting with the schema.
pub trait ResultReader<'stmt>: Iterator<Item = Result<RecordBatch, ConnectorError>> {
    /// Return the schema of the result.
    fn get_schema(&mut self) -> Result<SchemaRef, ConnectorError>;
}

/// Key of the metadata on [arrow::datatypes::Field] that stores the name of the database type
/// that this field was created from.
pub const METADATA_DB_TYPE: &str = "db_type";

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

/// A value from the Arrow type system.
///
/// Can only be implemented in this crate.
pub trait ArrowValue: sealed::Sealed + Any + std::fmt::Debug {
    fn get_data_type(&self) -> &DataType;

    fn as_any(&self) -> &dyn Any;
}

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Stub types that can be used to signal that certain capability is not implemented
/// for a data source.
pub mod unimplemented {
    pub struct Appender;

    impl super::Append<'_> for Appender {
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
