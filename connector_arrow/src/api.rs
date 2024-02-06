use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};

use super::errors::ConnectorError;

/// A connection to a data store.
pub trait Connection {
    type Stmt<'conn>: Statement<'conn>
    where
        Self: 'conn;

    fn prepare<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError>;

    fn get_relation_defs(&mut self) -> Result<Vec<RelationDef>, ConnectorError>;
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
    fn get_schema(&mut self) -> Result<Arc<arrow::datatypes::Schema>, ConnectorError>;
}

/// A definition of a relation stored in a data store. Also known as a "table".
#[derive(Debug)]
pub struct RelationDef {
    pub name: String,
    pub schema: Schema,
}
