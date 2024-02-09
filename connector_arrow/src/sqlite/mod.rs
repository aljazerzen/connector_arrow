mod append;
mod query;
mod schema;
mod types;

use std::sync::Arc;

use crate::api::{Connection, EditSchema, TableSchema};
use crate::errors::{ConnectorError, TableCreateError, TableDropError};

use arrow::datatypes::Schema;
use query::SQLiteStatement;

impl Connection for rusqlite::Connection {
    type Stmt<'conn> = SQLiteStatement<'conn> where Self: 'conn;

    type Append<'conn> = append::SQLiteAppender<'conn> where Self: 'conn;

    fn query(&mut self, query: &str) -> Result<SQLiteStatement, ConnectorError> {
        let stmt = rusqlite::Connection::prepare(self, query)?;
        Ok(SQLiteStatement { stmt })
    }

    fn get_table_schemas(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        schema::table_list(self)
    }

    fn append<'a>(&'a mut self, table: &str) -> Result<Self::Append<'a>, ConnectorError> {
        let transaction = self.transaction()?;

        append::SQLiteAppender::new(table.to_string(), transaction)
    }
}

impl EditSchema for rusqlite::Connection {
    fn table_create(&mut self, name: &str, schema: Arc<Schema>) -> Result<(), TableCreateError> {
        schema::table_create(self, name, schema)
    }

    fn table_drop(&mut self, name: &str) -> Result<(), TableDropError> {
        schema::table_drop(self, name)
    }
}
