mod append;
mod query;
mod schema;
mod types;

use crate::api::Connection;
use crate::errors::ConnectorError;

use query::SQLiteStatement;

impl Connection for rusqlite::Connection {
    type Stmt<'conn> = SQLiteStatement<'conn> where Self: 'conn;

    type Append<'conn> = append::SQLiteAppender<'conn> where Self: 'conn;

    fn query(&mut self, query: &str) -> Result<SQLiteStatement, ConnectorError> {
        let stmt = rusqlite::Connection::prepare(self, query)?;
        Ok(SQLiteStatement { stmt })
    }

    fn append<'a>(&'a mut self, table: &str) -> Result<Self::Append<'a>, ConnectorError> {
        let transaction = self.transaction()?;

        append::SQLiteAppender::new(table.to_string(), transaction)
    }
}
