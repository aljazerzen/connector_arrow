//! Provides `connector_arrow` traits for [rusqlite crate](https://docs.rs/rusqlite).

mod append;
mod query;
mod schema;
mod types;

use crate::api::Connection;
use crate::errors::ConnectorError;

#[doc(hidden)]
pub use append::SQLiteAppender;
#[doc(hidden)]
pub use query::SQLiteStatement;

impl Connection for rusqlite::Connection {
    type Stmt<'conn> = SQLiteStatement<'conn> where Self: 'conn;

    type Append<'conn> = SQLiteAppender<'conn> where Self: 'conn;

    fn query(&mut self, query: &str) -> Result<SQLiteStatement, ConnectorError> {
        let stmt = rusqlite::Connection::prepare(self, query)?;
        Ok(SQLiteStatement { stmt })
    }

    fn append<'a>(&'a mut self, table: &str) -> Result<Self::Append<'a>, ConnectorError> {
        let transaction = self.transaction()?;

        SQLiteAppender::new(table.to_string(), transaction)
    }
}
