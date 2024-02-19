//! Provides `connector_arrow` traits for [postgres crate](https://docs.rs/postgres).
//!
//! ```no_run
//! use postgres::{Client, NoTls};
//!
//! let client = Client::connect("postgres://localhost:5432/my_db", NoTls).unwrap();
//!
//! let mut conn = PostgresConnection::<ProtocolCursor>::new(client);
//!
//! let stmt = conn.query("SELECT * FROM my_table").unwrap();
//! ````

mod append;
mod protocol_extended;
mod protocol_simple;
mod schema;
mod types;

use postgres::Client;
use std::marker::PhantomData;
use thiserror::Error;

use crate::api::{Connection, Statement};
use crate::errors::ConnectorError;

pub struct PostgresConnection<'a, P> {
    client: &'a mut Client,
    _protocol: PhantomData<P>,
}

impl<'a, P> PostgresConnection<'a, P> {
    pub fn new(client: &'a mut Client) -> Self {
        PostgresConnection {
            client,
            _protocol: PhantomData,
        }
    }
}

/// Protocol - use Cursor
pub struct ProtocolExtended;

/// Protocol - use Simple Query
pub struct ProtocolSimple;

// /// Protocol - Binary based bulk load
// pub struct BinaryProtocol;

// /// Protocol - CSV based bulk load
// pub struct CSVProtocol;

#[derive(Error, Debug)]
pub enum PostgresError {
    #[error(transparent)]
    Postgres(#[from] postgres::Error),

    #[error(transparent)]
    CSV(#[from] csv::Error),

    #[error(transparent)]
    Hex(#[from] hex::FromHexError),

    #[error(transparent)]
    IO(#[from] std::io::Error),
}

impl<'c, P> Connection for PostgresConnection<'c, P>
where
    for<'conn> PostgresStatement<'conn, P>: Statement<'conn>,
{
    type Stmt<'conn> = PostgresStatement<'conn, P> where Self: 'conn;

    type Append<'conn> = append::PostgresAppender<'conn> where Self: 'conn;

    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        let stmt = self
            .client
            .prepare(query)
            .map_err(PostgresError::Postgres)?;
        Ok(PostgresStatement {
            client: self.client,
            query: query.to_string(),
            stmt,
            _protocol: &PhantomData,
        })
    }

    fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError> {
        append::PostgresAppender::new(self.client, table_name)
    }
}

pub struct PostgresStatement<'conn, P> {
    client: &'conn mut Client,
    query: String,
    stmt: postgres::Statement,
    _protocol: &'conn PhantomData<P>,
}
