mod protocol_cursor;
mod protocol_simple;
mod types;

use postgres::Client;
use std::marker::PhantomData;
use thiserror::Error;

use super::{
    api::{Connection, Statement},
    errors::ConnectorError,
};

pub struct PostgresConnection<'a, P> {
    client: &'a mut Client,
    // conn_downcast_mut: Arc<FnConnDowncastMut>,
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

/// Protocol - use Simple Query
pub struct SimpleProtocol;

/// Protocol - use Cursor
#[allow(dead_code)]
pub struct CursorProtocol;

/// Protocol - Binary based bulk load
#[allow(dead_code)]
pub struct BinaryProtocol;

/// Protocol - CSV based bulk load
#[allow(dead_code)]
pub struct CSVProtocol;

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
    type Stmt<'conn> = PostgresStatement<'conn, P>
    where
        Self: 'conn;

    fn prepare<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        let stmt = self.client.prepare(query).map_err(PostgresError::from)?;
        Ok(PostgresStatement {
            client: self.client,
            query: query.to_string(),
            stmt,
            _protocol: &PhantomData,
        })
    }

    fn get_relation_defs(&mut self) -> Result<Vec<crate::api::RelationDef>, ConnectorError> {
        unimplemented!()
    }
}

pub struct PostgresStatement<'conn, P> {
    client: &'conn mut Client,
    query: String,
    stmt: postgres::Statement,
    _protocol: &'conn PhantomData<P>,
}
