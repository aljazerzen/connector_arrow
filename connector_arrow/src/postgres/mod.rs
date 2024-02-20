//! Provides `connector_arrow` traits for [postgres crate](https://docs.rs/postgres).
//!
//! ```no_run
//! use postgres::{Client, NoTls};
//! use connector_arrow::postgres::{PostgresConnection, ProtocolExtended};
//! use connector_arrow::api::Connection;
//!
//! let client = Client::connect("postgres://localhost:5432/my_db", NoTls).unwrap();
//!
//! let mut conn = PostgresConnection::<ProtocolExtended>::new(client);
//!
//! let stmt = conn.query("SELECT * FROM my_table").unwrap();
//! ````

mod append;
mod protocol_extended;
mod protocol_simple;
mod schema;
mod types;

use arrow::datatypes::DataType;
use postgres::Client;
use std::marker::PhantomData;
use thiserror::Error;

use crate::api::{Connection, Statement};
use crate::errors::ConnectorError;

/// Connection to PostgreSQL that implements [Connection], [crate::api::SchemaGet] and [crate::api::SchemaEdit].
///
/// Requires generic argument `Protocol`, which can be one of the following types:
/// - [ProtocolExtended]
/// - [ProtocolSimple]
pub struct PostgresConnection<Protocol> {
    client: Client,
    _protocol: PhantomData<Protocol>,
}

impl<Protocol> PostgresConnection<Protocol> {
    pub fn new(client: Client) -> Self {
        PostgresConnection {
            client,
            _protocol: PhantomData,
        }
    }

    pub fn unwrap(self) -> Client {
        self.client
    }
}

/// Extended PostgreSQL wire protocol.
/// Supports query parameters (but they are not yet implemented).
/// Supports streaming, with batch size of 1024.
pub struct ProtocolExtended;

/// Simple PostgreSQL wire protocol.
/// This protocol returns the values in rows as strings rather than in their binary encodings.
/// Does not support query parameters.
/// Does not support streaming.
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

impl<P> Connection for PostgresConnection<P>
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
            client: &mut self.client,
            query: query.to_string(),
            stmt,
            _protocol: &PhantomData,
        })
    }

    fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError> {
        append::PostgresAppender::new(&mut self.client, table_name)
    }

    fn coerce_type(ty: &DataType) -> Option<DataType> {
        match ty {
            DataType::Null => Some(DataType::Int16),
            DataType::Int8 => Some(DataType::Int16),
            DataType::UInt8 => Some(DataType::Int16),
            DataType::UInt16 => Some(DataType::Int32),
            DataType::UInt32 => Some(DataType::Int64),
            DataType::UInt64 => Some(DataType::Decimal128(20, 0)),
            DataType::Float16 => Some(DataType::Float32),
            DataType::Utf8 => Some(DataType::LargeUtf8),
            _ => None,
        }
    }
}

pub struct PostgresStatement<'conn, P> {
    client: &'conn mut Client,
    query: String,
    stmt: postgres::Statement,
    _protocol: &'conn PhantomData<P>,
}
