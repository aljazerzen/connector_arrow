//! Provides `connector_arrow` traits for [postgres crate](https://docs.rs/postgres).
//!
//! ```no_run
//! use postgres::{Client, NoTls};
//! use connector_arrow::postgres::{PostgresConnection, ProtocolExtended};
//! use connector_arrow::api::Connector;
//!
//! let client = Client::connect("postgres://localhost:5432/my_db", NoTls).unwrap();
//!
//! let mut conn = PostgresConnection::<ProtocolExtended>::new(client);
//!
//! let stmt = conn.query("SELECT * FROM my_table").unwrap();
//! ````

mod append;
mod decimal;
mod protocol_extended;
mod protocol_simple;
mod schema;
mod types;

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use postgres::Client;
use std::marker::PhantomData;
use thiserror::Error;

use crate::api::{Connector, Statement};
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

impl<P> Connector for PostgresConnection<P>
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
            DataType::UInt64 => Some(DataType::Utf8),
            DataType::Float16 => Some(DataType::Float32),

            // PostgreSQL timestamps cannot store timezone in the schema.
            // PostgreSQL timestamps are microseconds since 2000-01-01T00:00.
            // Arrow timestamps can be microseconds since   1970-01-01T00:00.
            // ... which means we cannot store the full range of the Arrow microsecond
            //     timestamp in PostgreSQL timestamp without changing its meaning.
            // ... so we must use Int64 instead.
            DataType::Timestamp(_, _) => Some(DataType::Int64),
            DataType::Date32 => Some(DataType::Int32),
            DataType::Date64 => Some(DataType::Int64),
            DataType::Time32(_) => Some(DataType::Int32),
            DataType::Time64(_) => Some(DataType::Int64),
            DataType::Duration(_) => Some(DataType::Int64),

            DataType::LargeUtf8 => Some(DataType::Utf8),
            DataType::LargeBinary => Some(DataType::Binary),
            DataType::FixedSizeBinary(_) => Some(DataType::Binary),

            DataType::Decimal128(_, _) => Some(DataType::Utf8),
            DataType::Decimal256(_, _) => Some(DataType::Utf8),
            _ => None,
        }
    }

    fn type_db_into_arrow(ty: &str) -> Option<DataType> {
        Some(match ty {
            "boolean" | "bool" => DataType::Boolean,
            "smallint" | "int2" => DataType::Int16,
            "integer" | "int4" => DataType::Int32,
            "bigint" | "int8" => DataType::Int64,
            "real" | "float4" => DataType::Float32,
            "double precision" | "float8" => DataType::Float64,
            "numeric" | "decimal" => DataType::Utf8,

            "timestamp" | "timestamp without time zone" => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            "timestamptz" | "timestamp with time zone" => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
            }
            "date" => DataType::Date32,
            "time" | "time without time zone" => DataType::Time64(TimeUnit::Microsecond),
            "interval" => DataType::Interval(IntervalUnit::MonthDayNano),

            "bytea" => DataType::Binary,
            "bit" | "bit varying" | "varbit" => DataType::Binary,
            _ if ty.starts_with("bit") => DataType::Binary,

            "text" | "varchar" | "char" | "bpchar" => DataType::Utf8,
            _ if ty.starts_with("varchar") | ty.starts_with("char") | ty.starts_with("bpchar") => {
                DataType::Utf8
            }

            _ => return None,
        })
    }
}

pub struct PostgresStatement<'conn, P> {
    client: &'conn mut Client,
    query: String,
    stmt: postgres::Statement,
    _protocol: &'conn PhantomData<P>,
}
