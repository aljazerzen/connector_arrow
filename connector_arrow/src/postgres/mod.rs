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
mod query;
mod schema;
mod types;

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use postgres::Client;
use thiserror::Error;

use crate::api::Connector;
use crate::errors::ConnectorError;

/// Connection to PostgreSQL that implements [Connection], [crate::api::SchemaGet] and [crate::api::SchemaEdit].
///
/// Requires generic argument `Protocol`, which can be one of the following types:
/// - [ProtocolExtended]
/// - [ProtocolSimple]
pub struct PostgresConnection {
    client: Client,
}

impl PostgresConnection {
    pub fn new(client: Client) -> Self {
        PostgresConnection { client }
    }

    pub fn unwrap(self) -> Client {
        self.client
    }

    pub fn inner_mut(&mut self) -> &mut Client {
        &mut self.client
    }
}

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

impl Connector for PostgresConnection {
    type Stmt<'conn> = query::PostgresStatement<'conn> where Self: 'conn;

    type Append<'conn> = append::PostgresAppender<'conn> where Self: 'conn;

    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        let stmt = self
            .client
            .prepare(query)
            .map_err(PostgresError::Postgres)?;
        Ok(query::PostgresStatement {
            client: &mut self.client,
            query: query.to_string(),
            stmt,
        })
    }

    fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError> {
        append::PostgresAppender::new(&mut self.client, table_name)
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

            "text" | "varchar" | "char" | "bpchar" => DataType::Utf8,

            _ if ty.starts_with("bit") => DataType::Binary,
            _ if ty.starts_with("varchar") | ty.starts_with("char") | ty.starts_with("bpchar") => {
                DataType::Utf8
            }
            _ if ty.starts_with("decimal") | ty.starts_with("numeric") => DataType::Utf8,

            _ => return None,
        })
    }

    fn type_arrow_into_db(ty: &DataType) -> Option<String> {
        Some(
            match ty {
                DataType::Null => "smallint",
                DataType::Boolean => "bool",

                DataType::Int8 => "smallint",
                DataType::Int16 => "smallint",
                DataType::Int32 => "integer",
                DataType::Int64 => "bigint",

                DataType::UInt8 => "smallint",
                DataType::UInt16 => "integer",
                DataType::UInt32 => "bigint",
                DataType::UInt64 => "decimal(20, 0)",

                DataType::Float16 => "real",
                DataType::Float32 => "real",
                DataType::Float64 => "double precision",

                // PostgreSQL timestamps cannot store timezone in the schema.
                // PostgreSQL timestamps are microseconds since 2000-01-01T00:00.
                // Arrow timestamps *can be* microseconds since 1970-01-01T00:00.
                // ... which means we cannot store the full range of the Arrow microsecond
                //     timestamp in PostgreSQL timestamp without changing its meaning.
                // ... so we must Int64 instead.
                DataType::Timestamp(_, _) => "bigint",
                DataType::Date32 => "integer",
                DataType::Date64 => "bigint",
                DataType::Time32(_) => "integer",
                DataType::Time64(_) => "bigint",
                DataType::Duration(_) => "bigint",
                DataType::Interval(_) => return None,

                DataType::Utf8 | DataType::LargeUtf8 => "text",

                DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => "bytea",

                DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                    return Some(format!("decimal({precision}, {scale})"))
                }

                DataType::List(_)
                | DataType::FixedSizeList(_, _)
                | DataType::LargeList(_)
                | DataType::Struct(_)
                | DataType::Union(_, _)
                | DataType::Dictionary(_, _)
                | DataType::Map(_, _)
                | DataType::RunEndEncoded(_, _)
                | DataType::BinaryView
                | DataType::Utf8View
                | DataType::ListView(_)
                | DataType::LargeListView(_) => return None,
            }
            .into(),
        )
    }
}
