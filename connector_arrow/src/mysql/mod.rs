mod append;
mod query;
mod schema;
mod types;

pub use mysql;

use arrow::datatypes::*;
use mysql::prelude::*;

use crate::api::Connector;
use crate::ConnectorError;

pub struct MySQLConnection<Q: Queryable> {
    queryable: Q,
}

impl<Q: Queryable> MySQLConnection<Q> {
    pub fn new(conn: Q) -> Self {
        MySQLConnection { queryable: conn }
    }

    pub fn unwrap(self) -> Q {
        self.queryable
    }

    pub fn inner_mut(&mut self) -> &mut Q {
        &mut self.queryable
    }
}

impl<Q: Queryable> Connector for MySQLConnection<Q> {
    type Stmt<'conn>
        = query::MySQLStatement<'conn, Q>
    where
        Self: 'conn;

    type Append<'conn>
        = append::MySQLAppender<'conn, Q>
    where
        Self: 'conn;

    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        let stmt = self.queryable.prep(query)?;
        Ok(query::MySQLStatement {
            queryable: &mut self.queryable,
            stmt,
        })
    }

    fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError> {
        append::MySQLAppender::new(&mut self.queryable, table_name)
    }

    fn type_db_into_arrow(ty: &str) -> Option<DataType> {
        let (ty, unsigned) = ty
            .strip_suffix(" unsigned")
            .map(|p| (p, true))
            .unwrap_or((ty, false));

        // strip size suffix and anything following it
        let ty = if let Some(open_parent) = ty.find('(') {
            &ty[0..open_parent]
        } else {
            ty
        };
        let ty = ty.to_lowercase();

        Some(match (ty.as_str(), unsigned) {
            ("null", _) => DataType::Null,

            ("tinyint" | "bool" | "boolean", false) => DataType::Int8,
            ("smallint", false) => DataType::Int16,
            ("integer" | "int", false) => DataType::Int32,
            ("bigint", false) => DataType::Int64,

            ("tinyint", true) => DataType::UInt8,
            ("smallint", true) => DataType::UInt16,
            ("integer" | "int", true) => DataType::UInt32,
            ("bigint", true) => DataType::UInt64,

            ("real" | "float" | "float4", _) => DataType::Float32,
            ("double" | "float8", _) => DataType::Float64,

            ("bit" | "tinyblob" | "mediumblob" | "longblob" | "blob" | "binary", _) => {
                DataType::Binary
            }

            ("tinytext" | "mediumtext" | "longtext" | "text" | "varchar" | "char", _) => {
                DataType::Utf8
            }

            ("decimal" | "numeric" | "newdecimal", _) => DataType::Utf8,

            // MySQL DATETIME has range 1000-01-01 00:00:00.000000 and
            // 9999-12-31 23:59:59.999999 with microsecond precision.
            // Arrow Date64 has only millisecond precision, so it cannot be used.
            // MySQL DATETIME uses "current time" used by the server, which is unknown
            // to connector_arrow. Because of that, it also cannot be converted to a timestamp,
            // even a timestamp of unknown timezone.
            // So we default to Utf8.
            // TODO: if we send `SET timezone = 'UTC'` before executing queries, we could convert
            // to timestamp in 'UTC' timezone.
            ("datetime" | "timestamp", _) => DataType::Utf8,

            _ => return None,
        })
    }

    fn type_arrow_into_db(ty: &DataType) -> Option<String> {
        Some(
            match ty {
                DataType::Null => "tinyint",
                DataType::Boolean => "tinyint",
                DataType::Int8 => "tinyint",
                DataType::Int16 => "smallint",
                DataType::Int32 => "integer",
                DataType::Int64 => "bigint",
                DataType::UInt8 => "tinyint unsigned",
                DataType::UInt16 => "smallint unsigned",
                DataType::UInt32 => "integer unsigned",
                DataType::UInt64 => "bigint unsigned",
                DataType::Float16 => "float",
                DataType::Float32 => "float",
                DataType::Float64 => "double",

                DataType::Binary => "longblob",
                DataType::FixedSizeBinary(1) => "binary",
                DataType::FixedSizeBinary(2) => "blob",
                DataType::FixedSizeBinary(3) => "mediumblob",
                DataType::FixedSizeBinary(4) => "longblob",
                DataType::FixedSizeBinary(_) => return None,
                DataType::LargeBinary => return None,

                DataType::Decimal128(p, s) | DataType::Decimal256(p, s) if *p <= 65 => {
                    return Some(format!("decimal({p}, {s})"))
                }
                DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "text",

                // MySQL TIMESTAMP cannot store timezone in schema, when inserting it will
                // use server's "current time zone". Since we don't know what that is, we cannot
                // convert timestamps to DATETIME.
                // MySQL TIMESTAMP can store timestamps up to micro second precision.
                //
                // So we store the timestamp as a number of time units from unix epoch.
                // Beware: this loses time unit and time zone information.
                DataType::Timestamp(_, _) => "bigint",

                DataType::Utf8 => "longtext",
                DataType::LargeUtf8 => return None,

                _ => return None,
            }
            .to_string(),
        )
    }
}
