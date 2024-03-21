mod query;
mod types;

use arrow::datatypes::*;
use mysql::prelude::*;

use crate::api::{unimplemented, Connector};
use crate::ConnectorError;

pub struct MySQLConnection<C: Queryable> {
    conn: C,
}

impl<C: Queryable> MySQLConnection<C> {
    pub fn new(conn: C) -> Self {
        MySQLConnection { conn }
    }

    pub fn unwrap(self) -> C {
        self.conn
    }
}

impl<C: Queryable> Connector for MySQLConnection<C> {
    type Stmt<'conn> = query::MySQLStatement<'conn, C> where Self: 'conn;

    type Append<'conn> = unimplemented::Appender where Self: 'conn;

    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        let stmt = self.conn.prep(query)?;
        Ok(query::MySQLStatement {
            conn: &mut self.conn,
            stmt,
        })
    }

    fn append<'a>(&'a mut self, _table_name: &str) -> Result<Self::Append<'a>, ConnectorError> {
        Ok(unimplemented::Appender {})
    }

    fn type_db_into_arrow(ty: &str) -> Option<DataType> {
        Some(match ty {
            "null" => DataType::Null,

            "tinyint" | "bool" | "boolean" => DataType::Int8,
            "smallint" => DataType::Int16,
            "integer" | "int" => DataType::Int32,
            "bigint" => DataType::Int64,

            "tinyint unsigned" => DataType::UInt8,
            "smallint unsigned" => DataType::UInt16,
            "integer unsigned" | "int unsigned" => DataType::UInt32,
            "bigint unsigned" => DataType::UInt64,

            "real" | "float4" => DataType::Float32,
            "double" | "float8" => DataType::Float64,

            "bytea" => DataType::Binary,
            "bit" | "tiny_blob" | "medium_blob" | "long_blob" | "blob" => DataType::Binary,

            "varchar" | "var_string" | "string" => DataType::Utf8,

            _ => return None,
        })
    }

    fn type_arrow_into_db(_ty: &DataType) -> Option<String> {
        None
    }
}
