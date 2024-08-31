mod append;
mod query;
mod schema;
mod types;

use arrow::datatypes::*;
use mysql::prelude::*;

use crate::api::Connector;
use crate::ConnectorError;

pub struct MySQLConnection<Q: Queryable> {
    pub queryable: Q,
}

impl<C: Queryable> MySQLConnection<C> {
    pub fn new(conn: C) -> Self {
        MySQLConnection { queryable: conn }
    }

    pub fn unwrap(self) -> C {
        self.queryable
    }
}

impl<C: Queryable> Connector for MySQLConnection<C> {
    type Stmt<'conn> = query::MySQLStatement<'conn, C> where Self: 'conn;

    type Append<'conn> = append::MySQLAppender<'conn, C> where Self: 'conn;

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
        dbg!(ty);

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

        Some(match (ty, unsigned) {
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

            ("tinytext" | "mediumtext" | "longtext" | "text" | "varchar", _) => DataType::Utf8,

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

                DataType::Utf8 => "longtext",
                DataType::LargeUtf8 => return None,

                _ => return None,
            }
            .to_string(),
        )
    }
}
