mod append;
mod query;
mod schema;
mod types;

use arrow::datatypes::*;
use futures::{AsyncRead, AsyncWrite};
use itertools::Itertools;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::api::Connector;
use crate::ConnectorError;

pub struct TiberiusConnection<S: AsyncRead + AsyncWrite + Unpin + Send> {
    rt: Arc<Runtime>,
    client: tiberius::Client<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> TiberiusConnection<S> {
    pub fn new(rt: Arc<Runtime>, client: tiberius::Client<S>) -> Self {
        TiberiusConnection { rt, client }
    }

    pub fn unwrap(self) -> (Arc<Runtime>, tiberius::Client<S>) {
        (self.rt, self.client)
    }

    pub fn inner_mut(&mut self) -> (&mut Arc<Runtime>, &mut tiberius::Client<S>) {
        (&mut self.rt, &mut self.client)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connector for TiberiusConnection<S> {
    type Stmt<'conn> = query::TiberiusStatement<'conn, S> where Self: 'conn;

    type Append<'conn> = append::TiberiusAppender<'conn, S> where Self: 'conn;

    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        Ok(query::TiberiusStatement {
            conn: self,
            query: query.to_string(),
        })
    }

    fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError> {
        append::TiberiusAppender::new(self.rt.clone(), &mut self.client, table_name)
    }

    #[allow(clippy::get_first)]
    fn type_db_into_arrow(ty: &str) -> Option<DataType> {
        let ty = ty.to_lowercase();

        // parse arguments
        let (name, args) = if let Some((name, args)) = ty.split_once('(') {
            (
                name,
                args.trim_end_matches(')')
                    .split(',')
                    .filter_map(|a| a.trim().parse::<i16>().ok())
                    .collect_vec(),
            )
        } else {
            (ty.as_str(), vec![])
        };

        Some(match name {
            "null" | "intn" => DataType::Null,
            "bit" => DataType::Boolean,

            "tinyint" => DataType::UInt8,
            "smallint" => DataType::Int16,
            "int" => DataType::Int32,
            "bigint" => DataType::Int64,

            "char" | "nchar" | "varchar" | "nvarchar" | "text" | "ntext" => DataType::Utf8,

            "real" | "float" => {
                let is_f32 = args
                    .get(0)
                    .map(|p| *p <= 24)
                    .unwrap_or_else(|| name == "real");
                if is_f32 {
                    DataType::Float32
                } else {
                    DataType::Float64
                }
            }

            "decimal" | "numeric" => {
                let precision = args.get(0).cloned().unwrap_or(18) as u8;
                let scale = args.get(1).cloned().unwrap_or(0) as i8;

                if precision <= 128 {
                    DataType::Decimal128(precision, scale)
                } else {
                    DataType::Decimal256(precision, scale)
                }
            }

            _ => return None,
        })
    }

    fn type_arrow_into_db(ty: &DataType) -> Option<String> {
        Some(
            match ty {
                DataType::Null => "tinyint",
                DataType::Boolean => "bit",
                DataType::Int8 => "smallint",
                DataType::Int16 => "smallint",
                DataType::Int32 => "int",
                DataType::Int64 => "bigint",
                DataType::UInt8 => "tinyint",
                DataType::UInt16 => "int",
                DataType::UInt32 => "bigint",
                DataType::UInt64 => "decimal(20, 0)",

                DataType::Float32 => "float(24)", // 24 bits in mantissa
                DataType::Float64 => "float(53)", // 53 bits in mantissa
                DataType::Float16 => "float(24)", // could be float(11), but there is no storage saved

                // DataType::Timestamp(_, _) => todo!(),
                // DataType::Date32 => todo!(),
                // DataType::Date64 => todo!(),
                // DataType::Time32(_) => todo!(),
                // DataType::Time64(_) => todo!(),
                // DataType::Duration(_) => todo!(),
                // DataType::Interval(_) => todo!(),
                // DataType::Binary => "varbinary",
                // DataType::FixedSizeBinary(0) => "varbinary",
                // DataType::FixedSizeBinary(size) => return Some(format!("binary({size})")),
                // DataType::LargeBinary => "varbinary",

                // DataType::BinaryView => todo!(),
                DataType::Utf8 => "nvarchar(max)",
                DataType::LargeUtf8 => "nvarchar(max)",
                // DataType::Utf8View => todo!(),
                // DataType::List(_) => todo!(),
                // DataType::ListView(_) => todo!(),
                // DataType::FixedSizeList(_, _) => todo!(),
                // DataType::LargeList(_) => todo!(),
                // DataType::LargeListView(_) => todo!(),
                // DataType::Struct(_) => todo!(),
                // DataType::Union(_, _) => todo!(),
                // DataType::Dictionary(_, _) => todo!(),
                // DataType::Decimal128(_, _) => todo!(),
                // DataType::Decimal256(_, _) => todo!(),
                // DataType::Map(_, _) => todo!(),
                // DataType::RunEndEncoded(_, _) => todo!(),
                _ => return None,
            }
            .to_string(),
        )
    }
}
