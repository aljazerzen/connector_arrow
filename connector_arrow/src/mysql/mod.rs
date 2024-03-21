mod types;

use arrow::{datatypes::*, record_batch::RecordBatch};
use mysql::prelude::*;
use pac_cell::PacCell;

use crate::api::{unimplemented, Connector, ResultReader, Statement};
use crate::impl_produce_unsupported;
use crate::types::{ArrowType, FixedSizeBinaryType, NullType};
use crate::util::transport::ProduceTy;
use crate::util::{self, transport::Produce};
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

pub struct MySQLStatement<'conn, C: Queryable> {
    stmt: mysql::Statement,
    conn: &'conn mut C,
}

impl<C: Queryable> Connector for MySQLConnection<C> {
    type Stmt<'conn> = MySQLStatement<'conn, C> where Self: 'conn;

    type Append<'conn> = unimplemented::Appender where Self: 'conn;

    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        let stmt = self.conn.prep(query)?;
        Ok(MySQLStatement {
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

impl<'conn, C: Queryable> Statement<'conn> for MySQLStatement<'conn, C> {
    type Reader<'stmt> = MySQLQueryResult<'stmt>
    where
        Self: 'stmt;

    fn start<'p, I>(&mut self, _params: I) -> Result<Self::Reader<'_>, ConnectorError>
    where
        I: IntoIterator<Item = &'p dyn crate::api::ArrowValue>,
    {
        // TODO: params

        let query_result = self.conn.exec_iter(&self.stmt, ())?;

        // PacCell is needed so we can return query_result and result_set that mutably borrows query result.
        let pac = PacCell::try_new(query_result, |qr| -> Result<_, ConnectorError> {
            let result_set = qr.iter().ok_or(ConnectorError::NoResultSets)?;
            let schema = types::get_result_schema(&result_set)?;
            Ok(MySQLResultReader { result_set, schema })
        })?;
        Ok(MySQLQueryResult(pac))
    }
}

pub struct MySQLQueryResult<'stmt>(
    PacCell<
        mysql::QueryResult<'stmt, 'stmt, 'stmt, mysql::Binary>, // parent
        MySQLResultReader<'stmt>,                               // child
    >,
);

impl<'stmt> ResultReader<'stmt> for MySQLQueryResult<'stmt> {
    fn get_schema(&mut self) -> Result<arrow::datatypes::SchemaRef, ConnectorError> {
        Ok(self.0.with_mut(|x| x.schema.clone()))
    }
}

impl<'stmt> Iterator for MySQLQueryResult<'stmt> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.with_mut(|reader| {
            let schema = reader.schema.clone();
            util::next_batch_from_rows(&schema, reader, 1024).transpose()
        })
    }
}

struct MySQLResultReader<'stmt> {
    result_set: mysql::ResultSet<'stmt, 'stmt, 'stmt, 'stmt, mysql::Binary>,
    schema: SchemaRef,
}

impl<'s> util::RowsReader<'s> for MySQLResultReader<'s> {
    type CellReader<'row> = MySQLCellReader
    where
        Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
        let row = self.result_set.next().transpose()?;
        Ok(row.map(|row| MySQLCellReader { row, cell: 0 }))
    }
}

struct MySQLCellReader {
    row: mysql::Row,
    cell: usize,
}

impl<'a> util::CellReader<'a> for MySQLCellReader {
    type CellRef<'cell> = MySQLCellRef<'cell>
    where
        Self: 'cell;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        let r = MySQLCellRef {
            row: &mut self.row,
            cell: self.cell,
        };
        self.cell += 1;
        Some(r)
    }
}

#[derive(Debug)]
struct MySQLCellRef<'a> {
    row: &'a mut mysql::Row,
    cell: usize,
}

impl<'r> Produce<'r> for MySQLCellRef<'r> {}

#[macro_export]
macro_rules! impl_produce_ty {
    ($p: ty, ($($t: ty,)+)) => {
        $(
            impl<'r> ProduceTy<'r, $t> for $p {
                fn produce(self) -> Result<<$t as ArrowType>::Native, ConnectorError> {
                    Ok(self.row.take(self.cell).unwrap())
                }
                fn produce_opt(self) -> Result<Option<<$t as ArrowType>::Native>, ConnectorError> {
                    Ok(self.row.take(self.cell))
                }
            }
        )+
    };
}

impl_produce_ty!(
    MySQLCellRef<'r>,
    (
        BooleanType,
        Int8Type,
        Int16Type,
        Int32Type,
        Int64Type,
        UInt8Type,
        UInt16Type,
        UInt32Type,
        UInt64Type,
        Float32Type,
        Float64Type,
        Utf8Type,
        BinaryType,
    )
);

impl_produce_unsupported!(
    MySQLCellRef<'r>,
    (
        NullType,
        Float16Type,
        TimestampSecondType,
        TimestampMillisecondType,
        TimestampMicrosecondType,
        TimestampNanosecondType,
        Date32Type,
        Date64Type,
        Time32SecondType,
        Time32MillisecondType,
        Time64MicrosecondType,
        Time64NanosecondType,
        IntervalYearMonthType,
        IntervalDayTimeType,
        IntervalMonthDayNanoType,
        DurationSecondType,
        DurationMillisecondType,
        DurationMicrosecondType,
        DurationNanosecondType,
        LargeUtf8Type,
        LargeBinaryType,
        FixedSizeBinaryType,
        Decimal128Type,
        Decimal256Type,
    )
);
