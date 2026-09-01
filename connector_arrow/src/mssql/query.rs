use arrow::{datatypes::*, record_batch::RecordBatch};
use futures::{AsyncRead, AsyncWrite, StreamExt};
use itertools::Itertools;
use std::sync::Arc;
use mssql::{ColumnData, QueryStream, ToSql};
use tokio::runtime::Runtime;

use crate::api::{ResultReader, Statement};
use crate::impl_produce_unsupported;
use crate::types::{ArrowType, FixedSizeBinaryType, NullType};
use crate::util::transport::{self, ProduceTy};
use crate::util::ArrayCellRef;
use crate::util::{self, transport::Produce};
use crate::ConnectorError;

pub struct MssqlStatement<'conn, S: AsyncRead + AsyncWrite + Unpin + Send> {
    pub(super) conn: &'conn mut super::MssqlConnection<S>,
    pub(super) query: String,
}

impl<'conn, S: AsyncRead + AsyncWrite + Unpin + Send> Statement<'conn>
    for MssqlStatement<'conn, S>
{
    type Reader<'stmt>
        = MssqlResultReader<'stmt>
    where
        Self: 'stmt;

    fn start_batch<'p>(
        &mut self,
        args: (&RecordBatch, usize),
    ) -> Result<Self::Reader<'_>, ConnectorError> {
        // args
        let arg_cells = ArrayCellRef::vec_from_batch(args.0, args.1);
        let mut args: Vec<ColumnData<'static>> = Vec::with_capacity(arg_cells.len());
        for cell in arg_cells {
            transport::transport(cell.field, &cell, &mut args)?;
        }
        let args = args.iter().map(Value).collect_vec();
        let args = args.iter().map(|a| a as &dyn ToSql).collect_vec();

        // query
        let mut stream = self
            .conn
            .rt
            .block_on(self.conn.client.query(&self.query, args.as_slice()))?;

        // get columns
        let columns = self.conn.rt.block_on(stream.columns())?;
        let schema = super::types::get_result_schema(columns)?;
        self.conn.rt.block_on(stream.next());

        Ok(MssqlResultReader {
            schema,
            stream: MssqlStream {
                rt: self.conn.rt.clone(),
                stream,
            },
        })
    }
}

pub struct MssqlResultReader<'stmt> {
    schema: SchemaRef,
    stream: MssqlStream<'stmt>,
}

struct MssqlStream<'stmt> {
    rt: Arc<Runtime>,
    stream: QueryStream<'stmt>,
}

impl<'stmt> ResultReader<'stmt> for MssqlResultReader<'stmt> {
    fn get_schema(&mut self) -> Result<arrow::datatypes::SchemaRef, ConnectorError> {
        Ok(self.schema.clone())
    }
}

impl Iterator for MssqlResultReader<'_> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        util::next_batch_from_rows(&self.schema, &mut self.stream, 1024).transpose()
    }
}

impl<'s> util::RowsReader<'s> for MssqlStream<'s> {
    type CellReader<'row>
        = MssqlCellReader
    where
        Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
        let item = self.rt.block_on(self.stream.next());

        // are we done?
        let Some(item) = item else { return Ok(None) };

        // are there more result sets?
        let row = match item? {
            mssql::QueryItem::Row(row) => row,
            mssql::QueryItem::Metadata(_) => {
                // yes, this there are
                return Err(ConnectorError::MultipleResultSets);
            }
        };

        Ok(Some(MssqlCellReader { row, cell: 0 }))
    }
}

struct MssqlCellReader {
    row: mssql::Row,
    cell: usize,
}

impl util::CellReader<'_> for MssqlCellReader {
    type CellRef<'cell>
        = MssqlCellRef<'cell>
    where
        Self: 'cell;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        let r = MssqlCellRef {
            row: &mut self.row,
            cell: self.cell,
        };
        self.cell += 1;
        Some(r)
    }
}

#[derive(Debug)]
struct MssqlCellRef<'a> {
    row: &'a mut mssql::Row,
    cell: usize,
}

impl<'r> Produce<'r> for MssqlCellRef<'r> {}

macro_rules! impl_produce_ty {
    ($ArrTy: ty, $DbTy: ty) => {
        impl_produce_ty!($ArrTy, $DbTy, std::convert::identity);
    };

    ($ArrTy: ty, $DbTy: ty, $conversion: expr) => {
        impl<'r> ProduceTy<'r, $ArrTy> for MssqlCellRef<'r> {
            fn produce(self) -> Result<<$ArrTy as ArrowType>::Native, ConnectorError> {
                Ok(self
                    .row
                    .get::<$DbTy, usize>(self.cell)
                    .map($conversion)
                    .unwrap())
            }
            fn produce_opt(self) -> Result<Option<<$ArrTy as ArrowType>::Native>, ConnectorError> {
                Ok(self.row.get::<$DbTy, usize>(self.cell).map($conversion))
            }
        }
    };
}

impl_produce_ty!(BooleanType, bool);
impl_produce_ty!(Int16Type, i16);
impl_produce_ty!(Int32Type, i32);
impl_produce_ty!(Int64Type, i64);
impl_produce_ty!(UInt8Type, u8);
impl_produce_ty!(Float32Type, f32);
impl_produce_ty!(Float64Type, f64);
impl_produce_ty!(Utf8Type, StrOrNum, StrOrNum::into_inner);
impl_produce_ty!(LargeUtf8Type, &str, &str::to_owned);

impl_produce_unsupported!(
    MssqlCellRef<'r>,
    (
        NullType,
        Int8Type,
        UInt16Type,
        Float16Type,
        UInt32Type,
        UInt64Type,
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
        LargeBinaryType,
        FixedSizeBinaryType,
        Decimal128Type,
        Decimal256Type,
        BinaryType,
    )
);

struct StrOrNum(String);

impl StrOrNum {
    fn into_inner(self) -> String {
        self.0
    }
}

impl<'a> mssql::FromSql<'a> for StrOrNum {
    fn from_sql(value: &'a ColumnData<'static>) -> mssql::Result<Option<Self>> {
        match value {
            ColumnData::String(s) => Ok(s.as_ref().map(|x| StrOrNum(x.to_string()))),
            ColumnData::Numeric(n) => Ok(n.as_ref().map(|x| {
                if x.scale() > 0 {
                    let sign = if x.value() < 0 { "-" } else { "" };
                    StrOrNum(format!(
                        "{sign}{}.{:0pad$}",
                        x.int_part().abs(),
                        x.dec_part().abs(),
                        pad = x.scale() as usize
                    ))
                } else {
                    StrOrNum(format!("{}", x.value()))
                }
            })),
            _ => Err(mssql::error::Error::Conversion(
                format!("cannot convert `{value:?}` into string").into(),
            )),
        }
    }
}

struct Value<'a>(&'a ColumnData<'a>);

impl ToSql for Value<'_> {
    fn to_sql(&self) -> ColumnData<'_> {
        self.0.clone()
    }
}
