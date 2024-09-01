use arrow::{datatypes::*, record_batch::RecordBatch};
use futures::{AsyncRead, AsyncWrite, StreamExt};
use std::sync::Arc;
use tiberius::numeric::Numeric;
use tiberius::QueryStream;
use tokio::runtime::Runtime;

use crate::api::{ResultReader, Statement};
use crate::impl_produce_unsupported;
use crate::types::{ArrowType, FixedSizeBinaryType, NullType};
use crate::util::transport::ProduceTy;
use crate::util::{self, transport::Produce};
use crate::ConnectorError;

pub struct TiberiusStatement<'conn, S: AsyncRead + AsyncWrite + Unpin + Send> {
    pub(super) conn: &'conn mut super::TiberiusConnection<S>,
    pub(super) query: String,
}

impl<'conn, S: AsyncRead + AsyncWrite + Unpin + Send> Statement<'conn>
    for TiberiusStatement<'conn, S>
{
    type Reader<'stmt> = TiberiusResultReader<'stmt>
    where
        Self: 'stmt;

    fn start<'p, I>(&mut self, _params: I) -> Result<Self::Reader<'_>, ConnectorError>
    where
        I: IntoIterator<Item = &'p dyn crate::api::ArrowValue>,
    {
        // TODO: params

        let mut stream = self
            .conn
            .rt
            .block_on(self.conn.client.query(&self.query, &[]))?;

        // get columns
        let columns = self.conn.rt.block_on(stream.columns())?;
        let schema = super::types::get_result_schema(columns)?;
        self.conn.rt.block_on(stream.next());

        Ok(TiberiusResultReader {
            schema,
            stream: TiberiusStream {
                rt: self.conn.rt.clone(),
                stream,
            },
        })
    }
}

pub struct TiberiusResultReader<'stmt> {
    schema: SchemaRef,
    stream: TiberiusStream<'stmt>,
}

struct TiberiusStream<'stmt> {
    rt: Arc<Runtime>,
    stream: QueryStream<'stmt>,
}

impl<'stmt> ResultReader<'stmt> for TiberiusResultReader<'stmt> {
    fn get_schema(&mut self) -> Result<arrow::datatypes::SchemaRef, ConnectorError> {
        Ok(self.schema.clone())
    }
}

impl<'stmt> Iterator for TiberiusResultReader<'stmt> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        util::next_batch_from_rows(&self.schema, &mut self.stream, 1024).transpose()
    }
}

impl<'s> util::RowsReader<'s> for TiberiusStream<'s> {
    type CellReader<'row> = TiberiusCellReader
    where
        Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
        let item = self.rt.block_on(self.stream.next());

        // are we done?
        let Some(item) = item else { return Ok(None) };

        // are there more result sets?
        let row = match item? {
            tiberius::QueryItem::Row(row) => row,
            tiberius::QueryItem::Metadata(_) => {
                // yes, this there are
                return Err(ConnectorError::MultipleResultSets);
            }
        };

        Ok(Some(TiberiusCellReader { row, cell: 0 }))
    }
}

struct TiberiusCellReader {
    row: tiberius::Row,
    cell: usize,
}

impl<'a> util::CellReader<'a> for TiberiusCellReader {
    type CellRef<'cell> = TiberiusCellRef<'cell>
    where
        Self: 'cell;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        let r = TiberiusCellRef {
            row: &mut self.row,
            cell: self.cell,
        };
        self.cell += 1;
        Some(r)
    }
}

#[derive(Debug)]
struct TiberiusCellRef<'a> {
    row: &'a mut tiberius::Row,
    cell: usize,
}

impl<'r> Produce<'r> for TiberiusCellRef<'r> {}

macro_rules! impl_produce_ty {
    ($ArrTy: ty, $DbTy: ty) => {
        impl_produce_ty!($ArrTy, $DbTy, std::convert::identity);
    };

    ($ArrTy: ty, $DbTy: ty, $conversion: expr) => {
        impl<'r> ProduceTy<'r, $ArrTy> for TiberiusCellRef<'r> {
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
impl_produce_ty!(Decimal128Type, Numeric, numeric_to_i128);
impl_produce_ty!(Utf8Type, &str, &str::to_owned);
impl_produce_ty!(LargeUtf8Type, &str, &str::to_owned);

impl_produce_unsupported!(
    TiberiusCellRef<'r>,
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
        Decimal256Type,
        BinaryType,
    )
);

fn numeric_to_i128(val: Numeric) -> i128 {
    val.value()
}
