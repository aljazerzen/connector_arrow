use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::FromSql;
use postgres::{Row, RowIter};

use crate::api::{ArrowValue, ResultReader, Statement};
use crate::types::{ArrowType, FixedSizeBinaryType};
use crate::util::transport;
use crate::util::{ArrowRowWriter, CellReader};
use crate::{errors::ConnectorError, util::RowsReader};

use super::{types, PostgresError, PostgresStatement, ProtocolExtended};

impl<'conn> Statement<'conn> for PostgresStatement<'conn, ProtocolExtended> {
    type Reader<'stmt> = PostgresBatchStream<'stmt> where Self: 'stmt;

    fn start(&mut self, _params: &[&dyn ArrowValue]) -> Result<Self::Reader<'_>, ConnectorError> {
        let stmt = &self.stmt;
        let schema = types::pg_stmt_to_arrow(stmt)?;

        let rows = self
            .client
            .query_raw::<_, bool, _>(&self.query, vec![])
            .map_err(PostgresError::from)?;

        let row_reader = PostgresRowStream::new(rows);
        Ok(PostgresBatchStream {
            schema,
            row_reader,
            is_finished: false,
        })
    }
}

pub struct PostgresBatchStream<'a> {
    schema: SchemaRef,
    row_reader: PostgresRowStream<'a>,
    is_finished: bool,
}

impl<'a> ResultReader<'a> for PostgresBatchStream<'a> {
    fn get_schema(&mut self) -> Result<std::sync::Arc<arrow::datatypes::Schema>, ConnectorError> {
        Ok(self.schema.clone())
    }
}

impl<'a> PostgresBatchStream<'a> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.is_finished {
            return Ok(None);
        }

        let batch_size = 1024;

        let mut writer = ArrowRowWriter::new(self.schema.clone(), batch_size);

        for _ in 0..batch_size {
            if let Some(mut cell_reader) = self.row_reader.next_row()? {
                writer.prepare_for_batch(1)?;

                for field in &self.schema.fields {
                    let cell_ref = cell_reader.next_cell();

                    transport::transport(field, cell_ref.unwrap(), &mut writer)?;
                }
            } else {
                self.is_finished = true;
                break;
            }
        }

        let batches = writer.finish()?;
        if batches.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batches.into_iter().exactly_one().unwrap()))
        }
    }
}

impl<'a> Iterator for PostgresBatchStream<'a> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

struct PostgresRowStream<'a> {
    iter: postgres::RowIter<'a>,
}

impl<'a> PostgresRowStream<'a> {
    pub fn new(iter: RowIter<'a>) -> Self {
        Self { iter }
    }
}

impl<'stmt> RowsReader<'stmt> for PostgresRowStream<'stmt> {
    type CellReader<'row> = PostgresCellReader where Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
        let row = self.iter.next().map_err(PostgresError::from)?;

        Ok(row.map(|row| PostgresCellReader { row, next_col: 0 }))
    }
}

struct PostgresCellReader {
    row: Row,
    next_col: usize,
}

impl<'row> CellReader<'row> for PostgresCellReader {
    type CellRef<'cell> = CellRef<'cell> where Self: 'cell;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        if self.next_col >= self.row.columns().len() {
            return None;
        }
        let col = self.next_col;
        self.next_col += 1;
        Some((&self.row, col))
    }
}

type CellRef<'a> = (&'a Row, usize);

impl<'c> transport::Produce<'c> for CellRef<'c> {}

macro_rules! impl_produce {
    ($t: ty, $native: ty, $conversion_fn: expr) => {
        impl<'c> transport::ProduceTy<'c, $t> for CellRef<'c> {
            fn produce(self) -> Result<<$t as ArrowType>::Native, ConnectorError> {
                let value = self.0.get::<_, $native>(self.1);
                $conversion_fn(value)
            }

            fn produce_opt(self) -> Result<Option<<$t as ArrowType>::Native>, ConnectorError> {
                let value = self.0.get::<_, Option<$native>>(self.1);
                value.map($conversion_fn).transpose()
            }
        }
    };
}

impl_produce!(BooleanType, bool, Result::Ok);
impl_produce!(Int8Type, i8, Result::Ok);
impl_produce!(Int16Type, i16, Result::Ok);
impl_produce!(Int32Type, i32, Result::Ok);
impl_produce!(Int64Type, i64, Result::Ok);
impl_produce!(Float32Type, f32, Result::Ok);
impl_produce!(Float64Type, f64, Result::Ok);
impl_produce!(LargeBinaryType, Vec<u8>, Result::Ok);
impl_produce!(LargeUtf8Type, String, Result::Ok);
impl_produce!(
    TimestampSecondType,
    TimestampY2000,
    TimestampY2000::into_second
);
impl_produce!(
    TimestampMillisecondType,
    TimestampY2000,
    TimestampY2000::into_millisecond
);
impl_produce!(
    TimestampMicrosecondType,
    TimestampY2000,
    TimestampY2000::into_microsecond
);
impl_produce!(
    TimestampNanosecondType,
    TimestampY2000,
    TimestampY2000::into_nanosecond
);

crate::impl_produce_unsupported!(
    CellRef<'r>,
    (
        UInt8Type,
        UInt16Type,
        UInt32Type,
        UInt64Type,
        Float16Type,
        // TimestampSecondType,
        // TimestampMillisecondType,
        // TimestampMicrosecondType,
        // TimestampNanosecondType,
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
        BinaryType,
        FixedSizeBinaryType,
        Decimal128Type,
        Decimal256Type,
    )
);

struct Numeric(String);

impl<'a> FromSql<'a> for Numeric {
    fn from_sql(
        _ty: &postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(super::decimal::from_sql(raw).map(Numeric)?)
    }

    fn accepts(_ty: &postgres::types::Type) -> bool {
        true
    }
}

impl<'c> transport::ProduceTy<'c, Utf8Type> for CellRef<'c> {
    fn produce(self) -> Result<String, ConnectorError> {
        Ok(self.0.get::<_, Numeric>(self.1).0)
    }

    fn produce_opt(self) -> Result<Option<String>, ConnectorError> {
        Ok(self.0.get::<_, Option<Numeric>>(self.1).map(|n| n.0))
    }
}

struct TimestampY2000(i64);

const DUR_1970_TO_2000_SEC: i64 = 10957 * 24 * 60 * 60;

impl TimestampY2000 {
    fn into_nanosecond(self) -> Result<i64, ConnectorError> {
        self.0
            .checked_add(DUR_1970_TO_2000_SEC * 1000 * 1000)
            .and_then(|micros_y1970| micros_y1970.mul_checked(1000).ok())
            .ok_or(ConnectorError::DataOutOfRange)
    }
    fn into_microsecond(self) -> Result<i64, ConnectorError> {
        self.0
            .checked_add(DUR_1970_TO_2000_SEC * 1000 * 1000)
            .ok_or(ConnectorError::DataOutOfRange)
    }
    fn into_millisecond(self) -> Result<i64, ConnectorError> {
        self.0
            .div_checked(1000)
            .ok()
            .and_then(|millis_y2000| millis_y2000.checked_add(DUR_1970_TO_2000_SEC * 1000))
            .ok_or(ConnectorError::DataOutOfRange)
    }
    fn into_second(self) -> Result<i64, ConnectorError> {
        self.0
            .div_checked(1_000_000)
            .ok()
            .and_then(|sec_y2000| sec_y2000.checked_add(DUR_1970_TO_2000_SEC))
            .ok_or(ConnectorError::DataOutOfRange)
    }
}

impl<'a> FromSql<'a> for TimestampY2000 {
    fn from_sql(
        _ty: &postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        postgres_protocol::types::timestamp_from_sql(raw).map(TimestampY2000)
    }

    fn accepts(_ty: &postgres::types::Type) -> bool {
        true
    }
}
