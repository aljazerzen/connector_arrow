use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

use postgres::fallible_iterator::FallibleIterator;
use postgres::types::{FromSql, Type};
use postgres::{Client, Row, RowIter};

use crate::api::{ResultReader, Statement};
use crate::types::{ArrowType, FixedSizeBinaryType};
use crate::util::CellReader;
use crate::util::{transport, ArrayCellRef};
use crate::{errors::ConnectorError, util::RowsReader};

use super::{types, PostgresError};

pub struct PostgresStatement<'conn> {
    pub(super) client: &'conn mut Client,
    pub(super) stmt: postgres::Statement,
}

impl<'conn> Statement<'conn> for PostgresStatement<'conn> {
    type Reader<'stmt>
        = PostgresBatchStream<'stmt>
    where
        Self: 'stmt;

    fn start_batch<'p>(
        &mut self,
        args: (&RecordBatch, usize),
    ) -> Result<Self::Reader<'_>, ConnectorError> {
        let stmt = &self.stmt;
        let schema = types::pg_stmt_to_arrow(stmt)?;

        let arg_row = ArrayCellRef::vec_from_batch(args.0, args.1);

        // query
        let rows = self
            .client
            .query_raw::<_, _, _>(stmt, &arg_row)
            .map_err(PostgresError::from)?;

        // create the row reader
        let row_reader = PostgresRowStream::new(rows);
        Ok(PostgresBatchStream { schema, row_reader })
    }
}

pub struct PostgresBatchStream<'a> {
    schema: SchemaRef,
    row_reader: PostgresRowStream<'a>,
}

impl<'a> ResultReader<'a> for PostgresBatchStream<'a> {
    fn get_schema(&mut self) -> Result<std::sync::Arc<arrow::datatypes::Schema>, ConnectorError> {
        Ok(self.schema.clone())
    }
}

impl Iterator for PostgresBatchStream<'_> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        crate::util::next_batch_from_rows(&self.schema, &mut self.row_reader, 1024).transpose()
    }
}

struct PostgresRowStream<'a> {
    iter: postgres_fallible_iterator::Fuse<postgres::RowIter<'a>>,
}

impl<'a> PostgresRowStream<'a> {
    pub fn new(iter: RowIter<'a>) -> Self {
        Self { iter: iter.fuse() }
    }
}

impl<'stmt> RowsReader<'stmt> for PostgresRowStream<'stmt> {
    type CellReader<'row>
        = PostgresCellReader
    where
        Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
        let row = self.iter.next().map_err(PostgresError::from)?;

        Ok(row.map(|row| PostgresCellReader { row, next_col: 0 }))
    }
}

struct PostgresCellReader {
    row: Row,
    next_col: usize,
}

impl CellReader<'_> for PostgresCellReader {
    type CellRef<'cell>
        = CellRef<'cell>
    where
        Self: 'cell;

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
impl_produce!(BinaryType, Binary, Binary::into_arrow);
impl_produce!(LargeBinaryType, Binary, Binary::into_arrow);
impl_produce!(Utf8Type, StrOrNum, StrOrNum::into_arrow);
impl_produce!(LargeUtf8Type, String, Result::Ok);
impl_produce!(
    TimestampMicrosecondType,
    TimestampY2000,
    TimestampY2000::into_microsecond
);
impl_produce!(Time64MicrosecondType, Time64, Time64::into_microsecond);
impl_produce!(Date32Type, DaysSinceY2000, DaysSinceY2000::into_date32);
impl_produce!(
    IntervalMonthDayNanoType,
    IntervalMonthDayMicros,
    IntervalMonthDayMicros::into_arrow
);

crate::impl_produce_unsupported!(
    CellRef<'r>,
    (
        UInt8Type,
        UInt16Type,
        UInt32Type,
        UInt64Type,
        Float16Type,
        TimestampSecondType,
        TimestampMillisecondType,
        TimestampNanosecondType,
        Date64Type,
        Time32SecondType,
        Time32MillisecondType,
        Time64NanosecondType,
        IntervalYearMonthType,
        IntervalDayTimeType,
        DurationSecondType,
        DurationMillisecondType,
        DurationMicrosecondType,
        DurationNanosecondType,
        FixedSizeBinaryType,
        Decimal128Type,
        Decimal256Type,
    )
);

struct StrOrNum(String);

impl StrOrNum {
    fn into_arrow(self) -> Result<String, ConnectorError> {
        Ok(self.0)
    }
}

impl<'a> FromSql<'a> for StrOrNum {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if matches!(ty, &Type::NUMERIC) {
            Ok(super::decimal::from_sql(raw).map(StrOrNum)?)
        } else {
            let slice = postgres_protocol::types::text_from_sql(raw)?;
            Ok(StrOrNum(slice.to_string()))
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

const DUR_1970_TO_2000_DAYS: i32 = 10957;
const DUR_1970_TO_2000_SEC: i64 = DUR_1970_TO_2000_DAYS as i64 * 24 * 60 * 60;

struct TimestampY2000(i64);

impl<'a> FromSql<'a> for TimestampY2000 {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        postgres_protocol::types::timestamp_from_sql(raw).map(TimestampY2000)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl TimestampY2000 {
    fn into_microsecond(self) -> Result<i64, ConnectorError> {
        self.0
            .checked_add(DUR_1970_TO_2000_SEC * 1000 * 1000)
            .ok_or(ConnectorError::DataOutOfRange)
    }
}

struct DaysSinceY2000(i32);

impl<'a> FromSql<'a> for DaysSinceY2000 {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        postgres_protocol::types::date_from_sql(raw).map(DaysSinceY2000)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl DaysSinceY2000 {
    fn into_date32(self) -> Result<i32, ConnectorError> {
        self.0
            .checked_add(DUR_1970_TO_2000_DAYS)
            .ok_or(ConnectorError::DataOutOfRange)
    }
}

struct Time64(i64);

impl<'a> FromSql<'a> for Time64 {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        postgres_protocol::types::time_from_sql(raw).map(Time64)
    }
    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl Time64 {
    fn into_microsecond(self) -> Result<i64, ConnectorError> {
        Ok(self.0)
    }
}

struct IntervalMonthDayMicros {
    months: i32,
    days: i32,
    micros: i64,
}

impl<'a> FromSql<'a> for IntervalMonthDayMicros {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let micros = postgres_protocol::types::time_from_sql(&raw[0..8])?;
        let days = postgres_protocol::types::int4_from_sql(&raw[8..12])?;
        let months = postgres_protocol::types::int4_from_sql(&raw[12..16])?;
        Ok(IntervalMonthDayMicros {
            months,
            days,
            micros,
        })
    }
    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl IntervalMonthDayMicros {
    fn into_arrow(self) -> Result<IntervalMonthDayNano, ConnectorError> {
        let nanoseconds = (self.micros.checked_mul(1000)).ok_or(ConnectorError::DataOutOfRange)?;
        Ok(IntervalMonthDayNano {
            months: self.months,
            days: self.days,
            nanoseconds,
        })
    }
}

struct Binary<'a>(&'a [u8]);

impl<'a> FromSql<'a> for Binary<'a> {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(if matches!(ty, &Type::VARBIT | &Type::BIT) {
            let varbit = postgres_protocol::types::varbit_from_sql(raw)?;
            Binary(varbit.bytes())
        } else {
            Binary(postgres_protocol::types::bytea_from_sql(raw))
        })
    }
    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl Binary<'_> {
    fn into_arrow(self) -> Result<Vec<u8>, ConnectorError> {
        // this is a clone, that is needed because Produce requires Vec<u8>
        Ok(self.0.to_vec())
    }
}
