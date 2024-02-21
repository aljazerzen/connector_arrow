use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use itertools::zip_eq;
use itertools::Itertools;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Transaction};

use crate::impl_consume_unsupported;
use crate::types::{FixedSizeBinaryType, NullType};
use crate::util::transport;
use crate::util::transport::{Consume, ConsumeTy};
use crate::util::ArrayCellRef;
use crate::{api::Append, ConnectorError};

pub struct SQLiteAppender<'conn> {
    table: String,
    transaction: Transaction<'conn>,
}

impl<'conn> SQLiteAppender<'conn> {
    pub fn new(table: String, transaction: Transaction<'conn>) -> Result<Self, ConnectorError> {
        Ok(Self { table, transaction })
    }
}

impl<'conn> Append<'conn> for SQLiteAppender<'conn> {
    fn append(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<(), crate::ConnectorError> {
        // TODO: 30 is a guess, we need benchmarking to find the optimum value
        const BATCH_SIZE: usize = 30;

        let last_batch_size = batch.num_rows() % BATCH_SIZE;

        let batch_query = insert_query(&self.table, batch.num_columns(), BATCH_SIZE);
        for batch_number in 0..(batch.num_rows() / BATCH_SIZE) {
            let rows_range = (batch_number * BATCH_SIZE)..((batch_number + 1) * BATCH_SIZE);

            let params: Vec<Value> = collect_args(&batch, rows_range);
            self.transaction
                .execute(&batch_query, params_from_iter(params))?;
        }

        if last_batch_size > 0 {
            let rows_range = (batch.num_rows() - last_batch_size)..batch.num_rows();

            let last_query = insert_query(&self.table, batch.num_columns(), last_batch_size);
            let params: Vec<Value> = collect_args(&batch, rows_range);
            self.transaction
                .execute(&last_query, params_from_iter(params))?;
        }

        Ok(())
    }

    fn finish(self) -> Result<(), crate::ConnectorError> {
        Ok(self.transaction.commit()?)
    }
}

fn insert_query(table_name: &str, cols: usize, rows: usize) -> String {
    let values = (0..rows)
        .map(|_| {
            let row = (0..cols).map(|_| "?").join(",");
            format!("({row})")
        })
        .join(",");

    format!("INSERT INTO \"{table_name}\" VALUES {values}")
}

fn collect_args(batch: &RecordBatch, rows_range: std::ops::Range<usize>) -> Vec<Value> {
    let mut res = Vec::with_capacity(rows_range.len() * batch.num_columns());

    let schema = batch.schema();
    let mut row = zip_eq(batch.columns(), schema.fields())
        .map(|(array, field)| ArrayCellRef {
            array,
            field,
            row_number: 0,
        })
        .collect_vec();

    for row_number in rows_range {
        for cell in &mut row {
            cell.row_number = row_number;
            transport::transport(cell.field, cell as &_, &mut res).unwrap();
        }
    }
    res
}

impl Consume for Vec<Value> {}

macro_rules! impl_consume_ty {
    ($ArrTy: ty, $value_kind: expr) => {
        impl_consume_ty!($ArrTy, $value_kind, std::convert::identity);
    };

    ($ArrTy: ty, $value_kind: expr, $conversion: expr) => {
        impl ConsumeTy<$ArrTy> for Vec<Value> {
            fn consume(
                &mut self,
                _ty: &DataType,
                value: <$ArrTy as crate::types::ArrowType>::Native,
            ) {
                let value: Value = $value_kind(($conversion)(value));
                self.push(value);
            }

            fn consume_null(&mut self) {
                self.push(Value::Null);
            }
        }
    };
}

impl ConsumeTy<NullType> for Vec<Value> {
    fn consume(&mut self, _ty: &DataType, _value: ()) {
        self.push(Value::Null);
    }

    fn consume_null(&mut self) {
        self.push(Value::Null);
    }
}

impl ConsumeTy<Decimal128Type> for Vec<Value> {
    fn consume(&mut self, ty: &DataType, value: i128) {
        self.push(Value::Text(crate::util::decimal::decimal128_to_string(
            ty, value,
        )));
    }

    fn consume_null(&mut self) {
        self.push(Value::Null);
    }
}

impl ConsumeTy<Decimal256Type> for Vec<Value> {
    fn consume(&mut self, ty: &DataType, value: i256) {
        self.push(Value::Text(crate::util::decimal::decimal256_to_string(
            ty, value,
        )));
    }

    fn consume_null(&mut self) {
        self.push(Value::Null);
    }
}

impl_consume_ty!(BooleanType, Value::Integer, i64::from);
impl_consume_ty!(Int8Type, Value::Integer, i64::from);
impl_consume_ty!(Int16Type, Value::Integer, i64::from);
impl_consume_ty!(Int32Type, Value::Integer, i64::from);
impl_consume_ty!(Int64Type, Value::Integer);
impl_consume_ty!(UInt8Type, Value::Integer, i64::from);
impl_consume_ty!(UInt16Type, Value::Integer, i64::from);
impl_consume_ty!(UInt32Type, Value::Integer, i64::from);
impl_consume_ty!(UInt64Type, Value::Text, u64_to_string);
impl_consume_ty!(Float16Type, Value::Real, f64::from);
impl_consume_ty!(Float32Type, Value::Real, f64::from);
impl_consume_ty!(Float64Type, Value::Real);
impl_consume_ty!(BinaryType, Value::Blob);
impl_consume_ty!(LargeBinaryType, Value::Blob);
impl_consume_ty!(FixedSizeBinaryType, Value::Blob);
impl_consume_ty!(Utf8Type, Value::Text);
impl_consume_ty!(LargeUtf8Type, Value::Text);

impl_consume_unsupported!(
    Vec<Value>,
    (
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
    )
);

fn u64_to_string(u: u64) -> String {
    u64::to_string(&u)
}
