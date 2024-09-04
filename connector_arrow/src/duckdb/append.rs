use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use duckdb::{types::Value, Appender};
use itertools::zip_eq;
use itertools::Itertools;

use crate::impl_consume_unsupported;
use crate::types::{ArrowType, FixedSizeBinaryType, NullType};
use crate::util::transport::{self, Consume, ConsumeTy};
use crate::util::ArrayCellRef;
use crate::{api::Append, ConnectorError};

pub struct DuckDBAppender<'conn> {
    pub(super) inner: Appender<'conn>,
}

impl<'conn> Append<'conn> for DuckDBAppender<'conn> {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        let schema = batch.schema();
        let mut cell_refs = zip_eq(batch.columns(), schema.fields())
            .map(|(array, field)| ArrayCellRef {
                array,
                field,
                row_number: 0,
            })
            .collect_vec();

        for row_number in 0..batch.num_rows() {
            let mut row: Vec<Value> = Vec::new();

            for cell_ref in &mut cell_refs {
                cell_ref.row_number = row_number;
                transport::transport(cell_ref.field, cell_ref as &_, &mut row).unwrap();
            }
            let row = duckdb::appender_params_from_iter(row);
            self.inner.append_row(row)?;
        }

        Ok(())
    }

    fn finish(self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

impl Consume for Vec<Value> {}

macro_rules! impl_consume_ty {
    ($ArrTy: ty, $value_kind: expr) => {
        impl_consume_ty!($ArrTy, $value_kind, std::convert::identity);
    };

    ($ArrTy: ty, $value_kind: expr, $conversion: expr) => {
        impl ConsumeTy<$ArrTy> for Vec<Value> {
            fn consume(&mut self, _ty: &DataType, value: <$ArrTy as ArrowType>::Native) {
                self.push($value_kind(($conversion)(value)));
            }

            fn consume_null(&mut self, _ty: &DataType) {
                self.push(Value::Null);
            }
        }
    };
}

impl ConsumeTy<NullType> for Vec<Value> {
    fn consume(&mut self, _ty: &DataType, _value: ()) {
        self.push(Value::Null);
    }

    fn consume_null(&mut self, _ty: &DataType) {
        self.push(Value::Null);
    }
}

impl ConsumeTy<TimestampMicrosecondType> for Vec<Value> {
    fn consume(&mut self, _ty: &DataType, value: i64) {
        self.push(Value::Timestamp(
            duckdb::types::TimeUnit::Microsecond,
            value,
        ));
    }

    fn consume_null(&mut self, _ty: &DataType) {
        self.push(Value::Null);
    }
}

impl_consume_ty!(BooleanType, Value::Boolean);
impl_consume_ty!(Int8Type, Value::TinyInt);
impl_consume_ty!(Int16Type, Value::SmallInt);
impl_consume_ty!(Int32Type, Value::Int);
impl_consume_ty!(Int64Type, Value::BigInt);
impl_consume_ty!(UInt8Type, Value::UTinyInt);
impl_consume_ty!(UInt16Type, Value::USmallInt);
impl_consume_ty!(UInt32Type, Value::UInt);
impl_consume_ty!(UInt64Type, Value::UBigInt);
impl_consume_ty!(Float16Type, Value::Float, f32::from);
impl_consume_ty!(Float32Type, Value::Float);
impl_consume_ty!(Float64Type, Value::Double);

impl_consume_ty!(TimestampSecondType, Value::BigInt);
impl_consume_ty!(TimestampMillisecondType, Value::BigInt);
impl_consume_ty!(TimestampNanosecondType, Value::BigInt);

impl_consume_ty!(BinaryType, Value::Blob);
impl_consume_ty!(LargeBinaryType, Value::Blob);
impl_consume_ty!(FixedSizeBinaryType, Value::Blob);
impl_consume_ty!(Utf8Type, Value::Text);
impl_consume_ty!(LargeUtf8Type, Value::Text);

impl_consume_unsupported!(
    Vec<Value>,
    (
        Date32Type,
        Date64Type,
        Time32SecondType,
        Time32MillisecondType,
        Time64MicrosecondType,
        Time64NanosecondType,
        DurationSecondType,
        DurationMillisecondType,
        DurationMicrosecondType,
        DurationNanosecondType,
        IntervalYearMonthType,
        IntervalDayTimeType,
        IntervalMonthDayNanoType,
        Decimal128Type,
        Decimal256Type,
    )
);
