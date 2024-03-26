use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use itertools::{zip_eq, Itertools};
use mysql::prelude::Queryable;
use mysql::Value;

use crate::api::Append;
use crate::types::{FixedSizeBinaryType, NullType};
use crate::util::escape::escaped_ident_bt;
use crate::util::transport::{self, Consume, ConsumeTy};
use crate::util::ArrayCellRef;
use crate::{impl_consume_unsupported, ConnectorError};

pub struct MySQLAppender<'conn, C: Queryable> {
    table: String,
    client: &'conn mut C,
}

impl<'conn, C: Queryable> MySQLAppender<'conn, C> {
    pub fn new(client: &'conn mut C, table_name: &str) -> Result<Self, ConnectorError> {
        client.query_drop("START TRANSACTION;")?;
        Ok(Self {
            table: table_name.to_owned(),
            client,
        })
    }
}

impl<'conn, C: Queryable> Append<'conn> for MySQLAppender<'conn, C> {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        // TODO: 30 is a guess, we need benchmarking to find the optimum value
        const BATCH_SIZE: usize = 30;

        let last_batch_size = batch.num_rows() % BATCH_SIZE;

        let batch_query = insert_query(&self.table, batch.num_columns(), BATCH_SIZE);
        for batch_number in 0..(batch.num_rows() / BATCH_SIZE) {
            let rows_range = (batch_number * BATCH_SIZE)..((batch_number + 1) * BATCH_SIZE);

            let params: Vec<Value> = collect_args(&batch, rows_range);
            self.client.exec_iter(&batch_query, params)?;
        }

        if last_batch_size > 0 {
            let rows_range = (batch.num_rows() - last_batch_size)..batch.num_rows();

            let last_query = insert_query(&self.table, batch.num_columns(), last_batch_size);
            let params: Vec<Value> = collect_args(&batch, rows_range);
            self.client.exec_iter(&last_query, params)?;
        }

        Ok(())
    }

    fn finish(self) -> Result<(), ConnectorError> {
        self.client.query_drop("COMMIT;")?;
        Ok(())
    }
}

fn insert_query(table_name: &str, cols: usize, rows: usize) -> String {
    let values = (0..rows)
        .map(|_| {
            let row = (0..cols).map(|_| "?").join(",");
            format!("({row})")
        })
        .join(",");

    format!(
        "INSERT INTO {} VALUES {values}",
        escaped_ident_bt(table_name)
    )
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
                self.push(Value::NULL);
            }
        }
    };
}

impl_consume_ty!(BooleanType, Value::Int, i64::from);
impl_consume_ty!(Int8Type, Value::Int, i64::from);
impl_consume_ty!(Int16Type, Value::Int, i64::from);
impl_consume_ty!(Int32Type, Value::Int, i64::from);
impl_consume_ty!(Int64Type, Value::Int);
impl_consume_ty!(UInt8Type, Value::UInt, u64::from);
impl_consume_ty!(UInt16Type, Value::UInt, u64::from);
impl_consume_ty!(UInt32Type, Value::UInt, u64::from);
impl_consume_ty!(UInt64Type, Value::UInt);
impl_consume_ty!(Float16Type, Value::Float, f32::from);
impl_consume_ty!(Float32Type, Value::Float);
impl_consume_ty!(Float64Type, Value::Double);
impl_consume_ty!(Utf8Type, Value::Bytes, String::into_bytes);
impl_consume_ty!(BinaryType, Value::Bytes);
impl_consume_ty!(LargeBinaryType, Value::Bytes);
impl_consume_ty!(FixedSizeBinaryType, Value::Bytes);

impl ConsumeTy<NullType> for Vec<Value> {
    fn consume(&mut self, _ty: &DataType, _value: ()) {
        self.push(Value::NULL);
    }

    fn consume_null(&mut self) {
        self.push(Value::NULL);
    }
}

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
        DurationSecondType,
        DurationMillisecondType,
        DurationMicrosecondType,
        DurationNanosecondType,
        IntervalDayTimeType,
        IntervalMonthDayNanoType,
        IntervalYearMonthType,
        Decimal128Type,
        Decimal256Type,
        LargeUtf8Type,
    )
);
