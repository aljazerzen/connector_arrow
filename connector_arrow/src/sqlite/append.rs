use std::sync::Arc;

use arrow::array::*;
use arrow::{datatypes::DataType, record_batch::RecordBatch};
use itertools::Itertools;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Transaction};

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
    for row in rows_range {
        for col_array in batch.columns() {
            res.push(get_value(col_array, row).to_owned());
        }
    }
    res
}

fn get_value(array: &Arc<dyn Array>, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }

    match array.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => Value::Integer(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int8 => Value::Integer(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int16 => Value::Integer(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int32 => Value::Integer(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int64 => Value::Integer(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt8 => Value::Integer(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::UInt16 => Value::Integer(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::UInt32 => Value::Integer(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::UInt64 => unimplemented!(),
        DataType::Float16 => Value::Real(
            array
                .as_any()
                .downcast_ref::<Float16Array>()
                .unwrap()
                .value(row)
                .to_f64(),
        ),
        DataType::Float32 => Value::Real(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row) as f64,
        ),
        DataType::Float64 => Value::Real(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Timestamp(_, _) => unimplemented!(),
        DataType::Date32 => unimplemented!(),
        DataType::Date64 => unimplemented!(),
        DataType::Time32(_) => unimplemented!(),
        DataType::Time64(_) => unimplemented!(),
        DataType::Duration(_) => unimplemented!(),
        DataType::Interval(_) => unimplemented!(),
        DataType::Binary => Value::Blob(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row)
                .to_vec(),
        ),
        DataType::FixedSizeBinary(_) => Value::Blob(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(row)
                .to_vec(),
        ),
        DataType::LargeBinary => Value::Blob(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(row)
                .to_vec(),
        ),
        DataType::Utf8 => Value::Text(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::LargeUtf8 => Value::Text(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::List(_) => unimplemented!(),
        DataType::FixedSizeList(_, _) => unimplemented!(),
        DataType::LargeList(_) => unimplemented!(),
        DataType::Struct(_) => unimplemented!(),
        DataType::Union(_, _) => unimplemented!(),
        DataType::Dictionary(_, _) => unimplemented!(),
        DataType::Decimal128(_, _) => unimplemented!(),
        DataType::Decimal256(_, _) => unimplemented!(),
        DataType::Map(_, _) => unimplemented!(),
        DataType::RunEndEncoded(_, _) => unimplemented!(),
    }
}
