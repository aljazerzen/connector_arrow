use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use duckdb::{types::Value, Appender};

use crate::{api::Append, ConnectorError};

impl<'conn> Append<'conn> for Appender<'conn> {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        for row_index in 0..batch.num_rows() {
            let row = convert_row(&batch, row_index);
            self.append_row(duckdb::appender_params_from_iter(row))?;
        }

        Ok(())
    }

    fn finish(self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn convert_row(batch: &RecordBatch, row_index: usize) -> Vec<Value> {
    let mut res = Vec::with_capacity(batch.num_columns());
    for col_array in batch.columns() {
        res.push(convert_value(col_array, row_index));
    }
    res
}

fn convert_value(arr: &dyn Array, i: usize) -> Value {
    if arr.is_null(i) {
        return Value::Null;
    }

    match arr.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => Value::Boolean(arr.as_boolean().value(i)),
        DataType::Int8 => Value::TinyInt(arr.as_primitive::<Int8Type>().value(i)),
        DataType::Int16 => Value::SmallInt(arr.as_primitive::<Int16Type>().value(i)),
        DataType::Int32 => Value::Int(arr.as_primitive::<Int32Type>().value(i)),
        DataType::Int64 => Value::BigInt(arr.as_primitive::<Int64Type>().value(i)),
        DataType::UInt8 => Value::UTinyInt(arr.as_primitive::<UInt8Type>().value(i)),
        DataType::UInt16 => Value::USmallInt(arr.as_primitive::<UInt16Type>().value(i)),
        DataType::UInt32 => Value::UInt(arr.as_primitive::<UInt32Type>().value(i)),
        DataType::UInt64 => Value::UBigInt(arr.as_primitive::<UInt64Type>().value(i)),
        DataType::Float16 => Value::Float(arr.as_primitive::<Float16Type>().value(i).to_f32()),
        DataType::Float32 => Value::Float(arr.as_primitive::<Float32Type>().value(i)),
        DataType::Float64 => Value::Double(arr.as_primitive::<Float64Type>().value(i)),
        DataType::Timestamp(_, _) => unimplemented!(),
        DataType::Date32 => unimplemented!(),
        DataType::Date64 => unimplemented!(),
        DataType::Time32(_) => unimplemented!(),
        DataType::Time64(_) => unimplemented!(),
        DataType::Duration(_) => unimplemented!(),
        DataType::Interval(_) => unimplemented!(),
        DataType::Binary => Value::Blob(arr.as_binary::<i32>().value(i).to_vec()),
        DataType::FixedSizeBinary(_) => Value::Blob(arr.as_fixed_size_binary().value(i).to_vec()),
        DataType::LargeBinary => Value::Blob(arr.as_binary::<i64>().value(i).to_vec()),
        DataType::Utf8 => Value::Text(arr.as_string::<i32>().value(i).to_string()),
        DataType::LargeUtf8 => Value::Text(arr.as_string::<i32>().value(i).to_string()),
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
