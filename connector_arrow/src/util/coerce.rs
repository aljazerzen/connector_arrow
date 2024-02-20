use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float32Builder, Float64Builder};
use arrow::datatypes::{DataType, Field, Float16Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use itertools::Itertools;

pub fn coerce_batches<F>(
    batches: &[RecordBatch],
    coerce_fn: F,
) -> Result<Vec<RecordBatch>, arrow::error::ArrowError>
where
    F: Fn(&DataType) -> Option<DataType> + Copy,
{
    batches.iter().map(|b| coerce_batch(b, coerce_fn)).collect()
}

pub fn coerce_batch<F>(
    batch: &RecordBatch,
    coerce_fn: F,
) -> Result<RecordBatch, arrow::error::ArrowError>
where
    F: Fn(&DataType) -> Option<DataType> + Copy,
{
    let new_schema = coerce_schema(batch.schema(), coerce_fn);

    let new_columns = batch
        .columns()
        .iter()
        .map(|a| coerce_array(a.clone(), coerce_fn))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(new_schema, new_columns)
}

pub fn coerce_array<F>(array: ArrayRef, coerce_fn: F) -> Result<ArrayRef, arrow::error::ArrowError>
where
    F: Fn(&DataType) -> Option<DataType> + Copy,
{
    match coerce_fn(array.data_type()) {
        Some(new_ty) => match (array.data_type(), &new_ty) {
            (DataType::Float16, DataType::Float32) => Ok(coerce_float_16_to_32(&array)),
            (DataType::Float16, DataType::Float64) => Ok(coerce_float_16_to_64(&array)),
            _ => arrow::compute::cast(&array, &new_ty),
        },
        None => Ok(array),
    }
}

pub fn coerce_schema<F>(schema: SchemaRef, coerce_fn: F) -> SchemaRef
where
    F: Fn(&DataType) -> Option<DataType> + Copy,
{
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| match coerce_fn(f.data_type()) {
                Some(new_ty) => Field::new(f.name(), new_ty, true),
                None => Field::clone(f).with_nullable(true),
            })
            .collect_vec(),
    ))
}

fn coerce_float_16_to_32(array: &dyn Array) -> ArrayRef {
    // inefficient, but we don't need efficiency here

    let array = array.as_primitive::<Float16Type>();
    let mut builder = Float32Builder::with_capacity(array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(array.value(i).to_f32());
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn coerce_float_16_to_64(array: &dyn Array) -> ArrayRef {
    // inefficient, but we don't need efficiency here

    let array = array.as_primitive::<Float16Type>();
    let mut builder = Float64Builder::with_capacity(array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(array.value(i).to_f64());
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}
