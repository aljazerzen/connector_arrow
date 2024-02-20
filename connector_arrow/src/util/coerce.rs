use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
        Some(new_ty) => arrow::compute::cast(&array, &new_ty),
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
