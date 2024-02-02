//! Destination implementation for Arrow and Polars.

use arrow::array::builder::ArrayBuilder;
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use fehler::throws;
use std::any::Any;
use std::sync::Arc;

use super::errors::ConnectorError;
use super::transport::{Consume, ConsumeTy};

type Builder = Box<dyn ArrayBuilder>;

pub struct ArrowRowWriter {
    schema: Arc<Schema>,
    min_batch_size: usize,
    data: Vec<RecordBatch>,

    /// Determines into which column the next stream value should go.
    receiver: Organizer,

    /// Array buffers.
    builders: Option<Vec<Builder>>,
    /// Number of rows reserved to be written in by [ArrowPartitionWriter::prepare_for_batch]
    rows_reserved: usize,
    /// Number of rows allocated within builders.
    rows_capacity: usize,
}

// unsafe impl Sync for ArrowPartitionWriter {}

impl ArrowRowWriter {
    #[throws(ConnectorError)]
    pub fn new(schema: Arc<Schema>, min_batch_size: usize) -> Self {
        ArrowRowWriter {
            receiver: Organizer::new(schema.fields().len()),
            data: Vec::new(),

            builders: None,
            rows_reserved: 0,
            rows_capacity: 0,

            schema,
            min_batch_size,
        }
    }

    #[throws(ConnectorError)]
    pub fn prepare_for_batch(&mut self, row_count: usize) {
        self.receiver.reset_for_batch(row_count);
        self.allocate(row_count)?;
    }

    /// Make sure that there is enough memory allocated in builders for the incoming batch.
    /// Might allocate more than needed, for future row reservations.
    #[throws(ConnectorError)]
    fn allocate(&mut self, row_count: usize) {
        if self.rows_capacity >= row_count + self.rows_reserved {
            // there is enough capacity, no need to allocate
            self.rows_reserved += row_count;
            return;
        }

        if self.rows_reserved > 0 {
            self.flush()?;
        }

        let to_allocate = if row_count < self.min_batch_size {
            self.min_batch_size
        } else {
            row_count
        };

        let builders: Vec<Builder> = self
            .schema
            .fields
            .iter()
            .map(|f| arrow::array::make_builder(f.data_type(), to_allocate))
            .collect();

        self.builders = Some(builders);
        self.rows_reserved = row_count;
        self.rows_capacity = to_allocate;
    }

    #[throws(ConnectorError)]
    fn flush(&mut self) {
        let Some(mut builders) = self.builders.take() else {
            return Ok(());
        };
        let columns: Vec<ArrayRef> = builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();
        let rb = RecordBatch::try_new(self.schema.clone(), columns)?;
        self.data.push(rb);
    }

    #[throws(ConnectorError)]
    pub fn finish(mut self) -> Vec<RecordBatch> {
        self.flush()?;
        self.data
    }

    fn next_builder(&mut self) -> &mut dyn Any {
        let col = self.receiver.next_col_index();
        // this is safe, because prepare_for_batch must have been called earlier
        let builders = self.builders.as_mut().unwrap();
        builders[col].as_any_mut()
    }
}

impl Consume for ArrowRowWriter {}

impl ConsumeTy<bool> for ArrowRowWriter {
    fn consume(&mut self, value: bool) {
        self.next_builder()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_value(value);
    }
}
impl ConsumeTy<i64> for ArrowRowWriter {
    fn consume(&mut self, value: i64) {
        self.next_builder()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_value(value);
    }
}
impl ConsumeTy<i32> for ArrowRowWriter {
    fn consume(&mut self, value: i32) {
        self.next_builder()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_value(value);
    }
}
impl ConsumeTy<i16> for ArrowRowWriter {
    fn consume(&mut self, value: i16) {
        self.next_builder()
            .downcast_mut::<Int16Builder>()
            .unwrap()
            .append_value(value);
    }
}
impl ConsumeTy<i8> for ArrowRowWriter {
    fn consume(&mut self, value: i8) {
        self.next_builder()
            .downcast_mut::<Int8Builder>()
            .unwrap()
            .append_value(value);
    }
}
impl ConsumeTy<String> for ArrowRowWriter {
    fn consume(&mut self, value: String) {
        self.next_builder()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_value(value);
    }
}
impl ConsumeTy<f64> for ArrowRowWriter {
    fn consume(&mut self, value: f64) {
        self.next_builder()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_value(value);
    }
}
impl ConsumeTy<Vec<u8>> for ArrowRowWriter {
    fn consume(&mut self, value: Vec<u8>) {
        self.next_builder()
            .downcast_mut::<LargeBinaryBuilder>()
            .unwrap()
            .append_value(value);
    }
}

impl ConsumeTy<Option<bool>> for ArrowRowWriter {
    fn consume(&mut self, value: Option<bool>) {
        self.next_builder()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_option(value);
    }
}
impl ConsumeTy<Option<i64>> for ArrowRowWriter {
    fn consume(&mut self, value: Option<i64>) {
        self.next_builder()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_option(value);
    }
}
impl ConsumeTy<Option<i32>> for ArrowRowWriter {
    fn consume(&mut self, value: Option<i32>) {
        self.next_builder()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_option(value);
    }
}
impl ConsumeTy<Option<i16>> for ArrowRowWriter {
    fn consume(&mut self, value: Option<i16>) {
        self.next_builder()
            .downcast_mut::<Int16Builder>()
            .unwrap()
            .append_option(value);
    }
}
impl ConsumeTy<Option<i8>> for ArrowRowWriter {
    fn consume(&mut self, value: Option<i8>) {
        self.next_builder()
            .downcast_mut::<Int8Builder>()
            .unwrap()
            .append_option(value);
    }
}
impl ConsumeTy<Option<String>> for ArrowRowWriter {
    fn consume(&mut self, value: Option<String>) {
        self.next_builder()
            .downcast_mut::<LargeStringBuilder>()
            .unwrap()
            .append_option(value);
    }
}
impl ConsumeTy<Option<f64>> for ArrowRowWriter {
    fn consume(&mut self, value: Option<f64>) {
        self.next_builder()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_option(value);
    }
}
impl ConsumeTy<Option<Vec<u8>>> for ArrowRowWriter {
    fn consume(&mut self, value: Option<Vec<u8>>) {
        self.next_builder()
            .downcast_mut::<LargeBinaryBuilder>()
            .unwrap()
            .append_option(value);
    }
}

/// Determines into which column the next stream value should go.
pub struct Organizer {
    col_count: usize,
    row_count: usize,

    next_row: usize,
    next_col: usize,
}

impl Organizer {
    fn new(col_count: usize) -> Self {
        Organizer {
            col_count,
            row_count: 0,

            next_row: 0,
            next_col: 0,
        }
    }

    fn reset_for_batch(&mut self, row_count: usize) {
        self.row_count = row_count;
        self.next_row = 0;
        self.next_col = 0;
    }

    fn next_col_index(&mut self) -> usize {
        let col = self.next_col;

        self.next_col += 1;
        if self.next_col == self.col_count {
            self.next_col = 0;
            self.next_row += 1;
        }
        col
    }
}
