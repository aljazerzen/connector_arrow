use std::{any::Any, sync::Arc};

use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use fehler::throws;

use super::transport::Consume;
use crate::rewrite::errors::ConnectorError;

/// Receives values row-by-row and passes them to [ArrayBuilder]s,
/// which construct [RecordBatch]es.
pub struct ArrowRowWriter {
    schema: Arc<Schema>,
    min_batch_size: usize,
    data: Vec<RecordBatch>,

    /// Determines into which column the next stream value should go.
    receiver: Organizer,

    /// Array buffers.
    builders: Option<Vec<Box<dyn ArrayBuilder>>>,
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

        let builders: Vec<Box<dyn ArrayBuilder>> = self
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

macro_rules! impl_consume_ty {
    (
        $(
            { $Native:ty => $Builder:tt }
        )*
    ) => {
        $(
            impl super::transport::ConsumeTy<$Native> for ArrowRowWriter {
                fn consume(&mut self, value: $Native) {
                    self.next_builder()
                        .downcast_mut::<arrow::array::builder::$Builder>().unwrap()
                        .append_value(value);
                }
                fn consume_opt(&mut self, value: Option<$Native>) {
                    self.next_builder()
                        .downcast_mut::<arrow::array::builder::$Builder>().unwrap()
                        .append_option(value);
                }
            }
        )+
    };
}

// List of ConsumeTy implementations to generate.
// Must match with arrow::array::make_builder
impl_consume_ty! {
    // {         => NullBuilder            }  // Null
       { bool    => BooleanBuilder         }  // Boolean
       { i8      => Int8Builder            }  // Int8
       { i16     => Int16Builder           }  // Int16
       { i32     => Int32Builder           }  // Int32
       { i64     => Int64Builder           }  // Int64
       { u8      => UInt8Builder           }  // UInt8
       { u16     => UInt16Builder          }  // UInt16
       { u32     => UInt32Builder          }  // UInt32
       { u64     => UInt64Builder          }  // UInt64
    // {         => Float16Builder         }  // Float16
       { f32     => Float32Builder         }  // Float32
       { f64     => Float64Builder         }  // Float64
    // {         => BinaryBuilder          }  // Binary
       { Vec<u8> => LargeBinaryBuilder     }  // LargeBinary
    // {         => FixedSizeBinaryBuilder }  // FixedSizeBinary
    // {         => Decimal128Builde       }  // Decimal128
    // {         => Decimal256Builde       }  // Decimal256
    // {         => StringBuilde           }  // Utf8
       { String  => LargeStringBuilder     }  // LargeUtf8
    // {         => Date32Builde           }  // Date32
    // {         => Date64Builde           }  // Date64
}
