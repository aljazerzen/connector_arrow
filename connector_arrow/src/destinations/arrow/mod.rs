//! Destination implementation for Arrow and Polars.

mod arrow_assoc;
mod errors;
mod funcs;
pub mod typesystem;

pub use self::errors::{ArrowDestinationError, Result};
pub use self::typesystem::ArrowTypeSystem;
use super::{Consume, Destination, PartitionWriter};
use crate::constants::RECORD_BATCH_SIZE;
use crate::data_order::DataOrder;
use crate::typesystem::{Realize, Schema, TypeAssoc, TypeSystem};
use anyhow::anyhow;
use arrow::{datatypes::Schema as ArrowSchema, record_batch::RecordBatch};
use arrow_assoc::ArrowAssoc;
use fehler::throws;
use funcs::{FFinishBuilder, FNewBuilder, FNewField};
use itertools::zip_eq;
use std::any::Any;
use std::sync::{Arc, Mutex};

type Builder = Box<dyn Any + Send>;
type Builders = Vec<Builder>;

pub struct ArrowDestination {
    schema: Schema<ArrowTypeSystem>,
    arrow_schema: Arc<ArrowSchema>,
    data: Arc<Mutex<Vec<RecordBatch>>>,
    min_batch_size: usize,
}

impl Default for ArrowDestination {
    fn default() -> Self {
        ArrowDestination {
            schema: Schema::empty(),
            data: Arc::new(Mutex::new(vec![])),
            arrow_schema: Arc::new(ArrowSchema::empty()),
            min_batch_size: RECORD_BATCH_SIZE,
        }
    }
}

impl ArrowDestination {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_batch_size(batch_size: usize) -> Self {
        ArrowDestination {
            schema: Schema::empty(),
            data: Arc::new(Mutex::new(vec![])),
            arrow_schema: Arc::new(ArrowSchema::empty()),
            min_batch_size: batch_size,
        }
    }
}

impl Destination for ArrowDestination {
    type TypeSystem = ArrowTypeSystem;
    type PartitionWriter = ArrowPartitionWriter;
    type Error = ArrowDestinationError;

    #[throws(ArrowDestinationError)]
    fn set_schema(&mut self, schema: Schema<ArrowTypeSystem>) {
        // realize schema
        self.schema = schema;
        let fields = self
            .schema
            .iter()
            .map(|(h, &dt)| Ok(Realize::<FNewField>::realize(dt)?(h.as_str())))
            .collect::<Result<Vec<_>>>()?;
        self.arrow_schema = Arc::new(ArrowSchema::new(fields));
    }

    #[throws(ArrowDestinationError)]
    fn get_writer(&mut self, data_order: DataOrder) -> Self::PartitionWriter {
        ArrowPartitionWriter::new(
            self.schema.types.clone(),
            Arc::clone(&self.data),
            Arc::clone(&self.arrow_schema),
            self.min_batch_size,
            data_order,
        )
    }

    fn schema(&self) -> &Schema<ArrowTypeSystem> {
        &self.schema
    }
}

impl ArrowDestination {
    #[throws(ArrowDestinationError)]
    pub fn finish(self) -> Vec<RecordBatch> {
        let lock = Arc::try_unwrap(self.data).expect("Writers have not been dropped yet");
        lock.into_inner()
            .map_err(|e| anyhow!("mutex poisoned {}", e))?
    }

    #[throws(ArrowDestinationError)]
    pub fn get_one(&mut self) -> Option<RecordBatch> {
        let mut guard = self
            .data
            .lock()
            .map_err(|e| anyhow!("mutex poisoned {}", e))?;

        // TODO: this will return a batch from the end and mess up the order. Is this a problem?
        (*guard).pop()
    }

    pub fn arrow_schema(&self) -> Arc<ArrowSchema> {
        self.arrow_schema.clone()
    }
}

pub struct ArrowPartitionWriter {
    // settings
    schema: Vec<ArrowTypeSystem>,
    min_batch_size: usize,

    /// Determines into which column the next stream value should go.
    receiver: Organizer,

    /// Array buffers.
    builders: Option<Builders>,
    /// Number of rows reserved to be written in by [ArrowPartitionWriter::prepare_for_batch]
    rows_reserved: usize,
    /// Number of rows allocated within builders.
    rows_capacity: usize,

    // refs into ArrowDestination
    data: Arc<Mutex<Vec<RecordBatch>>>,
    arrow_schema: Arc<ArrowSchema>,
}

// unsafe impl Sync for ArrowPartitionWriter {}

impl ArrowPartitionWriter {
    fn new(
        schema: Vec<ArrowTypeSystem>,
        data: Arc<Mutex<Vec<RecordBatch>>>,
        arrow_schema: Arc<ArrowSchema>,
        min_batch_size: usize,
        data_order: DataOrder,
    ) -> Self {
        ArrowPartitionWriter {
            receiver: Organizer::new(data_order, schema.len()),

            builders: None,
            rows_reserved: 0,
            rows_capacity: 0,

            schema,
            min_batch_size,

            data,
            arrow_schema,
        }
    }

    /// Make sure that there is enough memory allocated in builders for the incoming batch.
    /// Might allocate more than needed, for future row reservations.
    #[throws(ArrowDestinationError)]
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

        let builders = self
            .schema
            .iter()
            .map(|dt| Ok(Realize::<FNewBuilder>::realize(*dt)?(to_allocate)))
            .collect::<Result<Vec<_>>>()?;
        self.builders = Some(builders);
        self.rows_reserved = row_count;
        self.rows_capacity = to_allocate;
    }

    #[throws(ArrowDestinationError)]
    fn flush(&mut self) {
        let Some(builders) = self.builders.take() else {
            return Ok(());
        };
        let columns = zip_eq(builders, &self.schema)
            .map(|(builder, dt)| Realize::<FFinishBuilder>::realize(*dt)?(builder))
            .collect::<std::result::Result<Vec<_>, crate::errors::ConnectorXError>>()?;
        let rb = RecordBatch::try_new(Arc::clone(&self.arrow_schema), columns)?;

        {
            let mut guard = self
                .data
                .lock()
                .map_err(|e| anyhow!("mutex poisoned {}", e))?;
            let inner_data = &mut *guard;
            inner_data.push(rb);
        }
    }
}

impl PartitionWriter for ArrowPartitionWriter {
    type TypeSystem = ArrowTypeSystem;
    type Error = ArrowDestinationError;

    #[throws(ArrowDestinationError)]
    fn prepare_for_batch(&mut self, row_count: usize) {
        self.receiver.reset_for_batch(row_count);
        self.allocate(row_count)?;
    }

    #[throws(ArrowDestinationError)]
    fn finalize(&mut self) {
        self.flush()?;
    }

    fn column_count(&self) -> usize {
        self.schema.len()
    }
}

impl<T> Consume<T> for ArrowPartitionWriter
where
    T: TypeAssoc<<Self as PartitionWriter>::TypeSystem> + ArrowAssoc + 'static,
{
    type Error = ArrowDestinationError;

    #[throws(ArrowDestinationError)]
    fn consume(&mut self, value: T) {
        let col = self.receiver.next_builder_index();

        self.schema[col].check::<T>()?;

        // this is safe, because prepare_for_batch must have been called earlier
        let builders = self.builders.as_mut().unwrap();
        let builder = builders[col]
            .downcast_mut::<T::Builder>()
            .ok_or_else(|| anyhow!("cannot cast arrow builder for append"))?;
        <T as ArrowAssoc>::append(builder, value)?;
    }
}

/// Determines into which column the next stream value should go.
pub struct Organizer {
    data_order: DataOrder,

    col_count: usize,
    row_count: usize,

    next_row: usize,
    next_col: usize,
}

impl Organizer {
    fn new(data_order: DataOrder, col_count: usize) -> Self {
        Organizer {
            data_order,

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

    fn next_builder_index(&mut self) -> usize {
        match self.data_order {
            DataOrder::RowMajor => {
                let col = self.next_col;

                self.next_col += 1;
                if self.next_col == self.col_count {
                    self.next_col = 0;
                    self.next_row += 1;
                }
                col
            }
            DataOrder::ColumnMajor => {
                let col = self.next_col;

                self.next_row += 1;
                if self.next_row == self.row_count {
                    self.next_row = 0;
                    self.next_col += 1;
                }
                col
            }
        }
    }
}
