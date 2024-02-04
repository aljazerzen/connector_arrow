use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};

use crate::rewrite::api::ResultReader;
use crate::rewrite::errors::ConnectorError;

/// Reader that contain all of the batches preloaded and just returns them one by one.
///
/// Useful for date store implementations that don't support streaming.
pub struct ArrowReader {
    schema: Arc<Schema>,
    inner: std::vec::IntoIter<RecordBatch>,
}

impl ArrowReader {
    pub fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        ArrowReader {
            schema,
            inner: batches.into_iter(),
        }
    }
}

impl Iterator for ArrowReader {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

impl<'stmt> ResultReader<'stmt> for ArrowReader {
    fn read_until_schema(&mut self) -> Result<Arc<arrow::datatypes::Schema>, ConnectorError> {
        Ok(self.schema.clone())
    }
}
