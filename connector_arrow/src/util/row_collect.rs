//! Destination implementation for Arrow and Polars.

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::errors::ConnectorError;
use crate::util::row_writer;
use crate::util::transport;

pub fn collect_rows_to_arrow<'stmt, T: RowsReader<'stmt>>(
    schema: Arc<Schema>,
    rows_reader: &mut T,
    min_batch_size: usize,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let mut writer = row_writer::ArrowRowWriter::new(schema.clone(), min_batch_size);
    log::debug!("reading rows");

    while let Some(mut row_reader) = rows_reader.next_row()? {
        writer.prepare_for_batch(1)?;

        log::debug!("reading row");
        for field in &schema.fields {
            log::debug!("reading cell");
            let cell_ref = row_reader.next_cell();
            log::debug!("transporting cell");

            transport::transport(field, cell_ref.unwrap(), &mut writer)?;
        }
    }
    writer.finish()
}

/// Iterator over rows.
// Cannot be an actual iterator, because of lifetime requirements (I think).
pub trait RowsReader<'stmt> {
    type CellReader<'row>: CellReader<'row>
    where
        Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError>;
}

/// Iterator over cells of a row.
// Cannot be an actual iterator, because of lifetime requirements (I think).
pub trait CellReader<'row> {
    type CellRef<'cell>: transport::Produce<'cell>
    where
        Self: 'cell;

    /// Will panic if called too many times.
    fn next_cell(&mut self) -> Option<Self::CellRef<'_>>;
}
