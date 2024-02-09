//! Utilities for converting row-major tabular data into Apache Arrow.
//! Used by database client implementations.

mod arrow_reader;
mod row_collect;
mod row_writer;
pub mod transport;

pub use arrow_reader::ArrowReader;
pub use row_collect::{collect_rows_to_arrow, CellReader, RowsReader};
pub use row_writer::ArrowRowWriter;
