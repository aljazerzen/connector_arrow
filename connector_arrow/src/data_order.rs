//! This module provides two data orders: row-wise and column-wise for tabular data,
//! as well as a function to coordinate the data order between source and destination.

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum DataOrder {
    RowMajor,
    ColumnMajor,
}
