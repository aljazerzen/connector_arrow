use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use super::errors::ConnectorError;
use super::transport::{Produce, ProduceTy};

/// Description of something that stores data and can execute queries that return data.
pub trait DataStore: Clone + Send + Sync {
    type Conn: DataStoreConnection;

    fn new_connection(&self) -> Result<Self::Conn, ConnectorError>;
}

/// Connection to the [DataStore]
pub trait DataStoreConnection: Send {
    type Task<'conn>: DataStoreTask<'conn>
    where
        Self: 'conn;

    fn prepare_task<'a>(&'a mut self, query: &str) -> Result<Self::Task<'a>, ConnectorError>;
}

/// Statement
pub trait DataStoreTask<'conn> {
    type Params: Send + Sync + Clone;

    type Reader<'task>: ResultReader<'task>
    where
        Self: 'task;

    fn start(&mut self, params: ()) -> Result<Self::Reader<'_>, ConnectorError>;
}

/// Reads result of the query, starting with the schema.
/// Usually implemented as either RowsReader or BatchReader.
pub trait ResultReader<'task>: Sized {
    type RowsReader: RowsReader<'task>;
    type BatchReader: BatchReader<'task>;

    fn read_until_schema(
        &mut self,
    ) -> Result<Option<Arc<arrow::datatypes::Schema>>, ConnectorError>;

    fn try_into_batch(self) -> Result<Self::BatchReader, Self> {
        Err(self)
    }

    fn try_into_rows(self) -> Result<Self::RowsReader, Self> {
        Err(self)
    }
}

pub trait BatchReader<'task>: Iterator<Item = RecordBatch> {}

pub trait RowsReader<'task> {
    type RowReader<'rows>: RowReader<'rows>
    where
        Self: 'rows;

    fn next_row(&mut self) -> Result<Option<Self::RowReader<'_>>, ConnectorError>;
}

pub trait RowReader<'rows> {
    type CellReader<'row>: Produce<'row>
    where
        Self: 'row;

    /// Will panic if called too many times.
    fn next_cell(&mut self) -> Self::CellReader<'_>;
}

pub use unsupported::UnsupportedReader;

mod unsupported {
    use std::marker::PhantomData;

    use arrow::record_batch::RecordBatch;

    use super::*;

    /// A noop reader whose type can be used for non-supported readers in implementation of [TaskReader].
    pub struct UnsupportedReader<'task>(&'task PhantomData<()>);

    impl<'task> Iterator for UnsupportedReader<'task> {
        type Item = RecordBatch;

        fn next(&mut self) -> Option<Self::Item> {
            unimplemented!()
        }
    }

    impl<'task> BatchReader<'task> for UnsupportedReader<'task> {}

    impl<'task> RowsReader<'task> for UnsupportedReader<'task> {
        type RowReader<'rows> = UnsupportedReader<'rows>
        where
            Self: 'rows;

        fn next_row(&mut self) -> Result<Option<Self::RowReader<'_>>, ConnectorError> {
            unimplemented!()
        }
    }

    impl<'task> RowReader<'task> for UnsupportedReader<'task> {
        type CellReader<'row> = UnsupportedReader<'row>
        where
            Self: 'row;

        fn next_cell(&mut self) -> Self::CellReader<'_> {
            unimplemented!()
        }
    }

    impl<'r> Produce<'r> for UnsupportedReader<'r> {}

    impl<'r, T> ProduceTy<'r, T> for UnsupportedReader<'r> {
        fn produce(&self) -> T {
            unimplemented!()
        }
    }
}
