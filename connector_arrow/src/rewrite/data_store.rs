use std::fmt::Debug;

use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use super::transport::{Produce, ProduceTy};

/// Description of something that stores data and can execute queries that return data.
pub trait DataStore: Clone + Send + Sync {
    type Error: Send + Debug;
    type Conn: DataStoreConnection<Error = Self::Error>;

    fn new_connection(&self) -> Result<Self::Conn, Self::Error>;
}

/// Connection to the [DataStore]
pub trait DataStoreConnection: Send {
    type Error: Send + Debug;
    type Task<'conn>: DataStoreTask<'conn, Error = Self::Error>
    where
        Self: 'conn;

    fn prepare_task<'a>(&'a mut self, query: &str) -> Result<Self::Task<'a>, Self::Error>;
}

/// Statement
pub trait DataStoreTask<'conn> {
    type Error: Send + Debug;
    type Params: Send + Sync + Clone;

    type Reader<'task>: ResultReader<'task, Error = Self::Error>
    where
        Self: 'task;

    fn start(&mut self, params: ()) -> Result<Self::Reader<'_>, Self::Error>;
}

/// Reads result of the query, starting with the schema.
/// Usually implemented as either RowsReader or BatchReader.
pub trait ResultReader<'task>: Sized {
    type Error: Send + Debug;
    type RowsReader: RowsReader<'task, Error = Self::Error>;
    type BatchReader: BatchReader<'task>;

    fn read_until_schema(&mut self) -> Result<Option<arrow::datatypes::Schema>, Self::Error>;

    fn try_into_batch(self) -> Result<Self::BatchReader, Self> {
        Err(self)
    }

    fn try_into_rows(self) -> Result<Self::RowsReader, Self> {
        Err(self)
    }
}

pub trait BatchReader<'task>: RecordBatchReader {}

pub trait RowsReader<'task> {
    type Error: Send + Debug;
    type RowReader<'rows>: RowReader<'rows, Error = Self::Error>
    where
        Self: 'rows;

    fn next_row(&mut self) -> Result<Option<Self::RowReader<'_>>, Self::Error>;
}

pub trait RowReader<'rows> {
    type Error: Send + Debug;
    type CellReader<'row>: Produce<'row>
    where
        Self: 'row;

    /// Will panic if called too many times.
    fn next_cell(&mut self) -> Self::CellReader<'_>;
}

pub use unsupported::UnsupportedReader;

mod unsupported {
    use std::marker::PhantomData;

    use super::*;

    /// A noop reader whose type can be used for non-supported readers in implementation of [TaskReader].
    pub struct UnsupportedReader<'task>(&'task PhantomData<bool>);

    impl<'task> Iterator for UnsupportedReader<'task> {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            unimplemented!()
        }
    }

    impl<'task> BatchReader<'task> for UnsupportedReader<'task> {}
    impl<'task> RecordBatchReader for UnsupportedReader<'task> {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            unimplemented!()
        }
    }

    impl<'task> RowsReader<'task> for UnsupportedReader<'task> {
        type Error = ();

        type RowReader<'rows> = UnsupportedReader<'rows>
        where
            Self: 'rows;

        fn next_row(&mut self) -> Result<Option<Self::RowReader<'_>>, Self::Error> {
            unimplemented!()
        }
    }

    impl<'task> RowReader<'task> for UnsupportedReader<'task> {
        type Error = ();

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
