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

/// A task that is to be executed in the data store, over a connection.
pub trait DataStoreTask<'conn> {
    type Params: Send + Sync + Clone;

    type Reader<'task>: ResultReader<'task>
    where
        Self: 'task;

    /// Start executing.
    /// This will create a reader that can retrieve schema and then the data.
    fn start(&mut self, params: ()) -> Result<Self::Reader<'_>, ConnectorError>;
}

/// Reads result of the query, starting with the schema.
/// Usually implemented as either RowsReader or BatchReader.
pub trait ResultReader<'task>: Sized {
    type RowsReader: RowsReader<'task>;
    type BatchReader: BatchReader<'task>;

    /// Read the results until the schema can be produced.
    /// Results are not discarded, but kept in a buffer to be read by one of the readers.
    fn read_until_schema(
        &mut self,
    ) -> Result<Option<Arc<arrow::datatypes::Schema>>, ConnectorError>;

    /// If implemented, returns a [BatchReader] that can iterate over [arrow::array::RecordBatch].
    /// If not implemented, returns self back.
    fn try_into_batch(self) -> Result<Self::BatchReader, Self> {
        Err(self)
    }

    /// If implemented, returns a [RowsReader] that can iterate over rows of the result.
    /// If not implemented, returns self back.
    fn try_into_rows(self) -> Result<Self::RowsReader, Self> {
        Err(self)
    }
}

/// Iterator over [RecordBatch]es.
pub trait BatchReader<'task>: Iterator<Item = RecordBatch> {}

/// Iterator over rows.
// Cannot be an actual iterator, because of lifetime requirements (I think).
pub trait RowsReader<'task> {
    type CellReader<'row>: CellReader<'row>
    where
        Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError>;
}

/// Iterator over cells of a row.
// Cannot be an actual iterator, because of lifetime requirements (I think).
pub trait CellReader<'row> {
    type CellRef<'cell>: Produce<'cell>
    where
        Self: 'cell;

    /// Will panic if called too many times.
    fn next_cell(&mut self) -> Option<Self::CellRef<'_>>;
}

pub use unsupported::UnsupportedReader;

mod unsupported {
    use std::marker::PhantomData;

    use arrow::record_batch::RecordBatch;

    use super::*;

    /// A noop reader whose type can be used for non-supported readers in implementation of [ResultReader].
    pub struct UnsupportedReader<'task>(&'task PhantomData<()>);

    impl<'task> Iterator for UnsupportedReader<'task> {
        type Item = RecordBatch;

        fn next(&mut self) -> Option<Self::Item> {
            unimplemented!()
        }
    }

    impl<'task> BatchReader<'task> for UnsupportedReader<'task> {}

    impl<'task> RowsReader<'task> for UnsupportedReader<'task> {
        type CellReader<'rows> = UnsupportedReader<'rows>
        where
            Self: 'rows;

        fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
            unimplemented!()
        }
    }

    impl<'task> CellReader<'task> for UnsupportedReader<'task> {
        type CellRef<'row> = UnsupportedReader<'row>
        where
            Self: 'row;

        fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
            unimplemented!()
        }
    }

    impl<'r> Produce<'r> for UnsupportedReader<'r> {}

    impl<'r, T> ProduceTy<'r, T> for UnsupportedReader<'r> {
        fn produce(&self) -> T {
            unimplemented!()
        }
        fn produce_opt(&self) -> Option<T> {
            unimplemented!()
        }
    }
}
