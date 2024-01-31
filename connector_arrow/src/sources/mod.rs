//! This module defines four traits [`Source`], [`SourcePartition`], [`PartitionParser`], and [`Produce`]  to define a source.
//! This module also contains source implementations for various databases.

#[cfg(feature = "src_bigquery")]
pub mod bigquery;
#[cfg(feature = "src_csv")]
pub mod csv;
#[cfg(feature = "src_dummy")]
pub mod dummy;
#[cfg(feature = "src_mssql")]
pub mod mssql;
#[cfg(feature = "src_mysql")]
pub mod mysql;
#[cfg(feature = "src_oracle")]
pub mod oracle;
#[cfg(feature = "src_postgres")]
pub mod postgres;
#[cfg(feature = "src_sqlite")]
pub mod sqlite;

use crate::data_order::DataOrder;
use crate::errors::ConnectorXError;
use crate::sql::CXQuery;
use crate::typesystem::{Schema, TypeAssoc, TypeSystem};
use std::fmt::Debug;

/// A description of a data storage facility which has the ability to execute some
/// kind of queries and return relational data.
pub trait Source {
    /// Supported data orders, ordering by preference.
    const DATA_ORDERS: &'static [DataOrder];
    /// The type system produced by this [Source]
    type TypeSystem: TypeSystem;
    /// A "connection" to the [Source], that can be sent between threads.
    type Reader: SourceReader<TypeSystem = Self::TypeSystem, Error = Self::Error> + Send;
    type Error: From<ConnectorXError> + Send + Debug;

    fn reader(
        &mut self,
        query: &CXQuery,
        data_order: DataOrder,
    ) -> Result<Self::Reader, Self::Error>;
}

/// Obtained connection to the [Source], with an intent to execute a query.
pub trait SourceReader {
    type TypeSystem: TypeSystem;
    type Stream<'a>: ValueStream<'a, TypeSystem = Self::TypeSystem, Error = Self::Error>
    where
        Self: 'a;
    type Error: From<ConnectorXError> + Send + Debug;

    /// Start reading the source until the schema of the result can be constructed.
    /// All received values are stored in a buffer, so they can be emitted later via [SourceReader::value_stream].
    ///
    /// Note: currently most of the readers make a separate request to retrieve the schema.
    /// Most of them even modify the SQL query and add a LIMIT 1 clause. This is to be removed.
    fn fetch_until_schema(&mut self) -> Result<Schema<Self::TypeSystem>, Self::Error>;

    fn value_stream(
        &mut self,
        schema: &Schema<Self::TypeSystem>,
    ) -> Result<Self::Stream<'_>, Self::Error>;
}

/// Ability to provide a stream of values of variate types, representing a relation, in [DataOrder::RowMajor].
pub trait ValueStream<'a>: Send {
    type TypeSystem: TypeSystem;
    type Error: From<ConnectorXError> + Send + Debug;

    /// Fetch next batch from the data source.
    /// Returns the number of rows that can now be retrieved via [ValueStream::next_value].
    fn next_batch(&mut self) -> Result<Option<usize>, Self::Error> {
        #[allow(deprecated)]
        let (size, is_last) = self.fetch_batch()?;
        Ok(if size == 0 && is_last {
            None
        } else {
            Some(size)
        })
    }

    /// Fetch next batch from the data source.
    /// Returns the number of rows that can now be retrieved via [ValueStream::next_value].
    /// Also returns a bool, specifying that this is the last batch and that
    /// [ValueStream::fetch_batch] will always produce `(0, true)` from now on.
    /// (although the function might be called even after the last batch is fetched).
    #[deprecated(note = "use next_batch instead")]
    fn fetch_batch(&mut self) -> Result<(usize, bool), Self::Error>;

    /// Returns the next value in the stream. Must not be called without previously calling
    /// [ValueStream::fetch_batch]. Must only be called the number of times specified by the
    /// result of that call.
    ///
    /// Works by obtaining a value of type T by calling [Produce::produce].
    /// Usually this function does not need to be implemented,
    /// as it is defined for any T that we can [Produce].
    fn next_value<'r, T>(&'r mut self) -> Result<T, <Self as ValueStream<'a>>::Error>
    where
        T: TypeAssoc<Self::TypeSystem>,
        Self: Produce<'r, T, Error = <Self as ValueStream<'a>>::Error>,
    {
        self.produce()
    }
}

/// A type implemented `Produce<T>` means that it can produce a value `T` by consuming part of it's raw data buffer.
pub trait Produce<'r, T> {
    type Error: From<ConnectorXError> + Send;

    fn produce(&'r mut self) -> Result<T, Self::Error>;
}
