//! This module defines three traits [`Destination`], [`DestinationPartition`], and [`Consume`] to define a destination.
//! This module also contains destination implementations for various dataframes.

pub mod arrow;

use crate::data_order::DataOrder;
use crate::errors::ConnectorXError;
use crate::typesystem::{Schema, TypeAssoc, TypeSystem};

/// A `Destination` is associated with a `TypeSystem` and a `PartitionDestination`.
/// `PartitionDestination` allows multiple threads write data into the buffer owned by `Destination`.
pub trait Destination: Sized {
    const DATA_ORDERS: &'static [DataOrder];
    type TypeSystem: TypeSystem;
    type PartitionWriter: PartitionWriter<TypeSystem = Self::TypeSystem, Error = Self::Error>;
    type Error: From<ConnectorXError> + Send;

    /// Set metadata of the destination writer.
    fn set_schema(&mut self, schema: Schema<Self::TypeSystem>) -> Result<(), Self::Error>;

    /// Allocates memory for a destination partition and returns a writer for that partition.
    fn alloc_writer(&mut self, data_order: DataOrder)
        -> Result<Self::PartitionWriter, Self::Error>;

    /// Return the schema of the destination.
    fn schema(&self) -> &Schema<Self::TypeSystem>;
}

/// Ability to write values to its own partition of the destination.
pub trait PartitionWriter: Send {
    type TypeSystem: TypeSystem;
    type Error: From<ConnectorXError> + Send;

    /// The next value into the destination buffer. If T mismatches with the
    /// schema, `ConnectorXError::TypeCheckFailed` will return.
    fn write<T>(&mut self, value: T) -> Result<(), <Self as PartitionWriter>::Error>
    where
        T: TypeAssoc<Self::TypeSystem>,
        Self: Consume<T, Error = <Self as PartitionWriter>::Error>,
    {
        self.consume(value)
    }

    /// Number of rows this `PartitionDestination` controls.
    fn column_count(&self) -> usize;

    /// Final clean ups
    fn finalize(&mut self) -> Result<(), Self::Error>;
}

/// A type implemented `Consume<T>` means that it can consume a value `T` by adding it to it's own buffer.
pub trait Consume<T> {
    type Error: From<ConnectorXError> + Send;
    fn consume(&mut self, value: T) -> Result<(), Self::Error>;
}
