//! This module defines traits that required to define a typesystem.
//!
//! A typesystem is an enum that describes what types can be produced by a source and accepted by a destination.
//! A typesystem also needs to implement [`TypeAssoc`] to associate the enum variants to the physical representation
//! of the types in the typesystem.

use fehler::throws;
use itertools::Itertools;

use crate::destinations::{Consume, Destination, PartitionWriter};
use crate::errors::{ConnectorXError, Result as CXResult};
use crate::sources::{Produce, Source, SourceReader, ValueStream};

#[derive(Debug, Clone, PartialEq)]
pub struct Schema<T: TypeSystem> {
    pub names: Vec<String>,
    pub types: Vec<T>,
}

impl<T: TypeSystem> Schema<T> {
    pub fn empty() -> Self {
        Schema {
            names: vec![],
            types: vec![],
        }
    }

    #[throws(ConnectorXError)]
    pub fn convert<D: TypeSystem, TP: Transport<TSS = T, TSD = D>>(&self) -> Schema<D> {
        let names = self.names.clone();
        let types = self
            .types
            .iter()
            .map(|&s| TP::convert_typesystem(s))
            .collect::<CXResult<Vec<_>>>()?;
        Schema { names, types }
    }

    pub fn len(&self) -> usize {
        let names_len = self.names.len();
        assert_eq!(names_len, self.types.len());
        names_len
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &T)> {
        self.names.iter().zip_eq(self.types.iter())
    }
}

#[doc(hidden)]
/// `TypeSystem` describes all the types a source or destination support
/// using enum variants.
/// The variant can be used to type check with a static type `T` through the `check` method.
pub trait TypeSystem: Copy + Clone + Send + Sync {
    /// Check whether T is the same type as defined by self.
    fn check<T: TypeAssoc<Self>>(self) -> CXResult<()> {
        T::check(self)
    }
}

#[doc(hidden)]
/// Associate a static type to a TypeSystem
pub trait TypeAssoc<TS: TypeSystem> {
    fn check(ts: TS) -> CXResult<()>;
}

#[doc(hidden)]
/// Realize means that a TypeSystem can realize a parameterized func F, based on its current variants.
pub trait Realize<F>
where
    F: ParameterizedFunc,
{
    /// realize a parameterized function with the type that self currently is.
    fn realize(self) -> CXResult<F::Function>;
}

#[doc(hidden)]
/// A ParameterizedFunc refers to a function that is parameterized on a type T,
/// where type T will be dynaically determined by the variant of a TypeSystem.
/// An example is the `transmit<S,W,T>` function. When piping values from a source
/// to the destination, its type `T` is determined by the schema at the runtime.
pub trait ParameterizedFunc {
    type Function;
    fn realize<T>() -> Self::Function
    where
        Self: ParameterizedOn<T>,
    {
        Self::parameterize()
    }
}

#[doc(hidden)]
/// `ParameterizedOn` indicates a parameterized function `Self`
/// is parameterized on type `T`
pub trait ParameterizedOn<T>: ParameterizedFunc {
    fn parameterize() -> Self::Function;
}

/// Defines a rule to convert a type `T` to a type `U`.
pub trait TypeConversion<T, U> {
    fn convert(val: T) -> U;
}

/// Transport asks the source to produce a value, do type conversion and then write
/// the value to a destination. Do not manually implement this trait for types.
/// Use [`impl_transport!`] to create a struct that implements this trait instead.
pub trait Transport {
    type TSS: TypeSystem;
    type TSD: TypeSystem;
    type S: Source;
    type D: Destination;
    type Error: From<ConnectorXError>
        + From<<Self::S as Source>::Error>
        + From<<Self::D as Destination>::Error>
        + Send
        + std::fmt::Debug;

    /// Convert the source type system TSS to the destination type system TSD.
    fn convert_typesystem(ts: Self::TSS) -> CXResult<Self::TSD>;

    /// Convert the type T1 from the source type system TSS into
    /// the type T2 from the destination type system TSD.
    fn convert_type<T1, T2>(val: T1) -> T2
    where
        Self: TypeConversion<T1, T2>,
    {
        <Self as TypeConversion<T1, T2>>::convert(val)
    }

    /// Ask source to produce a value of type T1 from TSS, and then
    /// do type conversion using [Transport::convert_type] to get value of type T2 from TSD.
    /// Finally, it will write the value of type T2 to the destination.
    fn transport<'s, 'd, 'r>(
        ts1: Self::TSS,
        ts2: Self::TSD,
        src: &'r mut <<Self::S as Source>::Reader as SourceReader>::Stream<'s>,
        dst: &'r mut <Self::D as Destination>::PartitionWriter,
    ) -> Result<(), Self::Error>
    where
        Self: 'd;

    #[allow(clippy::type_complexity)]
    fn transporter<'s, 'd>(
        ts1: Self::TSS,
        ts2: Self::TSD,
    ) -> CXResult<
        fn(
            src: &mut <<Self::S as Source>::Reader as SourceReader>::Stream<'s>,
            dst: &mut <Self::D as Destination>::PartitionWriter,
        ) -> Result<(), Self::Error>,
    >
    where
        Self: 'd;
}

#[doc(hidden)]
pub fn process<'s, 'd, 'r, T1, T2, TP, S, D, ES, ED, ET>(
    src: &'r mut <<S as Source>::Reader as SourceReader>::Stream<'s>,
    dst: &'r mut <D as Destination>::PartitionWriter,
) -> Result<(), ET>
where
    T1: TypeAssoc<<S as Source>::TypeSystem>,
    S: Source<Error = ES>,
    <S as Source>::Reader: SourceReader<Error = ES>,

    <<S as Source>::Reader as SourceReader>::Stream<'s>: Produce<'r, T1, Error = ES>,
    ES: From<ConnectorXError> + Send,

    T2: TypeAssoc<<D as Destination>::TypeSystem>,
    D: Destination<Error = ED>,
    <D as Destination>::PartitionWriter: Consume<T2, Error = ED>,
    ED: From<ConnectorXError> + Send,

    TP: TypeConversion<T1, T2>,
    ET: From<ES> + From<ED>,
{
    let val: T1 = ValueStream::next_value(src)?;
    let val: T2 = <TP as TypeConversion<T1, _>>::convert(val);
    PartitionWriter::write(dst, val)?;
    Ok(())
}
