//! This module provides [`dispatcher::Dispatcher`], the core struct in ConnectorX
//! that drives the data loading from a source to a destination.
use crate::{
    data_order::{coordinate, DataOrder},
    destinations::{Destination, PartitionWriter},
    sources::{Source, SourceReader, ValueStream},
    sql::CXQuery,
    typesystem::{Schema, Transport},
};
use itertools::zip_eq;
use log::debug;
use rayon::prelude::*;
use std::marker::PhantomData;

/// A dispatcher takes a `S: Source`, a `D: Destination`, a `TP: Transport` and a vector of `queries` as input to
/// load data from `S` to `D` using the queries.
pub struct Dispatcher<'a, S, D, TP> {
    src: S,
    dst: &'a mut D,
    queries: Vec<CXQuery<String>>,
    _phantom: PhantomData<TP>,
}

pub struct PreparedDispatch<S: Source, D: Destination> {
    pub data_order: DataOrder,
    pub src_readers: Vec<S::Reader>,
    pub dst_writers: Vec<D::PartitionWriter>,
    pub src_schema: Schema<S::TypeSystem>,
    pub dst_schema: Schema<D::TypeSystem>,
}

impl<'w, S, D, TP> Dispatcher<'w, S, D, TP>
where
    S: Source,
    D: Destination,
    TP: Transport<TSS = S::TypeSystem, TSD = D::TypeSystem, S = S, D = D>,
{
    /// Create a new dispatcher by providing a source, a destination and the queries.
    pub fn new<Q>(src: S, dst: &'w mut D, queries: &[Q]) -> Self
    where
        for<'a> &'a Q: Into<CXQuery>,
    {
        Self {
            src,
            dst,
            queries: queries.iter().map(Into::into).collect(),
            _phantom: PhantomData,
        }
    }

    pub fn prepare(mut self) -> Result<PreparedDispatch<S, D>, TP::Error> {
        debug!("Prepare");
        let data_order = coordinate(S::DATA_ORDERS, D::DATA_ORDERS)?;

        let mut src_readers = Vec::with_capacity(self.queries.len());
        for query in &self.queries {
            let reader = self.src.reader(query, data_order)?;
            src_readers.push(reader);
        }

        debug!("Fetching metadata");
        let first_reader = src_readers.first_mut().unwrap();
        let src_schema = first_reader.fetch_until_schema()?;
        let dst_schema = src_schema.convert::<TP::TSD, TP>()?;

        self.dst.set_schema(dst_schema.clone())?;

        debug!("Create destination partition");
        let mut dst_writers = Vec::with_capacity(self.queries.len());
        for _ in 0..self.queries.len() {
            dst_writers.push(self.dst.get_writer(data_order)?);
        }

        Ok(PreparedDispatch {
            data_order,
            src_readers,
            dst_writers,
            src_schema,
            dst_schema,
        })
    }

    /// Start the data loading process.
    pub fn run(self) -> Result<(), TP::Error> {
        debug!("Run dispatcher");
        let PreparedDispatch {
            data_order,
            src_readers,
            dst_writers: dst_partitions,
            src_schema,
            dst_schema,
        } = self.prepare()?;

        #[cfg(all(not(feature = "branch"), not(feature = "fptr")))]
        compile_error!("branch or fptr, pick one");

        #[cfg(feature = "branch")]
        let types: Vec<_> = zip_eq(src_schema.types.clone(), dst_schema.types).collect();

        debug!("Start writing");
        // parse and write
        dst_partitions
            .into_par_iter()
            .zip_eq(src_readers)
            .enumerate()
            .try_for_each(|(i, (mut dst, mut src))| -> Result<(), TP::Error> {
                #[cfg(feature = "fptr")]
                let transporters: Vec<_> = zip_eq(&src_schema.types, &dst_schema.types)
                    .map(|(src_ty, dst_ty)| TP::transporter(*src_ty, *dst_ty))
                    .collect::<crate::errors::Result<Vec<_>>>()?;

                let mut value_stream = src.value_stream(&src_schema)?;

                while let Some(batch_size) = value_stream.next_batch()? {
                    dst.prepare_for_batch(batch_size)?;
                    match data_order {
                        DataOrder::RowMajor => {
                            for _ in 0..batch_size {
                                #[allow(clippy::needless_range_loop)]
                                for col in 0..dst.column_count() {
                                    #[cfg(feature = "fptr")]
                                    {
                                        let transporter = transporters[col];
                                        transporter(&mut value_stream, &mut dst)?;
                                    }

                                    #[cfg(feature = "branch")]
                                    {
                                        let (s1, s2) = types[col];
                                        TP::transport(s1, s2, &mut value_stream, &mut dst)?;
                                    }
                                }
                            }
                        }
                        DataOrder::ColumnMajor => {
                            // TODO: this could be optimized, since the lookup for the transportor does
                            // not need to happen for each iteration over rows.

                            #[allow(clippy::needless_range_loop)]
                            for col in 0..dst.column_count() {
                                for _ in 0..batch_size {
                                    #[cfg(feature = "fptr")]
                                    {
                                        let transporter = transporters[col];
                                        transporter(&mut value_stream, &mut dst)?;
                                    }

                                    #[cfg(feature = "branch")]
                                    {
                                        let (s1, s2) = types[col];
                                        TP::transport(s1, s2, &mut value_stream, &mut dst)?;
                                    }
                                }
                            }
                        }
                    }
                }

                debug!("Finalize partition {}", i);
                dst.finalize()?;
                debug!("Partition {} finished", i);
                Ok(())
            })?;

        debug!("Writing finished");

        Ok(())
    }

    /// Only fetch the metadata (header) of the destination.
    pub fn get_meta(&mut self) -> Result<(), TP::Error> {
        let dorder = coordinate(S::DATA_ORDERS, D::DATA_ORDERS)?;

        let mut reader = self.src.reader(self.queries.first().unwrap(), dorder)?;

        let src_schema = reader.fetch_until_schema()?;
        let dst_schema = src_schema.convert::<TP::TSD, TP>()?;
        self.dst.set_schema(dst_schema)?;
        Ok(())
    }
}
