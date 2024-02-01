//! A dummy source that generates different values based on an internal counter.
//! This source is for test purpose.

mod typesystem;

pub use self::typesystem::DummyTypeSystem;
use super::{Produce, Source, SourceReader, ValueStream};
use crate::data_order::DataOrder;
use crate::errors::{ConnectorXError, Result};
use crate::sql::CXQuery;
use crate::typesystem::Schema;
use chrono::{offset, DateTime, Utc};

use num_traits::cast::FromPrimitive;

pub struct DummySource {
    schema: Schema<DummyTypeSystem>,
}

impl DummySource {
    pub fn new<S: AsRef<str>>(names: &[S], types: &[DummyTypeSystem]) -> Self {
        assert_eq!(names.len(), types.len());

        let names = names.iter().map(|s| s.as_ref().to_string()).collect();
        let types = types.to_vec();
        let schema = Schema { names, types };

        DummySource { schema }
    }
}

impl Source for DummySource {
    const DATA_ORDER: DataOrder = DataOrder::RowMajor;
    type TypeSystem = DummyTypeSystem;
    type Reader = DummyReader;
    type Error = ConnectorXError;

    fn reader(&mut self, query: &CXQuery) -> Result<Self::Reader> {
        Ok(DummyReader::new(self.schema.clone(), query))
    }
}

pub struct DummyReader {
    schema: Schema<DummyTypeSystem>,
    nrows: usize,
    ncols: usize,
    counter: usize,
}

impl DummyReader {
    pub fn new(schema: Schema<DummyTypeSystem>, q: &CXQuery<String>) -> Self {
        let v: Vec<usize> = q.as_str().split(',').map(|s| s.parse().unwrap()).collect();

        DummyReader {
            schema,
            nrows: v[0],
            ncols: v[1],
            counter: 0,
        }
    }
}

impl SourceReader for DummyReader {
    type TypeSystem = DummyTypeSystem;
    type Stream<'a> = DummyStream<'a>;
    type Error = ConnectorXError;

    fn fetch_until_schema(&mut self) -> Result<Schema<Self::TypeSystem>> {
        Ok(self.schema.clone())
    }

    fn value_stream(&mut self, _schema: &Schema<DummyTypeSystem>) -> Result<Self::Stream<'_>> {
        Ok(DummyStream::new(&mut self.counter, self.nrows, self.ncols))
    }
}

pub struct DummyStream<'a> {
    counter: &'a mut usize,
    #[allow(unused)]
    nrows: usize,
    ncols: usize,
}

impl<'a> DummyStream<'a> {
    fn new(counter: &'a mut usize, nrows: usize, ncols: usize) -> Self {
        DummyStream {
            counter,
            ncols,
            nrows,
        }
    }

    fn next_val(&mut self) -> usize {
        let ret = *self.counter / self.ncols;
        *self.counter += 1;
        ret
    }
}

impl<'a> ValueStream<'a> for DummyStream<'a> {
    type TypeSystem = DummyTypeSystem;
    type Error = ConnectorXError;

    fn fetch_batch(&mut self) -> Result<(usize, bool)> {
        Ok(if *self.counter == 0 {
            (self.nrows, true)
        } else {
            (0, true)
        })
    }
}

macro_rules! numeric_impl {
    ($($t: ty),+) => {
        $(
            impl<'r, 'a> Produce<'r, $t> for DummyStream<'a> {
                type Error = ConnectorXError;

                fn produce(&mut self) -> Result<$t> {
                    let ret = self.next_val();
                    Ok(FromPrimitive::from_usize(ret).unwrap_or_default())
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for DummyStream<'a> {
                type Error = ConnectorXError;

                fn produce(&mut self) -> Result<Option<$t>> {
                    let ret = self.next_val();
                    Ok(Some(FromPrimitive::from_usize(ret).unwrap_or_default()))
                }
            }
        )+
    };
}

numeric_impl!(u64, i32, i64, f64);

impl<'r, 'a> Produce<'r, String> for DummyStream<'a> {
    type Error = ConnectorXError;

    fn produce(&mut self) -> Result<String> {
        let ret = self.next_val().to_string();
        Ok(ret)
    }
}

impl<'r, 'a> Produce<'r, Option<String>> for DummyStream<'a> {
    type Error = ConnectorXError;

    fn produce(&mut self) -> Result<Option<String>> {
        let ret = self.next_val().to_string();
        Ok(Some(ret))
    }
}

impl<'r, 'a> Produce<'r, bool> for DummyStream<'a> {
    type Error = ConnectorXError;

    fn produce(&mut self) -> Result<bool> {
        let ret = self.next_val() % 2 == 0;
        Ok(ret)
    }
}

impl<'r, 'a> Produce<'r, Option<bool>> for DummyStream<'a> {
    type Error = ConnectorXError;

    fn produce(&mut self) -> Result<Option<bool>> {
        let ret = match self.next_val() % 3 {
            0 => Some(true),
            1 => Some(false),
            2 => None,
            _ => unreachable!(),
        };

        Ok(ret)
    }
}

impl<'r, 'a> Produce<'r, DateTime<Utc>> for DummyStream<'a> {
    type Error = ConnectorXError;

    fn produce(&mut self) -> Result<DateTime<Utc>> {
        self.next_val();
        let ret = offset::Utc::now();

        Ok(ret)
    }
}

impl<'r, 'a> Produce<'r, Option<DateTime<Utc>>> for DummyStream<'a> {
    type Error = ConnectorXError;

    fn produce(&mut self) -> Result<Option<DateTime<Utc>>> {
        self.next_val();
        let ret = match self.next_val() % 2 {
            0 => Some(offset::Utc::now()),
            1 => None,
            _ => unreachable!(),
        };
        Ok(ret)
    }
}
