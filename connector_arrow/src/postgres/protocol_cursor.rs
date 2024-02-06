use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{Row, RowIter};

use crate::api::{ResultReader, Statement};
use crate::util::row_writer;
use crate::util::transport::{self, Produce, ProduceTy};
use crate::util::CellReader;
use crate::{errors::ConnectorError, util::RowsReader};

use super::{types, CursorProtocol, PostgresStatement};
pub use super::{PostgresError, SimpleProtocol};

impl<'conn> Statement<'conn> for PostgresStatement<'conn, CursorProtocol> {
    type Params = ();

    type Reader<'stmt> = PostgresBatchStream<'stmt> where Self: 'stmt;

    fn start(&mut self, _params: ()) -> Result<Self::Reader<'_>, ConnectorError> {
        let stmt = &self.stmt;
        let schema = types::convert_schema(stmt)?;

        let rows = self
            .client
            .query_raw::<_, bool, _>(&self.query, vec![])
            .map_err(PostgresError::from)?;

        let row_reader = PostgresRowStream::new(rows);
        Ok(PostgresBatchStream {
            schema,
            row_reader,
            is_finished: false,
        })
    }
}

pub struct PostgresBatchStream<'a> {
    schema: Arc<Schema>,
    row_reader: PostgresRowStream<'a>,
    is_finished: bool,
}

impl<'a> ResultReader<'a> for PostgresBatchStream<'a> {
    fn get_schema(&mut self) -> Result<std::sync::Arc<arrow::datatypes::Schema>, ConnectorError> {
        Ok(self.schema.clone())
    }
}

impl<'a> PostgresBatchStream<'a> {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.is_finished {
            return Ok(None);
        }

        let batch_size = 1024;

        let mut writer = row_writer::ArrowRowWriter::new(self.schema.clone(), batch_size);

        for _ in 0..batch_size {
            if let Some(mut cell_reader) = self.row_reader.next_row()? {
                writer.prepare_for_batch(1)?;

                for field in &self.schema.fields {
                    let cell_ref = cell_reader.next_cell();

                    transport::transport(field, cell_ref.unwrap(), &mut writer)?;
                }
            } else {
                self.is_finished = true;
                break;
            }
        }

        let batches = writer.finish()?;
        if batches.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batches.into_iter().exactly_one().unwrap()))
        }
    }
}

impl<'a> Iterator for PostgresBatchStream<'a> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

struct PostgresRowStream<'a> {
    iter: postgres::RowIter<'a>,
}

impl<'a> PostgresRowStream<'a> {
    pub fn new(iter: RowIter<'a>) -> Self {
        Self { iter }
    }
}

impl<'stmt> RowsReader<'stmt> for PostgresRowStream<'stmt> {
    type CellReader<'row> = PostgresCellReader where Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
        let row = self.iter.next().map_err(PostgresError::from)?;

        Ok(row.map(|row| PostgresCellReader { row, next_col: 0 }))
    }
}

struct PostgresCellReader {
    row: Row,
    next_col: usize,
}

impl<'row> CellReader<'row> for PostgresCellReader {
    type CellRef<'cell> = CellRef<'cell> where Self: 'cell;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        if self.next_col >= self.row.columns().len() {
            return None;
        }
        let col = self.next_col;
        self.next_col += 1;
        Some((&self.row, col))
    }
}

type CellRef<'a> = (&'a Row, usize);

impl<'c> Produce<'c> for CellRef<'c> {}

macro_rules! impl_produce {
    ($($t: ty,)+) => {
        $(
            impl<'c> ProduceTy<'c, $t> for CellRef<'c> {
                fn produce(self) -> Result<$t, ConnectorError> {
                    Ok(self.0.get::<usize, $t>(self.1))
                }

                fn produce_opt(self) -> Result<Option<$t>, ConnectorError> {
                    Ok(self.0.get::<usize, Option<$t>>(self.1))
                }
            }
        )+
    };
}

macro_rules! impl_produce_unimplemented {
    ($($t: ty,)+) => {
        $(
            impl<'r> ProduceTy<'r, $t> for CellRef<'r> {
                fn produce(self) -> Result<$t, ConnectorError> {
                   unimplemented!();
                }

                fn produce_opt(self) -> Result<Option<$t>, ConnectorError> {
                   unimplemented!();
                }
            }
        )+
    };
}

impl_produce!(bool, i8, i16, i32, i64, f32, f64, Vec<u8>, String,);
impl_produce_unimplemented!(u8, u16, u32, u64,);
