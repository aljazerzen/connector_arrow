use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{Row, RowIter};

use crate::api::{ResultReader, Statement};
use crate::types::{ArrowType, FixedSizeBinaryType};
use crate::util::transport;
use crate::util::{ArrowRowWriter, CellReader};
use crate::{errors::ConnectorError, util::RowsReader};

use super::{types, PostgresError, PostgresStatement, ProtocolExtended};

impl<'conn> Statement<'conn> for PostgresStatement<'conn, ProtocolExtended> {
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
    schema: SchemaRef,
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

        let mut writer = ArrowRowWriter::new(self.schema.clone(), batch_size);

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

impl<'c> transport::Produce<'c> for CellRef<'c> {}

macro_rules! impl_produce {
    ($($t: ty,)+) => {
        $(
            impl<'c> transport::ProduceTy<'c, $t> for CellRef<'c> {
                fn produce(self) -> Result<<$t as ArrowType>::Native, ConnectorError> {
                    Ok(self.0.get(self.1))
                }

                fn produce_opt(self) -> Result<Option<<$t as ArrowType>::Native>, ConnectorError> {
                    Ok(self.0.get(self.1))
                }
            }
        )+
    };
}
impl_produce!(
    BooleanType,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    LargeBinaryType,
    LargeUtf8Type,
);

crate::impl_produce_unused!(
    CellRef<'r>,
    (
        UInt8Type,
        UInt16Type,
        UInt32Type,
        UInt64Type,
        Float16Type,
        TimestampSecondType,
        TimestampMillisecondType,
        TimestampMicrosecondType,
        TimestampNanosecondType,
        Date32Type,
        Date64Type,
        Time32SecondType,
        Time32MillisecondType,
        Time64MicrosecondType,
        Time64NanosecondType,
        IntervalYearMonthType,
        IntervalDayTimeType,
        IntervalMonthDayNanoType,
        DurationSecondType,
        DurationMillisecondType,
        DurationMicrosecondType,
        DurationNanosecondType,
        BinaryType,
        FixedSizeBinaryType,
        Utf8Type,
        Decimal128Type,
        Decimal256Type,
    )
);
