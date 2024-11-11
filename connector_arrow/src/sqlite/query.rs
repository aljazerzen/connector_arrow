use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::*;
use itertools::{zip_eq, Itertools};
use rusqlite::types::{Type, Value};

use crate::api::{Connector, Statement};
use crate::types::FixedSizeBinaryType;
use crate::util::transport::{self, Produce, ProduceTy};
use crate::util::{collect_rows_to_arrow, CellReader, RowsReader};
use crate::util::{ArrayCellRef, ArrowReader};
use crate::ConnectorError;

use super::SQLiteConnection;

pub struct SQLiteStatement<'conn> {
    pub stmt: rusqlite::Statement<'conn>,
}

impl<'conn> Statement<'conn> for SQLiteStatement<'conn> {
    type Reader<'task>
        = ArrowReader
    where
        Self: 'task;

    fn start_batch<'p>(
        &mut self,
        args: (&RecordBatch, usize),
    ) -> Result<Self::Reader<'_>, ConnectorError> {
        let column_count = self.stmt.column_count();

        // args
        let arg_cells = ArrayCellRef::vec_from_batch(args.0, args.1);
        let mut args: Vec<Value> = Vec::with_capacity(arg_cells.len());
        for cell in arg_cells {
            transport::transport(cell.field, &cell, &mut args)?;
        }
        let args = args.iter().map(|x| x as &dyn rusqlite::ToSql).collect_vec();

        // query
        let rows: Vec<Vec<Value>> = {
            let mut rows_iter = self.stmt.query(args.as_slice())?;

            // read all of the rows into a buffer
            let mut rows = Vec::with_capacity(1024);
            while let Some(row_ref) = rows_iter.next()? {
                let mut row = Vec::with_capacity(column_count);
                for col_index in 0..column_count {
                    let value = row_ref.get::<_, Value>(col_index).unwrap();
                    row.push(value);
                }
                rows.push(row);
            }
            rows
        };

        // infer schema
        let schema = infer_schema(&self.stmt, &rows, column_count)?;

        // iterate over rows and convert into arrow
        let row_count = rows.len();
        let mut rows = SQLiteRowsReader {
            rows: rows.into_iter(),
        };
        let batches = collect_rows_to_arrow(schema.clone(), &mut rows, row_count)?;

        Ok(ArrowReader::new(schema, batches))
    }
}

fn infer_schema(
    stmt: &rusqlite::Statement,
    rows: &Vec<Vec<Value>>,
    column_count: usize,
) -> Result<Arc<arrow::datatypes::Schema>, ConnectorError> {
    let mut types = vec![None; column_count];

    for row in rows {
        let mut all_known = true;

        for (col_index, cell) in row.iter().enumerate() {
            let ty = &mut types[col_index];
            if ty.is_none() {
                *ty = match cell.data_type() {
                    Type::Null => None,
                    dt => {
                        let dt = dt.to_string().to_uppercase();
                        Some(SQLiteConnection::type_db_into_arrow(&dt).unwrap())
                    }
                };
            }
            if ty.is_none() {
                all_known = false;
            }
        }

        if all_known {
            break;
        }
    }

    let mut fields = Vec::with_capacity(column_count);
    for (name, ty) in zip_eq(stmt.column_names(), types) {
        let ty = ty.unwrap_or(DataType::Null);

        let nullable = true; // dynamic type system FTW
        fields.push(arrow::datatypes::Field::new(name, ty, nullable));
    }

    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

pub struct SQLiteRowsReader {
    rows: std::vec::IntoIter<Vec<Value>>,
}

impl RowsReader<'_> for SQLiteRowsReader {
    type CellReader<'rows>
        = SQLiteCellReader
    where
        Self: 'rows;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
        Ok(self.rows.next().map(|row| SQLiteCellReader {
            row: row.into_iter(),
        }))
    }
}

pub struct SQLiteCellReader {
    row: std::vec::IntoIter<Value>,
}

impl CellReader<'_> for SQLiteCellReader {
    type CellRef<'row>
        = Value
    where
        Self: 'row;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        self.row.next()
    }
}

impl Produce<'_> for Value {}

impl ProduceTy<'_, Int64Type> for Value {
    fn produce(self) -> Result<i64, ConnectorError> {
        unimplemented!()
    }
    fn produce_opt(self) -> Result<Option<i64>, ConnectorError> {
        Ok(match self {
            Self::Null => None,
            Self::Integer(v) => Some(v),
            _ => panic!("SQLite schema not inferred correctly"),
        })
    }
}

impl ProduceTy<'_, Float64Type> for Value {
    fn produce(self) -> Result<f64, ConnectorError> {
        unimplemented!()
    }
    fn produce_opt(self) -> Result<Option<f64>, ConnectorError> {
        Ok(match self {
            Self::Null => None,
            Self::Real(v) => Some(v),
            _ => panic!("SQLite schema not inferred correctly"),
        })
    }
}

impl ProduceTy<'_, Utf8Type> for Value {
    fn produce(self) -> Result<String, ConnectorError> {
        unimplemented!()
    }
    fn produce_opt(self) -> Result<Option<String>, ConnectorError> {
        Ok(match self {
            Self::Null => None,
            Self::Text(v) => Some(v),
            _ => panic!("SQLite schema not inferred correctly"),
        })
    }
}

impl ProduceTy<'_, BinaryType> for Value {
    fn produce(self) -> Result<Vec<u8>, ConnectorError> {
        unimplemented!()
    }
    fn produce_opt(self) -> Result<Option<Vec<u8>>, ConnectorError> {
        Ok(match self {
            Self::Null => None,
            Self::Blob(v) => Some(v),
            _ => panic!("SQLite schema not inferred correctly"),
        })
    }
}

crate::impl_produce_unsupported!(
    Value,
    (
        BooleanType,
        Int8Type,
        Int16Type,
        Int32Type,
        UInt8Type,
        UInt16Type,
        UInt32Type,
        UInt64Type,
        Float16Type,
        Float32Type,
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
        LargeBinaryType,
        FixedSizeBinaryType,
        LargeUtf8Type,
        Decimal128Type,
        Decimal256Type,
    )
);
