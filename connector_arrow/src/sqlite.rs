use std::sync::Arc;

use itertools::zip_eq;
use rusqlite::types::{Type, Value};

use super::api::*;
use super::errors::ConnectorError;
use super::util::arrow_reader::ArrowReader;
use super::util::transport::{Produce, ProduceTy};
use super::util::{collect_rows_to_arrow, CellReader, RowsReader};

impl Connection for rusqlite::Connection {
    type Stmt<'conn> = SQLiteStatement<'conn> where Self: 'conn;

    fn prepare(&mut self, query: &str) -> Result<SQLiteStatement, ConnectorError> {
        let stmt = rusqlite::Connection::prepare(self, query)?;
        Ok(SQLiteStatement { stmt })
    }
}

pub struct SQLiteStatement<'conn> {
    stmt: rusqlite::Statement<'conn>,
}

impl<'conn> Statement<'conn> for SQLiteStatement<'conn> {
    type Params = ();
    type Reader<'task> = ArrowReader where Self: 'task;

    fn start(&mut self, params: Self::Params) -> Result<Self::Reader<'_>, ConnectorError> {
        let column_count = self.stmt.column_count();

        let rows = {
            let mut rows_iter = self.stmt.query(params)?;

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

pub struct SQLiteRowsReader {
    rows: std::vec::IntoIter<Vec<Value>>,
}

impl<'stmt> RowsReader<'stmt> for SQLiteRowsReader {
    type CellReader<'rows> = SQLiteCellReader
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

impl<'rows> CellReader<'rows> for SQLiteCellReader {
    type CellRef<'row> = Value
    where
        Self: 'row;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        self.row.next()
    }
}

impl<'r> Produce<'r> for Value {}

impl<'r> ProduceTy<'r, i64> for Value {
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

impl<'r> ProduceTy<'r, f64> for Value {
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

impl<'r> ProduceTy<'r, String> for Value {
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

impl<'r> ProduceTy<'r, Vec<u8>> for Value {
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

macro_rules! impl_produce_unimplemented {
    ($($t: ty,)+) => {
        $(
            impl<'r> ProduceTy<'r, $t> for Value {
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

impl_produce_unimplemented!(bool, i8, i16, i32, u8, u16, u32, u64, f32,);

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
                *ty = convert_datatype(cell.data_type());
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
        let Some(ty) = ty else {
            return Err(ConnectorError::CannotConvertSchema);
        };

        let nullable = true; // dynamic type system FTW
        fields.push(arrow::datatypes::Field::new(name, ty, nullable));
    }

    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

fn convert_datatype(ty: Type) -> Option<arrow::datatypes::DataType> {
    match ty {
        Type::Integer => Some(arrow::datatypes::DataType::Int64),
        Type::Real => Some(arrow::datatypes::DataType::Float64),
        Type::Text => Some(arrow::datatypes::DataType::LargeUtf8),
        Type::Blob => Some(arrow::datatypes::DataType::LargeBinary),

        // first value was NULL, we cannot infer type of the column
        // TODO: maybe scan more rows in this case?
        Type::Null => None,
    }
}
