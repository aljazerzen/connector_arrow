use std::sync::Arc;

use fallible_streaming_iterator::FallibleStreamingIterator;
use fehler::throws;
use rusqlite::types::{FromSql, Type};
use rusqlite::{Row, Rows};

use super::api::*;
use super::errors::ConnectorError;
use super::util::arrow_reader::ArrowReader;
use super::util::transport::{Produce, ProduceTy};
use super::util::{collect_rows_to_arrow, CellReader, RowsReader};

impl Connection for rusqlite::Connection {
    type Stmt<'conn> = SQLiteStatement<'conn> where Self: 'conn;

    #[throws(ConnectorError)]
    fn prepare(&mut self, query: &str) -> SQLiteStatement {
        let stmt = rusqlite::Connection::prepare(self, query)?;
        SQLiteStatement { stmt }
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
        let mut rows = self.stmt.query(params)?;

        let schema = read_schema(&mut rows, column_count)?;

        let mut rows = SQLiteRowsReader {
            rows,
            advanced_but_not_consumed: true, // read_schema has advanced the first row
            column_count,
        };
        let batches = collect_rows_to_arrow(schema.clone(), &mut rows, 1024)?;

        Ok(ArrowReader::new(schema, batches))
    }
}

pub struct SQLiteRowsReader<'task> {
    rows: Rows<'task>,
    advanced_but_not_consumed: bool,
    column_count: usize,
}

impl<'stmt> RowsReader<'stmt> for SQLiteRowsReader<'stmt> {
    type CellReader<'rows> = SQLiteCellReader<'rows>
    where
        Self: 'rows;

    #[throws(ConnectorError)]
    fn next_row(&mut self) -> Option<Self::CellReader<'_>> {
        let column_count = self.column_count;
        let row = if self.advanced_but_not_consumed {
            self.advanced_but_not_consumed = false;
            self.rows.get()
        } else {
            self.rows.next()?
        };
        row.map(|row| SQLiteCellReader {
            row,
            next_col: 0,
            column_count,
        })
    }
}

pub struct SQLiteCellReader<'rows> {
    row: &'rows Row<'rows>,
    next_col: usize,
    column_count: usize,
}

impl<'rows> CellReader<'rows> for SQLiteCellReader<'rows> {
    type CellRef<'row> = SQLCellRef<'row>
    where
        Self: 'row;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        if self.next_col == self.column_count {
            None
        } else {
            let col = self.next_col;
            self.next_col += 1;
            Some((self.row, col))
        }
    }
}

type SQLCellRef<'row> = (&'row Row<'row>, usize);

impl<'r> Produce<'r> for SQLCellRef<'r> {}

impl<'r, T: FromSql> ProduceTy<'r, T> for SQLCellRef<'r> {
    #[throws(ConnectorError)]
    fn produce(&self) -> T {
        self.0.get_unwrap::<usize, T>(self.1)
    }
    #[throws(ConnectorError)]
    fn produce_opt(&self) -> Option<T> {
        self.0.get_unwrap::<usize, Option<T>>(self.1)
    }
}

fn read_schema(
    rows: &mut Rows,
    column_count: usize,
) -> Result<Arc<arrow::datatypes::Schema>, ConnectorError> {
    rows.advance()?;
    let first_row: Option<&Row<'_>> = rows.get();

    let stmt = rows.as_ref();
    let Some(stmt) = stmt else {
        return Err(ConnectorError::CannotConvertSchema);
    };

    let mut fields = Vec::with_capacity(column_count);

    for (i, col) in stmt.columns().iter().enumerate() {
        let ty_of_first_val = first_row.map(|r| r.get_ref(i).unwrap().data_type());

        let ty = convert_type_with_name(col.decl_type(), ty_of_first_val);

        let Some(ty) = ty else {
            return Err(ConnectorError::CannotConvertSchema);
        };

        let nullable = true; // dynamic type system FTW

        fields.push(arrow::datatypes::Field::new(col.name(), ty, nullable));
    }

    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

fn convert_type_with_name(
    decl_name: Option<&str>,
    ty_of_first_val: Option<Type>,
) -> Option<arrow::datatypes::DataType> {
    if let Some(ty) = decl_name.and_then(convert_decl_name) {
        return Some(ty);
    }

    if let Some(ty) = ty_of_first_val.and_then(convert_datatype) {
        return Some(ty);
    }

    None
}

fn convert_decl_name(name_hint: &str) -> Option<arrow::datatypes::DataType> {
    // derive from column's declare type, some rules refer to:
    // https://www.sqlite.org/datatype3.html#affname
    let decl_type = name_hint.to_lowercase();

    Some(match decl_type.as_str() {
        "int4" => arrow::datatypes::DataType::Int32,
        "int2" => arrow::datatypes::DataType::Int16,
        "boolean" | "bool" => arrow::datatypes::DataType::Boolean,
        _ if decl_type.contains("int") => arrow::datatypes::DataType::Int64,
        _ if decl_type.contains("char")
            || decl_type.contains("clob")
            || decl_type.contains("text") =>
        {
            arrow::datatypes::DataType::LargeUtf8
        }
        _ if decl_type.contains("real")
            || decl_type.contains("floa")
            || decl_type.contains("doub") =>
        {
            arrow::datatypes::DataType::Float64
        }
        _ if decl_type.contains("blob") => arrow::datatypes::DataType::LargeBinary,
        _ => return None,
    })
}

fn convert_datatype(ty: Type) -> Option<arrow::datatypes::DataType> {
    match ty {
        Type::Integer => Some(arrow::datatypes::DataType::Int8),
        Type::Real => Some(arrow::datatypes::DataType::Float64),
        Type::Text => Some(arrow::datatypes::DataType::LargeUtf8),
        Type::Blob => Some(arrow::datatypes::DataType::LargeBinary),

        // first value was NULL, we cannot infer type of the column
        // TODO: maybe scan more rows in this case?
        Type::Null => None,
    }
}
