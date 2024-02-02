use std::sync::Arc;

use fallible_streaming_iterator::FallibleStreamingIterator;
use fehler::throws;
use log;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::{FromSql, Type};
use rusqlite::{Row, Rows, Statement};

use super::errors::ConnectorError;
use super::transport::ProduceTy;
use super::{data_store::*, transport::Produce};

#[derive(Clone)]
pub struct SQLiteSource {
    pool: Pool<SqliteConnectionManager>,
}

impl SQLiteSource {
    #[throws(ConnectorError)]
    pub fn new(url: &str, max_conn: usize) -> Self {
        let decoded_conn = urlencoding::decode(url)?.into_owned();
        log::debug!("decoded conn: {}", decoded_conn);
        let manager = SqliteConnectionManager::file(decoded_conn);
        let pool = Pool::builder().max_size(max_conn as u32).build(manager)?;

        Self { pool }
    }
}

impl DataStore for SQLiteSource {
    type Conn = SQLiteConnection;

    #[throws(ConnectorError)]
    fn new_connection(&self) -> Self::Conn {
        let conn = self.pool.get()?;
        SQLiteConnection { conn }
    }
}

pub struct SQLiteConnection {
    conn: PooledConnection<SqliteConnectionManager>,
}

impl DataStoreConnection for SQLiteConnection {
    type Task<'conn> = SQLiteTask<'conn> where Self: 'conn;

    #[throws(ConnectorError)]
    fn prepare_task(&mut self, query: &str) -> SQLiteTask {
        let stmt = self.conn.prepare(query)?;
        SQLiteTask { stmt }
    }
}

pub struct SQLiteTask<'conn> {
    stmt: Statement<'conn>,
}

impl<'conn> DataStoreTask<'conn> for SQLiteTask<'conn> {
    type Params = ();
    type Reader<'task> = SQLiteRowsReader<'task> where Self: 'task;

    fn start(&mut self, params: Self::Params) -> Result<Self::Reader<'_>, ConnectorError> {
        let rows = self.stmt.query(params)?;
        let reader = SQLiteRowsReader {
            rows,
            advanced_but_not_consumed: false,
        };
        Ok(reader)
    }
}

pub struct SQLiteRowsReader<'task> {
    rows: Rows<'task>,
    advanced_but_not_consumed: bool,
}

impl<'task> ResultReader<'task> for SQLiteRowsReader<'task> {
    type RowsReader = Self;
    type BatchReader = UnsupportedReader<'task>;

    fn try_into_rows(self) -> Result<Self::RowsReader, Self> {
        Ok(self)
    }

    fn read_until_schema(
        &mut self,
    ) -> Result<Option<Arc<arrow::datatypes::Schema>>, ConnectorError> {
        self.rows.advance()?;
        self.advanced_but_not_consumed = true;
        let first_row: Option<&Row<'_>> = self.rows.get();

        let stmt = self.rows.as_ref();
        let Some(stmt) = stmt else {
            return Ok(None);
        };

        let column_count = stmt.column_count();
        let mut fields = Vec::with_capacity(column_count);

        for (i, col) in stmt.columns().iter().enumerate() {
            let ty_of_first_val = first_row.map(|r| r.get_ref(i).unwrap().data_type());

            let ty = convert_type_with_name(col.decl_type(), ty_of_first_val);

            let Some(ty) = ty else {
                return Ok(None);
            };

            let nullable = true; // dynamic type system FTW

            fields.push(arrow::datatypes::Field::new(col.name(), ty, nullable));
        }

        Ok(Some(Arc::new(arrow::datatypes::Schema::new(fields))))
    }
}

impl<'stmt> RowsReader<'stmt> for SQLiteRowsReader<'stmt> {
    type RowReader<'rows> = SQLiteRowReader<'rows>
    where
        Self: 'rows;

    #[throws(ConnectorError)]
    fn next_row(&mut self) -> Option<Self::RowReader<'_>> {
        let row = if self.advanced_but_not_consumed {
            self.advanced_but_not_consumed = false;
            self.rows.get()
        } else {
            self.rows.next()?
        };
        row.map(|row| SQLiteRowReader { row, next_col: 0 })
    }
}

pub struct SQLiteRowReader<'rows> {
    row: &'rows Row<'rows>,
    next_col: usize,
}

impl<'rows> RowReader<'rows> for SQLiteRowReader<'rows> {
    type CellReader<'row> = SQLCellRef<'row>
    where
        Self: 'row;

    fn next_cell(&mut self) -> Self::CellReader<'_> {
        let col = self.next_col;
        self.next_col += 1;
        (self.row, col)
    }
}

type SQLCellRef<'row> = (&'row Row<'row>, usize);

impl<'r> Produce<'r> for SQLCellRef<'r> {}

impl<'r, T: FromSql> ProduceTy<'r, T> for SQLCellRef<'r> {
    fn produce(&self) -> T {
        self.0.get_unwrap::<usize, T>(self.1)
    }
    fn produce_opt(&self) -> Option<T> {
        self.0.get_unwrap::<usize, Option<T>>(self.1)
    }
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
