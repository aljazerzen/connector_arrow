use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
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

    fn get_relation_defs(&mut self) -> Result<Vec<RelationDef>, ConnectorError> {
        // query table names
        let table_names = {
            let query_tables = "SELECT name FROM sqlite_master WHERE type = 'table';";
            let mut statement = rusqlite::Connection::prepare(self, query_tables)?;
            let mut tables_res = statement.query(())?;

            let mut table_names = Vec::new();
            while let Some(row) = tables_res.next()? {
                let table_name: String = row.get(0)?;
                table_names.push(table_name);
            }
            table_names
        };

        // for each table
        let mut defs = Vec::with_capacity(table_names.len());
        for table_name in table_names {
            let query_columns = format!("PRAGMA table_info(\"{}\");", table_name);
            let mut statement = rusqlite::Connection::prepare(self, &query_columns)?;
            let mut columns_res = statement.query(())?;
            // contains columns: cid, name, type, notnull, dflt_value, pk

            let mut fields = Vec::new();
            while let Some(row) = columns_res.next()? {
                let name: String = row.get(1)?;
                let ty: String = row.get(2)?;
                let not_null: bool = row.get(3)?;

                let ty = convert_decl_type(&ty, &name, &table_name)?;
                fields.push(Field::new(name, ty, !not_null));
            }

            defs.push(RelationDef {
                name: table_name,
                schema: Schema::new(fields),
            })
        }

        Ok(defs)
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

fn convert_datatype(ty: Type) -> Option<DataType> {
    match ty {
        Type::Integer => Some(DataType::Int64),
        Type::Real => Some(DataType::Float64),
        Type::Text => Some(DataType::LargeUtf8),
        Type::Blob => Some(DataType::LargeBinary),

        // first value was NULL, we cannot infer type of the column
        // TODO: maybe scan more rows in this case?
        Type::Null => None,
    }
}

fn convert_decl_type(decl_ty: &str, col: &str, table: &str) -> Result<DataType, ConnectorError> {
    // SQLite does not have a "required" column type, only "suggest" column type,
    // known a column type affinity. This function takes this affinity and tries to
    // match it to [DataType], but:
    // a) one could make a column with affinity TEXT, but then store INTEGER in it,
    // b) one could declare a column with arbitrary data type that does not map into any affinity,
    // c) NUMERIC affinity can either be INT or REAL

    // See: https://sqlite.org/datatype3.html#determination_of_column_affinity
    let ty = decl_ty.to_ascii_uppercase();
    if ty.contains("INT") {
        return Ok(DataType::Int64);
    }

    if ty.contains("CHAR") || ty.contains("CLOB") || ty.contains("TEXT") {
        return Ok(DataType::LargeUtf8);
    }

    if ty.contains("BLOB") {
        return Ok(DataType::LargeBinary);
    }

    if ty.contains("REAL") || ty.contains("FLOA") || ty.contains("DOUB") {
        return Ok(DataType::Float64);
    }

    Err(ConnectorError::IncompatibleSchema {
        table_name: table.to_string(),
        message: format!("column `{col}` was declared as `{ty}`, which results in `NUMERIC` affinity, which is not supported."),
        hint: Some("Supported types are INTEGER, REAL, TEXT and BLOB".to_string())
    })
}
