//! Source implementation for DuckDB embedded database.
mod append;
mod schema;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use duckdb::{Appender, Arrow};

use std::sync::Arc;

use crate::api::{Connection, ResultReader, Statement, TableSchema};
use crate::errors::ConnectorError;

impl Connection for duckdb::Connection {
    type Stmt<'conn> = DuckDBStatement<'conn>
    where
        Self: 'conn;

    type Append<'conn> = Appender<'conn> where Self: 'conn;

    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        let stmt = duckdb::Connection::prepare(self, query)?;

        Ok(DuckDBStatement { stmt })
    }

    fn get_table_schemas(&mut self) -> Result<Vec<TableSchema>, ConnectorError> {
        // query table names
        let table_names = {
            let query_tables = "SHOW TABLES;";
            let mut statement = self.prepare(query_tables)?;
            let mut tables_res = statement.query([])?;

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
            let query_schema = format!("SELECT * FROM \"{table_name}\" WHERE FALSE;");
            let mut statement = self.prepare(&query_schema)?;
            let results = statement.query_arrow([])?;

            defs.push(TableSchema {
                name: table_name,
                schema: Schema::clone(&results.get_schema()),
            });
        }

        Ok(defs)
    }
    fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError> {
        Ok(self.appender(table_name)?)
    }
}

pub struct DuckDBStatement<'conn> {
    stmt: duckdb::Statement<'conn>,
}

impl<'conn> Statement<'conn> for DuckDBStatement<'conn> {
    type Params = ();

    type Reader<'stmt> = DuckDBReader<'stmt>
    where
        Self: 'stmt;

    fn start(&mut self, _params: ()) -> Result<Self::Reader<'_>, ConnectorError> {
        let arrow = self.stmt.query_arrow([])?;
        Ok(DuckDBReader { arrow })
    }
}

pub struct DuckDBReader<'stmt> {
    arrow: Arrow<'stmt>,
}

impl<'stmt> ResultReader<'stmt> for DuckDBReader<'stmt> {
    fn get_schema(&mut self) -> Result<Arc<arrow::datatypes::Schema>, ConnectorError> {
        Ok(self.arrow.get_schema())
    }
}

impl<'stmt> Iterator for DuckDBReader<'stmt> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.arrow.next().map(Ok)
    }
}
