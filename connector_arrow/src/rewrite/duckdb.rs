//! Source implementation for DuckDB embedded database.

use arrow::record_batch::RecordBatch;
use duckdb::{Arrow, DuckdbConnectionManager};
use fehler::throws;
use log::debug;
use r2d2::{Pool, PooledConnection};
use std::sync::Arc;
use urlencoding::decode;

use super::api::{Connection, DataStore, ResultReader, Statement};
use super::errors::ConnectorError;

#[derive(Clone)]
pub struct DuckDBDataStore {
    pool: Pool<DuckdbConnectionManager>,
}

impl DuckDBDataStore {
    #[throws(ConnectorError)]
    pub fn new(connection_url: &str, nconn: usize) -> Self {
        let decoded_conn = decode(connection_url)?.into_owned();
        debug!("decoded conn: {}", decoded_conn);

        let manager = DuckdbConnectionManager::file(decoded_conn)?;

        let pool = r2d2::Pool::builder()
            .max_size(nconn as u32)
            .build(manager)?;

        Self { pool }
    }
}

impl DataStore for DuckDBDataStore {
    type Conn = DuckDBConnection;

    fn new_connection(&self) -> Result<Self::Conn, ConnectorError> {
        let conn = self.pool.get()?;
        Ok(DuckDBConnection { conn })
    }
}

pub struct DuckDBConnection {
    conn: PooledConnection<DuckdbConnectionManager>,
}

impl Connection for DuckDBConnection {
    type Stmt<'conn> = DuckDBStatement<'conn>
    where
        Self: 'conn;

    fn prepare_task<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        let stmt = self.conn.prepare(query)?;

        Ok(DuckDBStatement { stmt })
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
    #[throws(ConnectorError)]
    fn read_until_schema(&mut self) -> Arc<arrow::datatypes::Schema> {
        self.arrow.get_schema()
    }
}

impl<'stmt> Iterator for DuckDBReader<'stmt> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.arrow.next().map(Ok)
    }
}
