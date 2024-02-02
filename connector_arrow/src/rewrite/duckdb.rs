//! Source implementation for DuckDB embedded database.

use arrow::record_batch::RecordBatch;
use duckdb::{Arrow, DuckdbConnectionManager, Statement};
use fehler::throws;
use log::debug;
use r2d2::{Pool, PooledConnection};
use std::{string::FromUtf8Error, sync::Arc};
use thiserror::Error;
use urlencoding::decode;

use super::data_store::{
    BatchReader, DataStore, DataStoreConnection, DataStoreTask, ResultReader, UnsupportedReader,
};

#[derive(Clone)]
pub struct DuckDBDataStore {
    pool: Pool<DuckdbConnectionManager>,
}

#[derive(Error, Debug)]
pub enum DuckDBError {
    #[error("Cannot infer type from null for DuckDb")]
    InferTypeFromNull,

    #[error(transparent)]
    ConnectorXError(#[from] crate::errors::ConnectorXError),

    #[error(transparent)]
    DuckDbError(#[from] duckdb::Error),

    #[error(transparent)]
    DuckDbPoolError(#[from] r2d2::Error),

    #[error(transparent)]
    DuckDbUrlDecodeError(#[from] FromUtf8Error),

    /// Any other errors that are too trivial to be put here explicitly.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl DuckDBDataStore {
    #[throws(DuckDBError)]
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
    type Error = DuckDBError;

    type Conn = DuckDBConnection;

    fn new_connection(&self) -> Result<Self::Conn, Self::Error> {
        let conn = self.pool.get()?;
        Ok(DuckDBConnection { conn })
    }
}

pub struct DuckDBConnection {
    conn: PooledConnection<DuckdbConnectionManager>,
}

impl DataStoreConnection for DuckDBConnection {
    type Error = DuckDBError;

    type Task<'conn> = DuckDBTask<'conn>
    where
        Self: 'conn;

    fn prepare_task<'a>(&'a mut self, query: &str) -> Result<Self::Task<'a>, Self::Error> {
        let stmt = self.conn.prepare(query)?;

        Ok(DuckDBTask { stmt })
    }
}

pub struct DuckDBTask<'conn> {
    stmt: Statement<'conn>,
}

impl<'conn> DataStoreTask<'conn> for DuckDBTask<'conn> {
    type Error = DuckDBError;

    type Params = ();

    type Reader<'task> = DuckDBReader<'task>
    where
        Self: 'task;

    fn start(&mut self, _params: ()) -> Result<Self::Reader<'_>, Self::Error> {
        let arrow = self.stmt.query_arrow([])?;
        Ok(DuckDBReader { arrow })
    }
}

pub struct DuckDBReader<'task> {
    arrow: Arrow<'task>,
}

impl<'task> ResultReader<'task> for DuckDBReader<'task> {
    type Error = DuckDBError;
    type RowsReader = UnsupportedReader<'task, DuckDBError>;
    type BatchReader = Self;

    fn read_until_schema(&mut self) -> Result<Option<Arc<arrow::datatypes::Schema>>, Self::Error> {
        let schema = self.arrow.get_schema();
        Ok(Some(schema))
    }

    fn try_into_batch(self) -> Result<Self::BatchReader, Self> {
        Ok(self)
    }
}

impl<'task> BatchReader<'task> for DuckDBReader<'task> {}

impl<'task> Iterator for DuckDBReader<'task> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.arrow.next()
    }
}
