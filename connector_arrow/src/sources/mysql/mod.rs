//! Source implementation for MySQL database.

mod errors;
mod typesystem;

pub use self::errors::MySQLSourceError;
use crate::constants::DB_BUFFER_SIZE;
use crate::typesystem::Schema;
use crate::{
    data_order::DataOrder,
    sources::{Produce, Source, SourceReader, ValueStream},
    sql::CXQuery,
};
use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use log::debug;
use r2d2::{Pool, PooledConnection};
use r2d2_mysql::{
    mysql::{prelude::Queryable, Binary, Opts, OptsBuilder, QueryResult, Row, Text},
    MySqlConnectionManager,
};
use rust_decimal::Decimal;
use serde_json::Value;
use std::marker::PhantomData;
pub use typesystem::MySQLTypeSystem;

type MysqlConn = PooledConnection<MySqlConnectionManager>;

pub enum BinaryProtocol {}
pub enum TextProtocol {}

pub struct MySQLSource<P> {
    pool: Pool<MySqlConnectionManager>,
    _protocol: PhantomData<P>,
}

impl<P> MySQLSource<P> {
    #[throws(MySQLSourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        let manager = MySqlConnectionManager::new(OptsBuilder::from_opts(Opts::from_url(conn)?));
        let pool = r2d2::Pool::builder()
            .max_size(nconn as u32)
            .build(manager)?;

        Self {
            pool,
            _protocol: PhantomData,
        }
    }
}

impl<P> Source for MySQLSource<P>
where
    MySQLReader<P>: SourceReader<TypeSystem = MySQLTypeSystem, Error = MySQLSourceError>,
    P: Send,
{
    const DATA_ORDER: DataOrder = DataOrder::RowMajor;
    type Reader = MySQLReader<P>;
    type TypeSystem = MySQLTypeSystem;
    type Error = MySQLSourceError;

    #[throws(MySQLSourceError)]
    fn reader(&mut self, query: &CXQuery) -> Self::Reader {
        let conn = self.pool.get()?;
        MySQLReader::new(conn, query)
    }
}

pub struct MySQLReader<P> {
    conn: MysqlConn,
    query: CXQuery<String>,
    _protocol: PhantomData<P>,
}

impl<P> MySQLReader<P> {
    pub fn new(conn: MysqlConn, query: &CXQuery<String>) -> Self {
        Self {
            conn,
            query: query.clone(),
            _protocol: PhantomData,
        }
    }

    #[throws(MySQLSourceError)]
    fn fetch_metadata_generic(&mut self) -> Schema<MySQLTypeSystem> {
        let first_query = &self.query;

        match self.conn.prep(first_query) {
            Ok(stmt) => {
                let (names, types) = stmt
                    .columns()
                    .iter()
                    .map(|col| {
                        (
                            col.name_str().to_string(),
                            MySQLTypeSystem::from((&col.column_type(), &col.flags())),
                        )
                    })
                    .unzip();

                Schema { names, types }
            }
            Err(e) => {
                debug!(
                    "cannot get metadata for '{}', try next query: {}",
                    self.query, e
                );
                throw!(e)
            }
        }
    }
}

impl SourceReader for MySQLReader<BinaryProtocol> {
    type TypeSystem = MySQLTypeSystem;
    type Stream<'a> = MySQLBinaryStream<'a>;
    type Error = MySQLSourceError;

    fn fetch_until_schema(&mut self) -> Result<Schema<Self::TypeSystem>, Self::Error> {
        self.fetch_metadata_generic()
    }

    #[throws(MySQLSourceError)]
    fn value_stream(&mut self, schema: &Schema<MySQLTypeSystem>) -> Self::Stream<'_> {
        let stmt = self.conn.prep(self.query.as_str())?;
        let iter = self.conn.exec_iter(stmt, ())?;
        MySQLBinaryStream::new(iter, schema)
    }
}

impl SourceReader for MySQLReader<TextProtocol> {
    type TypeSystem = MySQLTypeSystem;
    type Stream<'a> = MySQLTextStream<'a>;
    type Error = MySQLSourceError;

    fn fetch_until_schema(&mut self) -> Result<Schema<Self::TypeSystem>, Self::Error> {
        self.fetch_metadata_generic()
    }

    #[throws(MySQLSourceError)]
    fn value_stream(&mut self, schema: &Schema<MySQLTypeSystem>) -> Self::Stream<'_> {
        let query = self.query.clone();
        let iter = self.conn.query_iter(query)?;
        MySQLTextStream::new(iter, schema)
    }
}

pub struct MySQLBinaryStream<'a> {
    iter: QueryResult<'a, 'a, 'a, Binary>,
    rowbuf: Vec<Row>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

impl<'a> MySQLBinaryStream<'a> {
    pub fn new(iter: QueryResult<'a, 'a, 'a, Binary>, schema: &Schema<MySQLTypeSystem>) -> Self {
        Self {
            iter,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols: schema.len(),
            current_row: 0,
            current_col: 0,
            is_finished: false,
        }
    }

    #[throws(MySQLSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> ValueStream<'a> for MySQLBinaryStream<'a> {
    type TypeSystem = MySQLTypeSystem;
    type Error = MySQLSourceError;

    #[throws(MySQLSourceError)]
    fn fetch_batch(&mut self) -> (usize, bool) {
        assert!(self.current_col == 0);
        if self.is_finished {
            return (0, true);
        }

        let remaining_rows = self.rowbuf.len() - self.current_row;
        if remaining_rows > 0 {
            return (remaining_rows, self.is_finished);
        } else if self.is_finished {
            return (0, self.is_finished);
        }

        if !self.rowbuf.is_empty() {
            self.rowbuf.drain(..);
        }

        for _ in 0..DB_BUFFER_SIZE {
            if let Some(item) = self.iter.next() {
                self.rowbuf.push(item?);
            } else {
                self.is_finished = true;
                break;
            }
        }
        self.current_row = 0;
        self.current_col = 0;

        (self.rowbuf.len(), self.is_finished)
    }
}

macro_rules! impl_produce_binary {
    ($($t: ty,)+) => {
        $(
            impl<'r, 'a> Produce<'r, $t> for MySQLBinaryStream<'a> {
                type Error = MySQLSourceError;

                #[throws(MySQLSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].take(cidx).ok_or_else(|| anyhow!("mysql cannot parse at position: ({}, {})", ridx, cidx))?;
                    res
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for MySQLBinaryStream<'a> {
                type Error = MySQLSourceError;

                #[throws(MySQLSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].take(cidx).ok_or_else(|| anyhow!("mysql cannot parse at position: ({}, {})", ridx, cidx))?;
                    res
                }
            }
        )+
    };
}

impl_produce_binary!(
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    NaiveDate,
    NaiveTime,
    NaiveDateTime,
    Decimal,
    String,
    Vec<u8>,
    Value,
);

pub struct MySQLTextStream<'a> {
    iter: QueryResult<'a, 'a, 'a, Text>,
    rowbuf: Vec<Row>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

impl<'a> MySQLTextStream<'a> {
    pub fn new(iter: QueryResult<'a, 'a, 'a, Text>, schema: &Schema<MySQLTypeSystem>) -> Self {
        Self {
            iter,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols: schema.len(),
            current_row: 0,
            current_col: 0,
            is_finished: false,
        }
    }

    #[throws(MySQLSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> ValueStream<'a> for MySQLTextStream<'a> {
    type TypeSystem = MySQLTypeSystem;
    type Error = MySQLSourceError;

    #[throws(MySQLSourceError)]
    fn fetch_batch(&mut self) -> (usize, bool) {
        assert!(self.current_col == 0);
        if self.is_finished {
            return (0, true);
        }

        let remaining_rows = self.rowbuf.len() - self.current_row;
        if remaining_rows > 0 {
            return (remaining_rows, self.is_finished);
        } else if self.is_finished {
            return (0, self.is_finished);
        }

        if !self.rowbuf.is_empty() {
            self.rowbuf.drain(..);
        }
        for _ in 0..DB_BUFFER_SIZE {
            if let Some(item) = self.iter.next() {
                self.rowbuf.push(item?);
            } else {
                self.is_finished = true;
                break;
            }
        }
        self.current_row = 0;
        self.current_col = 0;
        (self.rowbuf.len(), self.is_finished)
    }
}

macro_rules! impl_produce_text {
    ($($t: ty,)+) => {
        $(
            impl<'r, 'a> Produce<'r, $t> for MySQLTextStream<'a> {
                type Error = MySQLSourceError;

                #[throws(MySQLSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].take(cidx).ok_or_else(|| anyhow!("mysql cannot parse at position: ({}, {})", ridx, cidx))?;
                    res
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for MySQLTextStream<'a> {
                type Error = MySQLSourceError;

                #[throws(MySQLSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].take(cidx).ok_or_else(|| anyhow!("mysql cannot parse at position: ({}, {})", ridx, cidx))?;
                    res
                }
            }
        )+
    };
}

impl_produce_text!(
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    NaiveDate,
    NaiveTime,
    NaiveDateTime,
    Decimal,
    String,
    Vec<u8>,
    Value,
);
