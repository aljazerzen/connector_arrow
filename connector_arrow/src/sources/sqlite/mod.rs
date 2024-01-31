//! Source implementation for SQLite embedded database.

mod errors;
mod typesystem;

pub use self::errors::SQLiteSourceError;
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{Produce, Source, SourceReader, ValueStream},
    sql::{limit1_query, CXQuery},
    typesystem::Schema,
    utils::DummyBox,
};
use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fallible_streaming_iterator::FallibleStreamingIterator;
use fehler::{throw, throws};
use log::debug;
use owning_ref::OwningHandle;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Row, Rows, Statement};
use sqlparser::dialect::SQLiteDialect;
use std::convert::TryFrom;
pub use typesystem::SQLiteTypeSystem;
use urlencoding::decode;

pub struct SQLiteSource {
    pool: Pool<SqliteConnectionManager>,
}

impl SQLiteSource {
    #[throws(SQLiteSourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        let decoded_conn = decode(conn)?.into_owned();
        debug!("decoded conn: {}", decoded_conn);
        let manager = SqliteConnectionManager::file(decoded_conn);
        let pool = r2d2::Pool::builder()
            .max_size(nconn as u32)
            .build(manager)?;

        Self { pool }
    }
}

impl Source for SQLiteSource
where
    SQLiteReader: SourceReader<TypeSystem = SQLiteTypeSystem>,
{
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Reader = SQLiteReader;
    type TypeSystem = SQLiteTypeSystem;
    type Error = SQLiteSourceError;

    #[throws(SQLiteSourceError)]
    fn reader(&mut self, query: &CXQuery, data_order: DataOrder) -> Self::Reader {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorXError::UnsupportedDataOrder(data_order));
        }

        let conn = self.pool.get()?;
        SQLiteReader::new(conn, query)
    }
}

pub struct SQLiteReader {
    conn: PooledConnection<SqliteConnectionManager>,
    query: CXQuery<String>,
}

impl SQLiteReader {
    pub fn new(conn: PooledConnection<SqliteConnectionManager>, query: &CXQuery<String>) -> Self {
        Self {
            conn,
            query: query.clone(),
        }
    }
}

impl SourceReader for SQLiteReader {
    type TypeSystem = SQLiteTypeSystem;
    type Stream<'a> = SQLiteSourcePartitionParser<'a>;
    type Error = SQLiteSourceError;

    #[throws(SQLiteSourceError)]
    fn fetch_until_schema(&mut self) -> Schema<Self::TypeSystem> {
        let mut names = vec![];
        let mut types = vec![];

        let l1query = limit1_query(&self.query, &SQLiteDialect {})?;

        let result = self.conn.query_row(l1query.as_str(), [], |row| {
            for (j, col) in row.as_ref().columns().iter().enumerate() {
                if j >= names.len() {
                    names.push(col.name().to_string());
                }
                if j >= types.len() {
                    let vr = row.get_ref(j)?;
                    match SQLiteTypeSystem::try_from((col.decl_type(), vr.data_type())) {
                        Ok(t) => types.push(Some(t)),
                        Err(_) => {
                            types.push(None);
                        }
                    }
                } else if types[j].is_none() {
                    // We didn't get the type in the previous round
                    let vr = row.get_ref(j)?;
                    if let Ok(t) = SQLiteTypeSystem::try_from((col.decl_type(), vr.data_type())) {
                        types[j] = Some(t)
                    }
                }
            }
            Ok(())
        });

        match result {
            Ok(()) => {
                if !types.contains(&None) {
                    let types = types.into_iter().map(|t| t.unwrap()).collect();
                    Schema { names, types }
                } else {
                    debug!(
                        "cannot get metadata for '{}' due to null value: {:?}",
                        self.query, types
                    );
                    throw!(SQLiteSourceError::InferTypeFromNull);
                }
            }
            Err(e) => {
                debug!("cannot get metadata for '{}': {}", self.query, e);
                throw!(e)
            }
        }
    }

    #[throws(SQLiteSourceError)]
    fn value_stream(&mut self, schema: &Schema<SQLiteTypeSystem>) -> Self::Stream<'_> {
        SQLiteSourcePartitionParser::new(&self.conn, self.query.as_str(), schema)?
    }
}

unsafe impl<'a> Send for SQLiteSourcePartitionParser<'a> {}

pub struct SQLiteSourcePartitionParser<'a> {
    rows: OwningHandle<Box<Statement<'a>>, DummyBox<Rows<'a>>>,
    ncols: usize,
    current_col: usize,
    current_consumed: bool,
    is_finished: bool,
}

impl<'a> SQLiteSourcePartitionParser<'a> {
    #[throws(SQLiteSourceError)]
    pub fn new(
        conn: &'a PooledConnection<SqliteConnectionManager>,
        query: &str,
        schema: &Schema<SQLiteTypeSystem>,
    ) -> Self {
        let stmt: Statement<'a> = conn.prepare(query)?;

        // Safety: DummyBox borrows the on-heap stmt, which is owned by the OwningHandle.
        // No matter how we move the owning handle (thus the Box<Statment>), the Statement
        // keeps its address static on the heap, thus the borrow of MyRows keeps valid.
        let rows: OwningHandle<Box<Statement<'a>>, DummyBox<Rows<'a>>> =
            OwningHandle::new_with_fn(Box::new(stmt), |stmt: *const Statement<'a>| unsafe {
                DummyBox((*(stmt as *mut Statement<'_>)).query([]).unwrap())
            });
        Self {
            rows,
            ncols: schema.types.len(),
            current_col: 0,
            current_consumed: true,
            is_finished: false,
        }
    }

    #[throws(SQLiteSourceError)]
    fn next_loc(&mut self) -> (&Row, usize) {
        self.current_consumed = true;
        let row: &Row = (*self.rows)
            .get()
            .ok_or_else(|| anyhow!("Sqlite empty current row"))?;
        let col = self.current_col;
        self.current_col = (self.current_col + 1) % self.ncols;
        (row, col)
    }
}

impl<'a> ValueStream<'a> for SQLiteSourcePartitionParser<'a> {
    type TypeSystem = SQLiteTypeSystem;
    type Error = SQLiteSourceError;

    #[throws(SQLiteSourceError)]
    fn fetch_batch(&mut self) -> (usize, bool) {
        assert!(self.current_col == 0);

        if !self.current_consumed {
            return (1, false);
        } else if self.is_finished {
            return (0, true);
        }

        match (*self.rows).next()? {
            Some(_) => {
                self.current_consumed = false;
                (1, false)
            }
            None => {
                self.is_finished = true;
                (0, true)
            }
        }
    }
}

macro_rules! impl_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r, 'a> Produce<'r, $t> for SQLiteSourcePartitionParser<'a> {
                type Error = SQLiteSourceError;

                #[throws(SQLiteSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (row, col) = self.next_loc()?;
                    let val = row.get(col)?;
                    val
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for SQLiteSourcePartitionParser<'a> {
                type Error = SQLiteSourceError;

                #[throws(SQLiteSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (row, col) = self.next_loc()?;
                    let val = row.get(col)?;
                    val
                }
            }
        )+
    };
}

impl_produce!(
    bool,
    i64,
    i32,
    i16,
    f64,
    Box<str>,
    NaiveDate,
    NaiveTime,
    NaiveDateTime,
    Vec<u8>,
);
