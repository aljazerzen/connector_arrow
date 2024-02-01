//! Source implementation for SQLite embedded database.

mod errors;
mod typesystem;

pub use self::errors::SQLiteSourceError;
use crate::{
    data_order::DataOrder,
    sources::{Produce, Source, SourceReader, ValueStream},
    sql::{limit1_query, CXQuery},
    typesystem::Schema,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use log::debug;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Row, Rows, Statement};
use sqlparser::dialect::SQLiteDialect;
use std::{convert::TryFrom, pin::Pin};
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
    const DATA_ORDER: DataOrder = DataOrder::RowMajor;
    type Reader = SQLiteReader;
    type TypeSystem = SQLiteTypeSystem;
    type Error = SQLiteSourceError;

    #[throws(SQLiteSourceError)]
    fn reader(&mut self, query: &CXQuery) -> Self::Reader {
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
    type Stream<'conn> = SQLiteStream<'conn>;
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
    fn value_stream(&mut self, _: &Schema<SQLiteTypeSystem>) -> Self::Stream<'_> {
        SQLiteStream::new(&self.conn, self.query.as_str())?
    }
}

pub struct SQLiteStream<'conn> {
    stmt: Pin<Box<Statement<'conn>>>,
    rows: Option<Pin<Box<Rows<'conn>>>>,
    row: Option<&'conn Row<'conn>>,
    current_col: usize,
}

impl<'conn> SQLiteStream<'conn> {
    #[throws(SQLiteSourceError)]
    pub fn new(conn: &'conn PooledConnection<SqliteConnectionManager>, query: &str) -> Self {
        let stmt: Statement<'conn> = conn.prepare(query)?;

        let mut res = SQLiteStream {
            stmt: Box::pin(stmt),
            rows: None,
            row: None,
            current_col: 0,
        };

        // this is safe, because stmt is on the heap, so pointer to stmt will never change
        let stmt_mut = unsafe {
            // extend the lifetime of reference to stmt
            let stmt: &'conn mut Pin<Box<Statement<'conn>>> = std::mem::transmute(&mut res.stmt);
            // convert the pin into mutable reference
            Pin::get_unchecked_mut(stmt.as_mut())
        };
        let rows = stmt_mut.query([])?;
        res.rows = Some(Box::pin(rows));

        res
    }

    #[throws(SQLiteSourceError)]
    fn next_loc(&mut self) -> (&Row, usize) {
        let col = self.current_col;
        self.current_col += 1;
        (*self.row.as_ref().unwrap(), col)
    }
}

impl<'conn> ValueStream<'conn> for SQLiteStream<'conn> {
    type TypeSystem = SQLiteTypeSystem;
    type Error = SQLiteSourceError;

    fn next_batch(&mut self) -> Result<Option<usize>, Self::Error> {
        // this is safe, because rows are pinned on the heap and will not change address
        let rows = unsafe {
            // this is safe, because rows will always be initialized
            let rows_pin = self.rows.as_mut().unwrap();
            // extend the lifetime of mutable reference to rows
            let rows: &'conn mut Pin<Box<Rows<'conn>>> = std::mem::transmute(rows_pin);
            // convert the pin into mutable reference
            Pin::get_unchecked_mut(rows.as_mut())
        };

        self.row = rows.next()?;
        self.current_col = 0;

        Ok(if self.row.is_some() { Some(1) } else { None })
    }

    fn fetch_batch(&mut self) -> Result<(usize, bool), Self::Error> {
        unreachable!()
    }
}

macro_rules! impl_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r, 'conn> Produce<'r, $t> for SQLiteStream<'conn> {
                type Error = SQLiteSourceError;

                #[throws(SQLiteSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (row, col) = self.next_loc()?;
                    let val = row.get(col)?;
                    val
                }
            }

            impl<'r, 'conn> Produce<'r, Option<$t>> for SQLiteStream<'conn> {
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
