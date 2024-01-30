mod errors;
mod typesystem;

pub use self::errors::OracleSourceError;
pub use self::typesystem::OracleTypeSystem;
use crate::constants::{DB_BUFFER_SIZE, ORACLE_ARRAY_SIZE};
use crate::typesystem::Schema;
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{PartitionParser, Produce, Source, SourceReader},
    sql::{count_query, limit1_query_oracle, CXQuery},
    utils::DummyBox,
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use fehler::{throw, throws};
use log::debug;
use owning_ref::OwningHandle;
use r2d2::{Pool, PooledConnection};
use r2d2_oracle::oracle::ResultSet;
use r2d2_oracle::{
    oracle::{Connector, Row, Statement},
    OracleConnectionManager,
};
use sqlparser::dialect::Dialect;
use url::Url;
use urlencoding::decode;

type OracleManager = OracleConnectionManager;
type OracleConn = PooledConnection<OracleManager>;

#[derive(Debug)]
pub struct OracleDialect {}

// implementation copy from AnsiDialect
impl Dialect for OracleDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_'
    }
}

pub struct OracleSource {
    pool: Pool<OracleManager>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    types: Vec<OracleTypeSystem>,
}

#[throws(OracleSourceError)]
pub fn connect_oracle(conn: &Url) -> Connector {
    let user = decode(conn.username())?.into_owned();
    let password = decode(conn.password().unwrap_or(""))?.into_owned();
    let host = decode(conn.host_str().unwrap_or("localhost"))?.into_owned();
    let port = conn.port().unwrap_or(1521);
    let path = decode(conn.path())?.into_owned();

    let conn_str = format!("//{}:{}{}", host, port, path);
    let mut connector = oracle::Connector::new(user.as_str(), password.as_str(), conn_str.as_str());
    if user.is_empty() && password.is_empty() && host == "localhost" {
        debug!("No username or password provided, assuming system auth.");
        connector.external_auth(true);
    }
    connector
}

impl OracleSource {
    #[throws(OracleSourceError)]
    pub fn new(conn: &str, nconn: usize) -> Self {
        let conn = Url::parse(conn)?;
        let connector = connect_oracle(&conn)?;
        let manager = OracleConnectionManager::from_connector(connector);
        let pool = r2d2::Pool::builder()
            .max_size(nconn as u32)
            .build(manager)?;

        Self {
            pool,
            queries: vec![],
            names: vec![],
            types: vec![],
        }
    }
}

impl Source for OracleSource
where
    OracleSourcePartition: SourceReader<TypeSystem = OracleTypeSystem, Error = OracleSourceError>,
{
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Reader = OracleSourcePartition;
    type TypeSystem = OracleTypeSystem;
    type Error = OracleSourceError;

    fn set_queries<Q: ToString + AsRef<str>>(&mut self, queries: &[CXQuery<Q>]) {
        self.queries = queries.iter().map(|q| q.map(Q::to_string)).collect();
    }

    #[throws(OracleSourceError)]
    fn fetch_metadata(&mut self) -> Schema<Self::TypeSystem> {
        assert!(!self.queries.is_empty());

        let conn = self.pool.get()?;
        for (i, query) in self.queries.iter().enumerate() {
            // assuming all the partition queries yield same schema
            // without rownum = 1, derived type might be wrong
            // example: select avg(test_int), test_char from test_table group by test_char
            // -> (NumInt, Char) instead of (NumtFloat, Char)
            match conn.query(limit1_query_oracle(query)?.as_str(), &[]) {
                Ok(rows) => {
                    let (names, types) = rows
                        .column_info()
                        .iter()
                        .map(|col| {
                            (
                                col.name().to_string(),
                                OracleTypeSystem::from(col.oracle_type()),
                            )
                        })
                        .unzip();
                    self.names = names;
                    self.types = types;
                    return Schema {
                        names: self.names.clone(),
                        types: self.types.clone(),
                    };
                }
                Err(e) if i == self.queries.len() - 1 => {
                    // tried the last query but still get an error
                    debug!("cannot get metadata for '{}': {}", query, e);
                    throw!(e);
                }
                Err(_) => {}
            }
        }
        // tried all queries but all get empty result set
        let iter = conn.query(self.queries[0].as_str(), &[])?;
        let (names, types) = iter
            .column_info()
            .iter()
            .map(|col| (col.name().to_string(), OracleTypeSystem::VarChar(false)))
            .unzip();
        self.names = names;
        self.types = types;
        Schema {
            names: self.names.clone(),
            types: self.types.clone(),
        }
    }

    #[throws(OracleSourceError)]
    fn reader(&mut self, query: &CXQuery, data_order: DataOrder) -> Self::Reader {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorXError::UnsupportedDataOrder(data_order));
        }

        let conn = self.pool.get()?;
        OracleSourcePartition::new(conn, query, &self.types)
    }
}

pub struct OracleSourcePartition {
    conn: OracleConn,
    query: CXQuery<String>,
    schema: Vec<OracleTypeSystem>,
    nrows: usize,
    ncols: usize,
}

impl OracleSourcePartition {
    pub fn new(conn: OracleConn, query: &CXQuery<String>, schema: &[OracleTypeSystem]) -> Self {
        Self {
            conn,
            query: query.clone(),
            schema: schema.to_vec(),
            nrows: 0,
            ncols: schema.len(),
        }
    }
}

impl SourceReader for OracleSourcePartition {
    type TypeSystem = OracleTypeSystem;
    type Parser<'a> = OracleTextSourceParser<'a>;
    type Error = OracleSourceError;

    #[throws(OracleSourceError)]
    fn result_rows(&mut self) {
        self.nrows = self
            .conn
            .query_row_as::<usize>(count_query(&self.query, &OracleDialect {})?.as_str(), &[])?;
    }

    #[throws(OracleSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let query = self.query.clone();

        // let iter = self.conn.query(query.as_str(), &[])?;
        OracleTextSourceParser::new(&self.conn, query.as_str(), &self.schema)?
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

unsafe impl<'a> Send for OracleTextSourceParser<'a> {}

pub struct OracleTextSourceParser<'a> {
    rows: OwningHandle<Box<Statement<'a>>, DummyBox<ResultSet<'a, Row>>>,
    rowbuf: Vec<Row>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

impl<'a> OracleTextSourceParser<'a> {
    #[throws(OracleSourceError)]
    pub fn new(conn: &'a OracleConn, query: &str, schema: &[OracleTypeSystem]) -> Self {
        let stmt = conn
            .statement(query)
            .prefetch_rows(ORACLE_ARRAY_SIZE)
            .fetch_array_size(ORACLE_ARRAY_SIZE)
            .build()?;
        let rows: OwningHandle<Box<Statement<'a>>, DummyBox<ResultSet<'a, Row>>> =
            OwningHandle::new_with_fn(Box::new(stmt), |stmt: *const Statement<'a>| unsafe {
                DummyBox((*(stmt as *mut Statement<'_>)).query(&[]).unwrap())
            });

        Self {
            rows,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols: schema.len(),
            current_row: 0,
            current_col: 0,
            is_finished: false,
        }
    }

    #[throws(OracleSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> PartitionParser<'a> for OracleTextSourceParser<'a> {
    type TypeSystem = OracleTypeSystem;
    type Error = OracleSourceError;

    #[throws(OracleSourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        assert!(self.current_col == 0);
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
            if let Some(item) = (*self.rows).next() {
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
            impl<'r, 'a> Produce<'r, $t> for OracleTextSourceParser<'a> {
                type Error = OracleSourceError;

                #[throws(OracleSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].get(cidx)?;
                    res
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for OracleTextSourceParser<'a> {
                type Error = OracleSourceError;

                #[throws(OracleSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].get(cidx)?;
                    res
                }
            }
        )+
    };
}

impl_produce_text!(
    i64,
    f64,
    String,
    NaiveDate,
    NaiveDateTime,
    DateTime<Utc>,
    Vec<u8>,
);
