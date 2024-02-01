//! Source implementation for SQL Server.

mod errors;
mod typesystem;

pub use self::errors::MsSQLSourceError;
pub use self::typesystem::{FloatN, IntN, MsSQLTypeSystem};
use crate::constants::DB_BUFFER_SIZE;
use crate::typesystem::Schema;
use crate::{
    data_order::DataOrder,
    sources::{Produce, Source, SourceReader, ValueStream},
    sql::CXQuery,
    utils::DummyBox,
};
use anyhow::anyhow;
use bb8::{Pool, PooledConnection};
use bb8_tiberius::ConnectionManager;
use chrono::{DateTime, Utc};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use futures::StreamExt;
use log::debug;
use owning_ref::OwningHandle;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tiberius::{AuthMethod, Config, EncryptionLevel, QueryResult, Row};
use tokio::runtime::{Handle, Runtime};
use url::Url;
use urlencoding::decode;
use uuid::Uuid;

type Conn<'a> = PooledConnection<'a, ConnectionManager>;
pub struct MsSQLSource {
    rt: Arc<Runtime>,
    pool: Pool<ConnectionManager>,
}

#[throws(MsSQLSourceError)]
pub fn mssql_config(url: &Url) -> Config {
    let mut config = Config::new();

    let host = decode(url.host_str().unwrap_or("localhost"))?.into_owned();
    let hosts: Vec<&str> = host.split('\\').collect();
    match hosts.len() {
        1 => config.host(host),
        2 => {
            // SQL Server support instance name: `server\instance:port`
            config.host(hosts[0]);
            config.instance_name(hosts[1]);
        }
        _ => throw!(anyhow!("MsSQL hostname parse error: {}", host)),
    }
    config.port(url.port().unwrap_or(1433));
    // remove the leading "/"
    config.database(&url.path()[1..]);
    // Using SQL Server authentication.
    #[allow(unused)]
    let params: HashMap<String, String> = url.query_pairs().into_owned().collect();
    #[cfg(any(windows, feature = "integrated-auth-gssapi"))]
    match params.get("trusted_connection") {
        // pefer trusted_connection if set to true
        Some(v) if v == "true" => {
            debug!("mssql auth through trusted connection!");
            config.authentication(AuthMethod::Integrated);
        }
        _ => {
            debug!("mssql auth through sqlserver authentication");
            config.authentication(AuthMethod::sql_server(
                decode(url.username())?.to_owned(),
                decode(url.password().unwrap_or(""))?.to_owned(),
            ));
        }
    };
    #[cfg(all(not(windows), not(feature = "integrated-auth-gssapi")))]
    config.authentication(AuthMethod::sql_server(
        decode(url.username())?,
        decode(url.password().unwrap_or(""))?,
    ));

    match params.get("encrypt") {
        Some(v) if v.to_lowercase() == "true" => config.encryption(EncryptionLevel::Required),
        _ => config.encryption(EncryptionLevel::NotSupported),
    };

    if let Some(appname) = params.get("appname") {
        config.application_name(decode(appname)?)
    };

    config
}

impl MsSQLSource {
    #[throws(MsSQLSourceError)]
    pub fn new(rt: Arc<Runtime>, conn: &str, nconn: usize) -> Self {
        let url = Url::parse(conn)?;
        let config = mssql_config(&url)?;
        let manager = bb8_tiberius::ConnectionManager::new(config);
        let pool = rt.block_on(Pool::builder().max_size(nconn as u32).build(manager))?;

        Self { rt, pool }
    }
}

impl Source for MsSQLSource
where
    MsSQLReader: SourceReader<TypeSystem = MsSQLTypeSystem, Error = MsSQLSourceError>,
{
    const DATA_ORDER: DataOrder = DataOrder::RowMajor;
    type Reader = MsSQLReader;
    type TypeSystem = MsSQLTypeSystem;
    type Error = MsSQLSourceError;

    #[throws(MsSQLSourceError)]
    fn reader(&mut self, query: &CXQuery) -> Self::Reader {
        MsSQLReader::new(self.pool.clone(), self.rt.clone(), query)
    }
}

pub struct MsSQLReader {
    pool: Pool<ConnectionManager>,
    rt: Arc<Runtime>,
    query: CXQuery<String>,
}

impl MsSQLReader {
    pub fn new(
        pool: Pool<ConnectionManager>,
        handle: Arc<Runtime>,
        query: &CXQuery<String>,
    ) -> Self {
        Self {
            rt: handle,
            pool,
            query: query.clone(),
        }
    }
}

impl SourceReader for MsSQLReader {
    type TypeSystem = MsSQLTypeSystem;
    type Stream<'a> = MsSQLStream<'a>;
    type Error = MsSQLSourceError;

    #[throws(MsSQLSourceError)]
    fn fetch_until_schema(&mut self) -> Schema<Self::TypeSystem> {
        let mut conn = self.rt.block_on(self.pool.get())?;
        let first_query = &self.query;
        let (names, types) = match self.rt.block_on(conn.query(first_query.as_str(), &[])) {
            Ok(stream) => {
                let columns = stream.columns().ok_or_else(|| {
                    anyhow!("MsSQL failed to get the columns of query: {}", first_query)
                })?;
                columns
                    .iter()
                    .map(|col| {
                        (
                            col.name().to_string(),
                            MsSQLTypeSystem::from(&col.column_type()),
                        )
                    })
                    .unzip()
            }
            Err(e) => {
                // tried the last query but still get an error
                debug!(
                    "cannot get metadata for '{}', try next query: {}",
                    first_query, e
                );
                throw!(e);
            }
        };

        Schema { names, types }
    }

    #[throws(MsSQLSourceError)]
    fn value_stream<'a>(&'a mut self, schema: &Schema<MsSQLTypeSystem>) -> Self::Stream<'a> {
        let conn = self.rt.block_on(self.pool.get())?;
        let rows: OwningHandle<Box<Conn<'a>>, DummyBox<QueryResult<'a>>> =
            OwningHandle::new_with_fn(Box::new(conn), |conn: *const Conn<'a>| unsafe {
                let conn = &mut *(conn as *mut Conn<'a>);

                DummyBox(
                    self.rt
                        .block_on(conn.query(self.query.as_str(), &[]))
                        .unwrap(),
                )
            });

        MsSQLStream::new(self.rt.handle(), rows, schema)
    }
}

pub struct MsSQLStream<'a> {
    rt: &'a Handle,
    iter: OwningHandle<Box<Conn<'a>>, DummyBox<QueryResult<'a>>>,
    rowbuf: Vec<Row>,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    is_finished: bool,
}

impl<'a> MsSQLStream<'a> {
    fn new(
        rt: &'a Handle,
        iter: OwningHandle<Box<Conn<'a>>, DummyBox<QueryResult<'a>>>,
        schema: &Schema<MsSQLTypeSystem>,
    ) -> Self {
        Self {
            rt,
            iter,
            rowbuf: Vec::with_capacity(DB_BUFFER_SIZE),
            ncols: schema.len(),
            current_row: 0,
            current_col: 0,
            is_finished: false,
        }
    }

    #[throws(MsSQLSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> ValueStream<'a> for MsSQLStream<'a> {
    type TypeSystem = MsSQLTypeSystem;
    type Error = MsSQLSourceError;

    #[throws(MsSQLSourceError)]
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
            if let Some(item) = self.rt.block_on(self.iter.next()) {
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

macro_rules! impl_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r, 'a> Produce<'r, $t> for MsSQLStream<'a> {
                type Error = MsSQLSourceError;

                #[throws(MsSQLSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].get(cidx).ok_or_else(|| anyhow!("MsSQL get None at position: ({}, {})", ridx, cidx))?;
                    res
                }
            }

            impl<'r, 'a> Produce<'r, Option<$t>> for MsSQLStream<'a> {
                type Error = MsSQLSourceError;

                #[throws(MsSQLSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let res = self.rowbuf[ridx].get(cidx);
                    res
                }
            }
        )+
    };
}

impl_produce!(
    u8,
    i16,
    i32,
    i64,
    IntN,
    f32,
    f64,
    FloatN,
    bool,
    &'r str,
    &'r [u8],
    Uuid,
    Decimal,
    NaiveDateTime,
    NaiveDate,
    NaiveTime,
    DateTime<Utc>,
);
