//! Source implementation for Google BigQuery

mod errors;
mod typesystem;

pub use self::errors::BigQuerySourceError;
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{Produce, Source, SourceReader, ValueStream},
    sql::{limit1_query, CXQuery},
    typesystem::Schema,
};
use anyhow::anyhow;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use fehler::{throw, throws};
use gcp_bigquery_client::{
    model::{
        get_query_results_parameters::GetQueryResultsParameters,
        get_query_results_response::GetQueryResultsResponse, query_request::QueryRequest,
    },
    Client,
};
use sqlparser::dialect::Dialect;
use std::sync::Arc;
use tokio::runtime::Runtime;
pub use typesystem::BigQueryTypeSystem;
use url::Url;

#[derive(Debug)]
pub struct BigQueryDialect {}

impl Dialect for BigQueryDialect {
    // See https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_' || ch == '-'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit()
    }
}

pub struct BigQuerySource {
    rt: Arc<Runtime>,
    client: Arc<Client>,
    project_id: String,
}

impl BigQuerySource {
    #[throws(BigQuerySourceError)]
    pub fn new(rt: Arc<Runtime>, conn: &str) -> Self {
        let url = Url::parse(conn)?;
        let sa_key_path = url.path();
        let client = Arc::new(rt.block_on(
            gcp_bigquery_client::Client::from_service_account_key_file(sa_key_path),
        ));
        let auth_data = std::fs::read_to_string(sa_key_path)?;
        let auth_json: serde_json::Value = serde_json::from_str(&auth_data)?;
        let project_id = auth_json
            .get("project_id")
            .ok_or_else(|| anyhow!("Cannot get project_id from auth file"))?
            .as_str()
            .ok_or_else(|| anyhow!("Cannot get project_id as string from auth file"))?
            .to_string();
        Self {
            rt,
            client,
            project_id,
        }
    }
}

impl Source for BigQuerySource
where
    BigQueryReader: SourceReader<TypeSystem = BigQueryTypeSystem, Error = BigQuerySourceError>,
{
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Reader = BigQueryReader;
    type TypeSystem = BigQueryTypeSystem;
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn reader(&mut self, query: &CXQuery, data_order: DataOrder) -> Self::Reader {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorXError::UnsupportedDataOrder(data_order));
        }

        BigQueryReader::new(
            self.rt.clone(),
            self.client.clone(),
            self.project_id.clone(),
            query,
        )
    }
}

pub struct BigQueryReader {
    rt: Arc<Runtime>,
    client: Arc<Client>,
    project_id: String,
    query: CXQuery<String>,
}

impl BigQueryReader {
    pub fn new(
        handle: Arc<Runtime>,
        client: Arc<Client>,
        project_id: String,
        query: &CXQuery<String>,
    ) -> Self {
        Self {
            rt: handle,
            client,
            project_id: project_id.clone(),
            query: query.clone(),
        }
    }
}

impl SourceReader for BigQueryReader {
    type TypeSystem = BigQueryTypeSystem;
    type Stream<'a> = BigQueryValueStream;
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn fetch_until_schema(&mut self) -> Schema<Self::TypeSystem> {
        let job = self.client.job();
        let query = &self.query;

        let l1query = limit1_query(query, &BigQueryDialect {})?;
        let rs = self.rt.block_on(job.query(
            self.project_id.as_str(),
            QueryRequest::new(l1query.as_str()),
        ))?;
        let (names, types) = rs
            .query_response()
            .schema
            .as_ref()
            .ok_or_else(|| anyhow!("TableSchema is none"))?
            .fields
            .as_ref()
            .ok_or_else(|| anyhow!("TableFieldSchema is none"))?
            .iter()
            .map(|col| {
                (
                    col.clone().name,
                    BigQueryTypeSystem::from(&col.clone().r#type),
                )
            })
            .unzip();

        Schema { names, types }
    }

    #[throws(BigQuerySourceError)]
    fn value_stream(&mut self, schema: &Schema<BigQueryTypeSystem>) -> Self::Stream<'_> {
        let job = self.client.job();
        let qry = self.rt.block_on(job.query(
            self.project_id.as_str(),
            QueryRequest::new(self.query.as_str()),
        ))?;
        let job_info = qry
            .query_response()
            .job_reference
            .as_ref()
            .ok_or_else(|| anyhow!("job_reference is none"))?;
        let params = GetQueryResultsParameters {
            format_options: None,
            location: job_info.location.clone(),
            max_results: None,
            page_token: None,
            start_index: None,
            timeout_ms: None,
        };
        let rs = self.rt.block_on(
            job.get_query_results(
                self.project_id.as_str(),
                job_info
                    .job_id
                    .as_ref()
                    .ok_or_else(|| anyhow!("job_id is none"))?
                    .as_str(),
                params,
            ),
        )?;
        BigQueryValueStream::new(self.rt.clone(), self.client.clone(), rs, schema)
    }
}

pub struct BigQueryValueStream {
    rt: Arc<Runtime>,
    client: Arc<Client>,
    response: GetQueryResultsResponse,
    ncols: usize,
    current_col: usize,
    current_row: usize,
    nrows: Option<usize>,
}

impl BigQueryValueStream {
    fn new(
        rt: Arc<Runtime>,
        client: Arc<Client>,
        response: GetQueryResultsResponse,
        schema: &Schema<BigQueryTypeSystem>,
    ) -> Self {
        Self {
            rt,
            client,
            response,
            ncols: schema.len(),
            current_row: 0,
            current_col: 0,
            nrows: None,
        }
    }

    #[throws(BigQuerySourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> ValueStream<'a> for BigQueryValueStream {
    type TypeSystem = BigQueryTypeSystem;
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn fetch_batch(&mut self) -> (usize, bool) {
        if self.current_col != 0 {
            return (0, true);
        }
        match self.nrows {
            Some(total_rows) => (total_rows - self.current_row, true),
            None => {
                // Get all number of rows
                let total_rows = self
                    .response
                    .total_rows
                    .as_ref()
                    .ok_or_else(|| anyhow!("total_rows is none"))?
                    .parse::<usize>()?;
                self.nrows = Some(total_rows);
                (total_rows, true)
            }
        }
    }
}

macro_rules! impl_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r> Produce<'r, $t> for BigQueryValueStream {
                type Error = BigQuerySourceError;

                #[throws(BigQuerySourceError)]
                fn produce(&'r mut self) -> $t {
                    let (mut ridx, cidx) = self.next_loc()?;
                    if ridx == (self.response.rows.as_ref().ok_or_else(|| anyhow!("rows is none"))?.len()) {
                        let job = self.client.job();
                        let job_info = self.response.job_reference.as_ref().ok_or_else(|| anyhow!("job_reference is none"))?;
                        let params = GetQueryResultsParameters { format_options: None, location: job_info.location.clone(), max_results: None, page_token: self.response.page_token.clone(), start_index: None, timeout_ms: None };
                        self.response = self.rt.block_on(
                            job.get_query_results(
                                job_info.project_id.as_ref().ok_or_else(|| anyhow!("project_id is none"))?.as_str(),
                                job_info.job_id.as_ref().ok_or_else(|| anyhow!("job_id is none"))?.as_str(),
                                params,
                            ),
                        )?;
                        self.current_row = 0;
                        ridx = 0;
                    }
                    let rows = self.response.rows.as_ref().ok_or_else(|| anyhow!("rows is none"))?;
                    let columns = rows[ridx].columns.as_ref().ok_or_else(|| anyhow!("columns is none"))?;
                    let v = columns.get(cidx).ok_or_else(|| anyhow!("Table Cell is none"))?.value.as_ref().ok_or_else(|| anyhow!("value is none"))?;
                    let s = v
                        .as_str()
                        .ok_or_else(|| anyhow!("cannot get str from json value"))?;
                    s.parse()
                        .map_err(|_| {
                            ConnectorXError::cannot_produce::<$t>(Some(s.into()))
                        })?
                }
            }

            impl<'r> Produce<'r, Option<$t>> for BigQueryValueStream {
                type Error = BigQuerySourceError;

                #[throws(BigQuerySourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (mut ridx, cidx) = self.next_loc()?;
                    if ridx == (self.response.rows.as_ref().ok_or_else(|| anyhow!("rows is none"))?.len()) {
                        let job = self.client.job();
                        let job_info = self.response.job_reference.as_ref().ok_or_else(|| anyhow!("job_reference is none"))?;
                        let params = GetQueryResultsParameters { format_options: None, location: job_info.location.clone(), max_results: None, page_token: self.response.page_token.clone(), start_index: None, timeout_ms: None };
                        self.response = self.rt.block_on(
                            job.get_query_results(
                                job_info.project_id.as_ref().ok_or_else(|| anyhow!("project_id is none"))?.as_str(),
                                job_info.job_id.as_ref().ok_or_else(|| anyhow!("job_id is none"))?.as_str(),
                                params,
                            ),
                        )?;
                        self.current_row = 0;
                        ridx = 0;
                    }
                    let rows = self.response.rows.as_ref().ok_or_else(|| anyhow!("rows is none"))?;
                    let columns = rows[ridx].columns.as_ref().ok_or_else(|| anyhow!("columns is none"))?;
                    match &columns.get(cidx).ok_or_else(|| anyhow!("Table Cell is none"))?.value {
                        None => None,
                        Some(v) => {
                            let s = v.as_str().ok_or_else(|| anyhow!("cannot get str from json value"))?;
                            Some(s.parse().map_err(|_| {
                            ConnectorXError::cannot_produce::<$t>(Some(s.into()))
                        })?)},
                    }
                }
            }
        )+
    };
}

impl_produce!(i64, f64, String,);

impl<'r> Produce<'r, bool> for BigQueryValueStream {
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn produce(&mut self) -> bool {
        let (mut ridx, cidx) = self.next_loc()?;
        if ridx
            == (self
                .response
                .rows
                .as_ref()
                .ok_or_else(|| anyhow!("rows is none"))?
                .len())
        {
            let job = self.client.job();
            let job_info = self
                .response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("job_reference is none"))?;
            let params = GetQueryResultsParameters {
                format_options: None,
                location: job_info.location.clone(),
                max_results: None,
                page_token: self.response.page_token.clone(),
                start_index: None,
                timeout_ms: None,
            };
            self.response = self.rt.block_on(
                job.get_query_results(
                    job_info
                        .project_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("project_id is none"))?
                        .as_str(),
                    job_info
                        .job_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("job_id is none"))?
                        .as_str(),
                    params,
                ),
            )?;
            self.current_row = 0;
            ridx = 0;
        }
        let rows = self
            .response
            .rows
            .as_ref()
            .ok_or_else(|| anyhow!("rows is none"))?;
        let columns = rows[ridx]
            .columns
            .as_ref()
            .ok_or_else(|| anyhow!("columns is none"))?;
        let v = columns
            .get(cidx)
            .ok_or_else(|| anyhow!("Table Cell is none"))?
            .value
            .as_ref()
            .ok_or_else(|| anyhow!("value is none"))?;
        let s = v
            .as_str()
            .ok_or_else(|| anyhow!("cannot get str from json value"))?;

        let ret = match s {
            "true" => true,
            "false" => false,
            _ => throw!(ConnectorXError::cannot_produce::<bool>(Some(s.into()))),
        };
        ret
    }
}

impl<'r> Produce<'r, Option<bool>> for BigQueryValueStream {
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn produce(&mut self) -> Option<bool> {
        let (mut ridx, cidx) = self.next_loc()?;
        if ridx
            == (self
                .response
                .rows
                .as_ref()
                .ok_or_else(|| anyhow!("rows is none"))?
                .len())
        {
            let job = self.client.job();
            let job_info = self
                .response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("job_reference is none"))?;
            let params = GetQueryResultsParameters {
                format_options: None,
                location: job_info.location.clone(),
                max_results: None,
                page_token: self.response.page_token.clone(),
                start_index: None,
                timeout_ms: None,
            };
            self.response = self.rt.block_on(
                job.get_query_results(
                    job_info
                        .project_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("project_id is none"))?
                        .as_str(),
                    job_info
                        .job_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("job_id is none"))?
                        .as_str(),
                    params,
                ),
            )?;
            self.current_row = 0;
            ridx = 0;
        }
        let rows = self
            .response
            .rows
            .as_ref()
            .ok_or_else(|| anyhow!("rows is none"))?;
        let columns = rows[ridx]
            .columns
            .as_ref()
            .ok_or_else(|| anyhow!("columns is none"))?;
        let ret = match &columns
            .get(cidx)
            .ok_or_else(|| anyhow!("Table Cell is none"))?
            .value
        {
            None => None,
            Some(v) => {
                let s = v
                    .as_str()
                    .ok_or_else(|| anyhow!("cannot get str from json value"))?;
                match s {
                    "true" => Some(true),
                    "false" => Some(false),
                    _ => throw!(ConnectorXError::cannot_produce::<bool>(Some(s.into()))),
                }
            }
        };
        ret
    }
}

impl<'r> Produce<'r, NaiveDate> for BigQueryValueStream {
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn produce(&mut self) -> NaiveDate {
        let (mut ridx, cidx) = self.next_loc()?;
        if ridx
            == (self
                .response
                .rows
                .as_ref()
                .ok_or_else(|| anyhow!("rows is none"))?
                .len())
        {
            let job = self.client.job();
            let job_info = self
                .response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("job_reference is none"))?;
            let params = GetQueryResultsParameters {
                format_options: None,
                location: job_info.location.clone(),
                max_results: None,
                page_token: self.response.page_token.clone(),
                start_index: None,
                timeout_ms: None,
            };
            self.response = self.rt.block_on(
                job.get_query_results(
                    job_info
                        .project_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("project_id is none"))?
                        .as_str(),
                    job_info
                        .job_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("job_id is none"))?
                        .as_str(),
                    params,
                ),
            )?;
            self.current_row = 0;
            ridx = 0;
        }
        let rows = self
            .response
            .rows
            .as_ref()
            .ok_or_else(|| anyhow!("rows is none"))?;
        let columns = rows[ridx]
            .columns
            .as_ref()
            .ok_or_else(|| anyhow!("columns is none"))?;
        let v = columns
            .get(cidx)
            .ok_or_else(|| anyhow!("Table Cell is none"))?
            .value
            .as_ref()
            .ok_or_else(|| anyhow!("value is none"))?;
        let s = v
            .as_str()
            .ok_or_else(|| anyhow!("cannot get str from json value"))?;
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| ConnectorXError::cannot_produce::<NaiveDate>(Some(s.into())))?
    }
}

impl<'r> Produce<'r, Option<NaiveDate>> for BigQueryValueStream {
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn produce(&mut self) -> Option<NaiveDate> {
        let (mut ridx, cidx) = self.next_loc()?;
        if ridx
            == (self
                .response
                .rows
                .as_ref()
                .ok_or_else(|| anyhow!("rows is none"))?
                .len())
        {
            let job = self.client.job();
            let job_info = self
                .response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("job_reference is none"))?;
            let params = GetQueryResultsParameters {
                format_options: None,
                location: job_info.location.clone(),
                max_results: None,
                page_token: self.response.page_token.clone(),
                start_index: None,
                timeout_ms: None,
            };
            self.response = self.rt.block_on(
                job.get_query_results(
                    job_info
                        .project_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("project_id is none"))?
                        .as_str(),
                    job_info
                        .job_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("job_id is none"))?
                        .as_str(),
                    params,
                ),
            )?;
            self.current_row = 0;
            ridx = 0;
        }
        let rows = self
            .response
            .rows
            .as_ref()
            .ok_or_else(|| anyhow!("rows is none"))?;
        let columns = rows[ridx]
            .columns
            .as_ref()
            .ok_or_else(|| anyhow!("columns is none"))?;
        match &columns
            .get(cidx)
            .ok_or_else(|| anyhow!("Table Cell is none"))?
            .value
        {
            None => None,
            Some(v) => {
                let s = v
                    .as_str()
                    .ok_or_else(|| anyhow!("cannot get str from json value"))?;
                Some(
                    NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
                        ConnectorXError::cannot_produce::<NaiveDate>(Some(s.into()))
                    })?,
                )
            }
        }
    }
}

impl<'r> Produce<'r, NaiveDateTime> for BigQueryValueStream {
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn produce(&mut self) -> NaiveDateTime {
        let (mut ridx, cidx) = self.next_loc()?;
        if ridx
            == (self
                .response
                .rows
                .as_ref()
                .ok_or_else(|| anyhow!("rows is none"))?
                .len())
        {
            let job = self.client.job();
            let job_info = self
                .response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("job_reference is none"))?;
            let params = GetQueryResultsParameters {
                format_options: None,
                location: job_info.location.clone(),
                max_results: None,
                page_token: self.response.page_token.clone(),
                start_index: None,
                timeout_ms: None,
            };
            self.response = self.rt.block_on(
                job.get_query_results(
                    job_info
                        .project_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("project_id is none"))?
                        .as_str(),
                    job_info
                        .job_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("job_id is none"))?
                        .as_str(),
                    params,
                ),
            )?;
            self.current_row = 0;
            ridx = 0;
        }
        let rows = self
            .response
            .rows
            .as_ref()
            .ok_or_else(|| anyhow!("rows is none"))?;
        let columns = rows[ridx]
            .columns
            .as_ref()
            .ok_or_else(|| anyhow!("columns is none"))?;
        let v = columns
            .get(cidx)
            .ok_or_else(|| anyhow!("Table Cell is none"))?
            .value
            .as_ref()
            .ok_or_else(|| anyhow!("value is none"))?;
        let s = v
            .as_str()
            .ok_or_else(|| anyhow!("cannot get str from json value"))?;
        NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
            .map_err(|_| ConnectorXError::cannot_produce::<NaiveDateTime>(Some(s.into())))?
    }
}

impl<'r> Produce<'r, Option<NaiveDateTime>> for BigQueryValueStream {
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn produce(&mut self) -> Option<NaiveDateTime> {
        let (mut ridx, cidx) = self.next_loc()?;
        if ridx
            == (self
                .response
                .rows
                .as_ref()
                .ok_or_else(|| anyhow!("rows is none"))?
                .len())
        {
            let job = self.client.job();
            let job_info = self
                .response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("job_reference is none"))?;
            let params = GetQueryResultsParameters {
                format_options: None,
                location: job_info.location.clone(),
                max_results: None,
                page_token: self.response.page_token.clone(),
                start_index: None,
                timeout_ms: None,
            };
            self.response = self.rt.block_on(
                job.get_query_results(
                    job_info
                        .project_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("project_id is none"))?
                        .as_str(),
                    job_info
                        .job_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("job_id is none"))?
                        .as_str(),
                    params,
                ),
            )?;
            self.current_row = 0;
            ridx = 0;
        }
        let rows = self
            .response
            .rows
            .as_ref()
            .ok_or_else(|| anyhow!("rows is none"))?;
        let columns = rows[ridx]
            .columns
            .as_ref()
            .ok_or_else(|| anyhow!("columns is none"))?;
        match &columns
            .get(cidx)
            .ok_or_else(|| anyhow!("Table Cell is none"))?
            .value
        {
            None => None,
            Some(v) => {
                let s = v
                    .as_str()
                    .ok_or_else(|| anyhow!("cannot get str from json value"))?;
                Some(
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").map_err(|_| {
                        ConnectorXError::cannot_produce::<NaiveDateTime>(Some(s.into()))
                    })?,
                )
            }
        }
    }
}

impl<'r> Produce<'r, NaiveTime> for BigQueryValueStream {
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn produce(&mut self) -> NaiveTime {
        let (mut ridx, cidx) = self.next_loc()?;
        if ridx
            == (self
                .response
                .rows
                .as_ref()
                .ok_or_else(|| anyhow!("rows is none"))?
                .len())
        {
            let job = self.client.job();
            let job_info = self
                .response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("job_reference is none"))?;
            let params = GetQueryResultsParameters {
                format_options: None,
                location: job_info.location.clone(),
                max_results: None,
                page_token: self.response.page_token.clone(),
                start_index: None,
                timeout_ms: None,
            };
            self.response = self.rt.block_on(
                job.get_query_results(
                    job_info
                        .project_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("project_id is none"))?
                        .as_str(),
                    job_info
                        .job_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("job_id is none"))?
                        .as_str(),
                    params,
                ),
            )?;
            self.current_row = 0;
            ridx = 0;
        }
        let rows = self
            .response
            .rows
            .as_ref()
            .ok_or_else(|| anyhow!("rows is none"))?;
        let columns = rows[ridx]
            .columns
            .as_ref()
            .ok_or_else(|| anyhow!("columns is none"))?;
        let v = columns
            .get(cidx)
            .ok_or_else(|| anyhow!("Table Cell is none"))?
            .value
            .as_ref()
            .ok_or_else(|| anyhow!("value is none"))?;
        let s = v
            .as_str()
            .ok_or_else(|| anyhow!("cannot get str from json value"))?;
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .map_err(|_| ConnectorXError::cannot_produce::<NaiveTime>(Some(s.into())))?
    }
}

impl<'r> Produce<'r, Option<NaiveTime>> for BigQueryValueStream {
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn produce(&mut self) -> Option<NaiveTime> {
        let (mut ridx, cidx) = self.next_loc()?;
        if ridx
            == (self
                .response
                .rows
                .as_ref()
                .ok_or_else(|| anyhow!("rows is none"))?
                .len())
        {
            let job = self.client.job();
            let job_info = self
                .response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("job_reference is none"))?;
            let params = GetQueryResultsParameters {
                format_options: None,
                location: job_info.location.clone(),
                max_results: None,
                page_token: self.response.page_token.clone(),
                start_index: None,
                timeout_ms: None,
            };
            self.response = self.rt.block_on(
                job.get_query_results(
                    job_info
                        .project_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("project_id is none"))?
                        .as_str(),
                    job_info
                        .job_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("job_id is none"))?
                        .as_str(),
                    params,
                ),
            )?;
            self.current_row = 0;
            ridx = 0;
        }
        let rows = self
            .response
            .rows
            .as_ref()
            .ok_or_else(|| anyhow!("rows is none"))?;
        let columns = rows[ridx]
            .columns
            .as_ref()
            .ok_or_else(|| anyhow!("columns is none"))?;
        match &columns
            .get(cidx)
            .ok_or_else(|| anyhow!("Table Cell is none"))?
            .value
        {
            None => None,
            Some(v) => {
                let s = v
                    .as_str()
                    .ok_or_else(|| anyhow!("cannot get str from json value"))?;
                Some(
                    NaiveTime::parse_from_str(s, "%H:%M:%S").map_err(|_| {
                        ConnectorXError::cannot_produce::<NaiveTime>(Some(s.into()))
                    })?,
                )
            }
        }
    }
}

impl<'r> Produce<'r, DateTime<Utc>> for BigQueryValueStream {
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn produce(&mut self) -> DateTime<Utc> {
        let (mut ridx, cidx) = self.next_loc()?;
        if ridx
            == (self
                .response
                .rows
                .as_ref()
                .ok_or_else(|| anyhow!("rows is none"))?
                .len())
        {
            let job = self.client.job();
            let job_info = self
                .response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("job_reference is none"))?;
            let params = GetQueryResultsParameters {
                format_options: None,
                location: job_info.location.clone(),
                max_results: None,
                page_token: self.response.page_token.clone(),
                start_index: None,
                timeout_ms: None,
            };
            self.response = self.rt.block_on(
                job.get_query_results(
                    job_info
                        .project_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("project_id is none"))?
                        .as_str(),
                    job_info
                        .job_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("job_id is none"))?
                        .as_str(),
                    params,
                ),
            )?;
            self.current_row = 0;
            ridx = 0;
        }
        let rows = self
            .response
            .rows
            .as_ref()
            .ok_or_else(|| anyhow!("rows is none"))?;
        let columns = rows[ridx]
            .columns
            .as_ref()
            .ok_or_else(|| anyhow!("columns is none"))?;
        let v = columns
            .get(cidx)
            .ok_or_else(|| anyhow!("Table Cell is none"))?
            .value
            .as_ref()
            .ok_or_else(|| anyhow!("value is none"))?;
        let timestamp_ns = (v
            .as_str()
            .ok_or_else(|| anyhow!("cannot get str from json value"))?
            .parse::<f64>()?
            * 1e9) as i64;
        let secs = timestamp_ns / 1000000000;
        let nsecs = (timestamp_ns % 1000000000) as u32;
        DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDateTime::from_timestamp_opt(secs, nsecs)
                .ok_or_else(|| anyhow!("from_timestamp_opt return None"))?,
            Utc,
        )
    }
}

impl<'r> Produce<'r, Option<DateTime<Utc>>> for BigQueryValueStream {
    type Error = BigQuerySourceError;

    #[throws(BigQuerySourceError)]
    fn produce(&mut self) -> Option<DateTime<Utc>> {
        let (mut ridx, cidx) = self.next_loc()?;
        if ridx
            == (self
                .response
                .rows
                .as_ref()
                .ok_or_else(|| anyhow!("rows is none"))?
                .len())
        {
            let job = self.client.job();
            let job_info = self
                .response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("job_reference is none"))?;
            let params = GetQueryResultsParameters {
                format_options: None,
                location: job_info.location.clone(),
                max_results: None,
                page_token: self.response.page_token.clone(),
                start_index: None,
                timeout_ms: None,
            };
            self.response = self.rt.block_on(
                job.get_query_results(
                    job_info
                        .project_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("project_id is none"))?
                        .as_str(),
                    job_info
                        .job_id
                        .as_ref()
                        .ok_or_else(|| anyhow!("job_id is none"))?
                        .as_str(),
                    params,
                ),
            )?;
            self.current_row = 0;
            ridx = 0;
        }
        let rows = self
            .response
            .rows
            .as_ref()
            .ok_or_else(|| anyhow!("rows is none"))?;
        let columns = rows[ridx]
            .columns
            .as_ref()
            .ok_or_else(|| anyhow!("columns is none"))?;
        match &columns
            .get(cidx)
            .ok_or_else(|| anyhow!("Table Cell is none"))?
            .value
        {
            None => None,
            Some(v) => {
                let timestamp_ns = (v
                    .as_str()
                    .ok_or_else(|| anyhow!("cannot get str from json value"))?
                    .parse::<f64>()?
                    * 1e9) as i64;
                let secs = timestamp_ns / 1000000000;
                let nsecs = (timestamp_ns % 1000000000) as u32;
                NaiveDateTime::from_timestamp_opt(secs, nsecs)
                    .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
            }
        }
    }
}
