//! Source implementation for CSV files.

mod errors;
mod typesystem;

pub use self::errors::CSVSourceError;
pub use self::typesystem::CSVTypeSystem;
use super::{Produce, Source, SourceReader, ValueStream};
use crate::typesystem::Schema;
use crate::{data_order::DataOrder, errors::ConnectorXError, sql::CXQuery};
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use fehler::{throw, throws};
use regex::{Regex, RegexBuilder};
use std::collections::HashSet;
use std::fs::File;

pub struct CSVSource {
    types_override: Option<Vec<CSVTypeSystem>>,
}

impl CSVSource {
    pub fn new(types_override: Option<&[CSVTypeSystem]>) -> Self {
        CSVSource {
            types_override: types_override.map(|x| x.to_vec()),
        }
    }
}

impl Source for CSVSource {
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Reader = CSVReader;
    type TypeSystem = CSVTypeSystem;
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn reader(&mut self, query: &CXQuery, data_order: DataOrder) -> Self::Reader {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorXError::UnsupportedDataOrder(data_order))
        }

        CSVReader::new(query, self.types_override.clone())?
    }
}

pub struct CSVReader {
    types_override: Option<Vec<CSVTypeSystem>>,
    filepath: String,
    counter: usize,
    records: Option<Vec<csv::StringRecord>>,
}

impl CSVReader {
    #[throws(CSVSourceError)]
    pub fn new(filepath: &CXQuery<String>, types_override: Option<Vec<CSVTypeSystem>>) -> Self {
        let filepath = filepath.to_string();

        Self {
            types_override,
            filepath,
            records: None,
            counter: 0,
        }
    }

    #[throws(CSVSourceError)]
    fn read(&self) -> Vec<csv::StringRecord> {
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(File::open(&self.filepath)?);
        let mut records = vec![];
        reader
            .into_records()
            .try_for_each(|v| -> Result<(), CSVSourceError> {
                records.push(v.map_err(|e| anyhow!(e))?);
                Ok(())
            })?;

        records
    }
}

#[throws(CSVSourceError)]
fn infer_schema(reader: &mut csv::Reader<File>, num_cols: usize) -> Vec<CSVTypeSystem> {
    // regular expressions for infer CSVTypeSystem from string
    let decimal_re: Regex = Regex::new(r"^-?(\d+\.\d+)$")?;
    let integer_re: Regex = Regex::new(r"^-?(\d+)$")?;
    let boolean_re: Regex = RegexBuilder::new(r"^(true)$|^(false)$")
        .case_insensitive(true)
        .build()?;
    let datetime_re: Regex = Regex::new(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d$")?;

    let max_records_to_read = 50;

    let mut column_types: Vec<HashSet<CSVTypeSystem>> = vec![HashSet::new(); num_cols];
    let mut nulls: Vec<bool> = vec![false; num_cols];

    let mut record = csv::StringRecord::new();

    for _record_counter in 0..max_records_to_read {
        if !reader.read_record(&mut record)? {
            break;
        }
        for field_counter in 0..num_cols {
            if let Some(string) = record.get(field_counter) {
                if string.is_empty() {
                    nulls[field_counter] = true;
                } else {
                    let dt: CSVTypeSystem;

                    if string.starts_with('"') {
                        dt = CSVTypeSystem::String(false);
                    } else if boolean_re.is_match(string) {
                        dt = CSVTypeSystem::Bool(false);
                    } else if decimal_re.is_match(string) {
                        dt = CSVTypeSystem::F64(false);
                    } else if integer_re.is_match(string) {
                        dt = CSVTypeSystem::I64(false);
                    } else if datetime_re.is_match(string) {
                        dt = CSVTypeSystem::DateTime(false);
                    } else {
                        dt = CSVTypeSystem::String(false);
                    }
                    column_types[field_counter].insert(dt);
                }
            }
        }
    }

    // determine CSVTypeSystem based on possible candidates
    let mut schema = vec![];

    for field_counter in 0..num_cols {
        let possibilities = &column_types[field_counter];
        let has_nulls = nulls[field_counter];

        match possibilities.len() {
            1 => {
                for dt in possibilities.iter() {
                    match *dt {
                        CSVTypeSystem::I64(false) => {
                            schema.push(CSVTypeSystem::I64(has_nulls));
                        }
                        CSVTypeSystem::F64(false) => {
                            schema.push(CSVTypeSystem::F64(has_nulls));
                        }
                        CSVTypeSystem::Bool(false) => {
                            schema.push(CSVTypeSystem::Bool(has_nulls));
                        }
                        CSVTypeSystem::String(false) => {
                            schema.push(CSVTypeSystem::String(has_nulls));
                        }
                        CSVTypeSystem::DateTime(false) => {
                            schema.push(CSVTypeSystem::DateTime(has_nulls));
                        }
                        _ => {}
                    }
                }
            }
            2 => {
                if possibilities.contains(&CSVTypeSystem::I64(false))
                    && possibilities.contains(&CSVTypeSystem::F64(false))
                {
                    // Integer && Float -> Float
                    schema.push(CSVTypeSystem::F64(has_nulls));
                } else {
                    // Conflicting CSVTypeSystems -> String
                    schema.push(CSVTypeSystem::String(has_nulls));
                }
            }
            _ => {
                // Conflicting CSVTypeSystems -> String
                schema.push(CSVTypeSystem::String(has_nulls));
            }
        }
    }
    schema
}

impl SourceReader for CSVReader {
    type TypeSystem = CSVTypeSystem;
    type Stream<'a> = CSVSourcePartitionParser<'a>;
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn fetch_until_schema(&mut self) -> Schema<Self::TypeSystem> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(File::open(&self.filepath)?);
        let header = reader.headers()?;

        let names: Vec<String> = header.iter().map(|s| s.to_string()).collect();
        let types = if let Some(t) = self.types_override.clone() {
            t
        } else {
            infer_schema(&mut reader, names.len())?
        };

        assert_eq!(names.len(), types.len());
        Schema { names, types }
    }

    #[throws(CSVSourceError)]
    fn value_stream(&mut self, schema: &Schema<CSVTypeSystem>) -> Self::Stream<'_> {
        self.records = Some(self.read()?);

        CSVSourcePartitionParser {
            records: self.records.as_mut().unwrap(),
            counter: &mut self.counter,
            ncols: schema.len(),
        }
    }
}

pub struct CSVSourcePartitionParser<'a> {
    records: &'a mut [csv::StringRecord],
    counter: &'a mut usize,
    ncols: usize,
}

impl<'a> CSVSourcePartitionParser<'a> {
    fn next_val(&mut self) -> &str {
        let v: &str = self.records[*self.counter / self.ncols][*self.counter % self.ncols].as_ref();
        *self.counter += 1;

        v
    }
}

impl<'a> ValueStream<'a> for CSVSourcePartitionParser<'a> {
    type TypeSystem = CSVTypeSystem;
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn fetch_batch(&mut self) -> (usize, bool) {
        if *self.counter == 0 {
            (self.records.len(), true)
        } else {
            (0, true)
        }
    }
}

impl<'r, 'a> Produce<'r, i64> for CSVSourcePartitionParser<'a> {
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn produce(&mut self) -> i64 {
        let v = self.next_val();
        v.parse()
            .map_err(|_| ConnectorXError::cannot_produce::<i64>(Some(v.into())))?
    }
}

impl<'r, 'a> Produce<'r, Option<i64>> for CSVSourcePartitionParser<'a> {
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn produce(&mut self) -> Option<i64> {
        let v = self.next_val();
        if v.is_empty() {
            return None;
        }
        let v = v
            .parse()
            .map_err(|_| ConnectorXError::cannot_produce::<Option<i64>>(Some(v.into())))?;

        Some(v)
    }
}

impl<'r, 'a> Produce<'r, f64> for CSVSourcePartitionParser<'a> {
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn produce(&mut self) -> f64 {
        let v = self.next_val();
        v.parse()
            .map_err(|_| ConnectorXError::cannot_produce::<f64>(Some(v.into())))?
    }
}

impl<'r, 'a> Produce<'r, Option<f64>> for CSVSourcePartitionParser<'a> {
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn produce(&mut self) -> Option<f64> {
        let v = self.next_val();
        if v.is_empty() {
            return None;
        }
        let v = v
            .parse()
            .map_err(|_| ConnectorXError::cannot_produce::<Option<f64>>(Some(v.into())))?;

        Some(v)
    }
}

impl<'r, 'a> Produce<'r, bool> for CSVSourcePartitionParser<'a> {
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn produce(&mut self) -> bool {
        let v = self.next_val();
        v.parse()
            .map_err(|_| ConnectorXError::cannot_produce::<bool>(Some(v.into())))?
    }
}

impl<'r, 'a> Produce<'r, Option<bool>> for CSVSourcePartitionParser<'a> {
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn produce(&mut self) -> Option<bool> {
        let v = self.next_val();
        if v.is_empty() {
            return None;
        }
        let v = v
            .parse()
            .map_err(|_| ConnectorXError::cannot_produce::<Option<bool>>(Some(v.into())))?;

        Some(v)
    }
}

impl<'r, 'a> Produce<'r, String> for CSVSourcePartitionParser<'a> {
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn produce(&mut self) -> String {
        let v = self.next_val();
        String::from(v)
    }
}

impl<'a, 'r> Produce<'r, Option<String>> for CSVSourcePartitionParser<'a> {
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn produce(&'r mut self) -> Option<String> {
        let v = self.next_val();

        Some(String::from(v))
    }
}

impl<'r, 'a> Produce<'r, DateTime<Utc>> for CSVSourcePartitionParser<'a> {
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn produce(&mut self) -> DateTime<Utc> {
        let v = self.next_val();
        v.parse()
            .map_err(|_| ConnectorXError::cannot_produce::<DateTime<Utc>>(Some(v.into())))?
    }
}

impl<'r, 'a> Produce<'r, Option<DateTime<Utc>>> for CSVSourcePartitionParser<'a> {
    type Error = CSVSourceError;

    #[throws(CSVSourceError)]
    fn produce(&mut self) -> Option<DateTime<Utc>> {
        let v = self.next_val();
        if v.is_empty() {
            return None;
        }
        let v = v
            .parse()
            .map_err(|_| ConnectorXError::cannot_produce::<DateTime<Utc>>(Some(v.into())))?;
        Some(v)
    }
}
