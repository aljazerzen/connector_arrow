use fehler::throws;
use hex::decode;
use postgres::{SimpleQueryMessage, SimpleQueryRow};
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

use crate::rewrite::api::Statement;
use crate::rewrite::util::transport::{Produce, ProduceTy};
use crate::rewrite::util::{arrow_reader::ArrowReader, collect_rows_to_arrow, CellReader};
use crate::rewrite::{errors::ConnectorError, util::RowsReader};

use super::{types, PostgresStatement};
pub use super::{PostgresError, SimpleProtocol};

impl<'conn> Statement<'conn> for PostgresStatement<'conn, SimpleProtocol> {
    type Params = ();

    type Reader<'stmt> = ArrowReader where Self: 'stmt;

    fn start(&mut self, _params: ()) -> Result<Self::Reader<'_>, ConnectorError> {
        let stmt = &self.stmt;
        let schema = types::convert_schema(stmt)?;

        let rows = self
            .client
            .simple_query(&self.query)
            .map_err(PostgresError::from)?;

        let row_count = rows.len();

        let mut row_reader = PostgresRowsReader {
            rows: rows.into_iter(),
        };
        let batches = collect_rows_to_arrow(schema.clone(), &mut row_reader, row_count)?;

        Ok(ArrowReader::new(schema, batches))
    }
}

struct PostgresRowsReader {
    rows: std::vec::IntoIter<SimpleQueryMessage>,
}

impl<'stmt> RowsReader<'stmt> for PostgresRowsReader {
    type CellReader<'row> = PostgresCellReader where Self: 'row;

    #[throws(ConnectorError)]
    fn next_row(&mut self) -> Option<Self::CellReader<'_>> {
        self.rows.next().and_then(|message| match message {
            SimpleQueryMessage::Row(row) => Some(PostgresCellReader { row, next_col: 0 }),
            _ => None,
        })
    }
}

struct PostgresCellReader {
    row: SimpleQueryRow,
    next_col: usize,
}

impl<'row> CellReader<'row> for PostgresCellReader {
    type CellRef<'cell> = CellRef<'cell> where Self: 'cell;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        if self.next_col >= self.row.columns().len() {
            return None;
        }
        let col = self.next_col;
        self.next_col += 1;
        Some((&self.row, col))
    }
}

type CellRef<'a> = (&'a SimpleQueryRow, usize);

impl<'c> Produce<'c> for CellRef<'c> {}

fn err_null() -> ConnectorError {
    ConnectorError::DataSchemaMismatch("NULL in non-nullable column".into())
}

macro_rules! impl_simple_produce_unimplemented {
    ($($t: ty,)+) => {
        $(
            impl<'r> ProduceTy<'r, $t> for CellRef<'r> {
                fn produce(&self) -> Result<$t, ConnectorError> {
                   unimplemented!("not implemented!");
                }

                fn produce_opt(&self) -> Result<Option<$t>, ConnectorError> {
                   unimplemented!("not implemented!");
                }
            }
        )+
    };
}

macro_rules! impl_simple_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r> ProduceTy<'r, $t> for CellRef<'r> {
                fn produce(&self) -> Result<$t, ConnectorError> {
                    self.produce_opt()?.ok_or_else(err_null)
                }

                fn produce_opt(&self) -> Result<Option<$t>, ConnectorError> {
                    let s = self.0.get(self.1);

                    Ok(match s {
                        Some(s) => Some(
                            s.parse::<$t>().map_err(|e| ConnectorError::DataSchemaMismatch(e.to_string()))?,
                        ),
                        None => None,
                    })
                }
            }
        )+
    };
}

impl_simple_produce!(i8, i16, i32, i64, f32, f64, Decimal, Uuid,);
impl_simple_produce_unimplemented!(
    u64, u32, u16, u8, Value, HashMap<String, Option<String>>,
);

impl<'r> ProduceTy<'r, String> for CellRef<'r> {
    #[throws(ConnectorError)]
    fn produce(&self) -> String {
        let val = self.0.get(self.1).unwrap().to_string();
        val
    }

    #[throws(ConnectorError)]
    fn produce_opt(&self) -> Option<String> {
        self.0.get(self.1).map(|x| x.to_string())
    }
}

fn parse_bool(token: &str) -> Result<bool, ConnectorError> {
    match token {
        "t" => Ok(true),
        "f" => Ok(false),
        s => Err(ConnectorError::DataSchemaMismatch(format!(
            "expected t or f, got {s}"
        ))),
    }
}

impl<'r> ProduceTy<'r, bool> for CellRef<'r> {
    fn produce(&self) -> Result<bool, ConnectorError> {
        self.produce_opt()?.ok_or_else(err_null)
    }

    fn produce_opt(&self) -> Result<Option<bool>, ConnectorError> {
        let s = self.0.get(self.1);
        s.map(parse_bool).transpose()
    }
}

impl<'r> ProduceTy<'r, Vec<u8>> for CellRef<'r> {
    fn produce(&self) -> Result<Vec<u8>, ConnectorError> {
        self.produce_opt()?.ok_or_else(err_null)
    }

    #[throws(ConnectorError)]
    fn produce_opt(&self) -> Option<Vec<u8>> {
        let s = self.0.get(self.1);

        match s {
            Some(s) => {
                let mut res = s.chars();
                res.next();
                res.next();
                let bytes = res
                    .enumerate()
                    .fold(String::new(), |acc, (_i, c)| format!("{}{}", acc, c))
                    .chars()
                    .map(|c| c as u8)
                    .collect::<Vec<u8>>();
                Some(decode(bytes).map_err(PostgresError::from)?)
            }
            None => None,
        }
    }
}

fn rem_first_and_last(value: &str) -> &str {
    let mut chars = value.chars();
    chars.next();
    chars.next_back();
    chars.as_str()
}

fn parse_array<F, T>(val: Option<&str>, item_parser: F) -> Result<Option<Vec<T>>, ConnectorError>
where
    F: Fn(&str) -> Result<T, ConnectorError>,
{
    Ok(match val {
        None | Some("") => None,
        Some("{}") => Some(vec![]),
        Some(s) => Some(
            rem_first_and_last(s)
                .split(',')
                .map(item_parser)
                .collect::<Result<Vec<T>, ConnectorError>>()?,
        ),
    })
}

macro_rules! impl_simple_vec_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r> ProduceTy<'r, Vec<$t>> for CellRef<'r> {
                fn produce(&self) -> Result<Vec<$t>, ConnectorError> {
                    self.produce_opt()?.ok_or_else(err_null)
                }

                fn produce_opt(&self) -> Result<Option<Vec<$t>>, ConnectorError> {
                    let s = self.0.get(self.1);

                    parse_array(
                        s,
                        |token| token.parse::<$t>().map_err(|e| ConnectorError::DataSchemaMismatch(e.to_string()))
                    )
                }
            }
        )+
    };
}
impl_simple_vec_produce!(i16, i32, i64, f32, f64, Decimal, String,);

impl<'r> ProduceTy<'r, Vec<bool>> for CellRef<'r> {
    fn produce(&self) -> Result<Vec<bool>, ConnectorError> {
        self.produce_opt()?.ok_or_else(err_null)
    }

    fn produce_opt(&self) -> Result<Option<Vec<bool>>, ConnectorError> {
        let s = self.0.get(self.1);

        parse_array(s, parse_bool)
    }
}

// impl<'r> ProduceTy<'r, NaiveDate> for CellRef<'r> {
//     fn produce(&self) -> NaiveDate {
//         let (ridx, cidx) = self.next_cell();
//         let val = match &self.rows[ridx] {
//             SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
//                 Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
//                     .map_err(|_| ConnectorXError::cannot_produce::<NaiveDate>(Some(s.into())))?,
//                 None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
//             },
//             SimpleQueryMessage::CommandComplete(c) => {
//                 panic!("get command: {}", c);
//             }
//             _ => {
//                 panic!("what?");
//             }
//         };
//         val
//     }
// }

// impl<'r> ProduceTy<'r, Option<NaiveDate>> for CellRef<'r> {
//     fn produce(&self) -> Option<NaiveDate> {
//         let (ridx, cidx) = self.next_cell();
//         let val = match &self.rows[ridx] {
//             SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
//                 Some(s) => Some(NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
//                     ConnectorXError::cannot_produce::<Option<NaiveDate>>(Some(s.into()))
//                 })?),
//                 None => None,
//             },
//             SimpleQueryMessage::CommandComplete(c) => {
//                 panic!("get command: {}", c);
//             }
//             _ => {
//                 panic!("what?");
//             }
//         };
//         val
//     }
// }

// impl<'r> ProduceTy<'r, NaiveTime> for CellRef<'r> {
//     fn produce(&self) -> NaiveTime {
//         let (ridx, cidx) = self.next_cell();
//         let val = match &self.rows[ridx] {
//             SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
//                 Some(s) => NaiveTime::parse_from_str(s, "%H:%M:%S")
//                     .map_err(|_| ConnectorXError::cannot_produce::<NaiveTime>(Some(s.into())))?,
//                 None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
//             },
//             SimpleQueryMessage::CommandComplete(c) => {
//                 panic!("get command: {}", c);
//             }
//             _ => {
//                 panic!("what?");
//             }
//         };
//         val
//     }
// }

// impl<'r> ProduceTy<'r, Option<NaiveTime>> for CellRef<'r> {
//     fn produce(&self) -> Option<NaiveTime> {
//         let (ridx, cidx) = self.next_cell();
//         let val = match &self.rows[ridx] {
//             SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
//                 Some(s) => Some(NaiveTime::parse_from_str(s, "%H:%M:%S").map_err(|_| {
//                     ConnectorXError::cannot_produce::<Option<NaiveTime>>(Some(s.into()))
//                 })?),
//                 None => None,
//             },
//             SimpleQueryMessage::CommandComplete(c) => {
//                 panic!("get command: {}", c);
//             }
//             _ => {
//                 panic!("what?");
//             }
//         };
//         val
//     }
// }

// impl<'r> ProduceTy<'r, NaiveDateTime> for CellRef<'r> {
//     fn produce(&self) -> NaiveDateTime {
//         let (ridx, cidx) = self.next_cell();
//         let val = match &self.rows[ridx] {
//             SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
//                 Some(s) => NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map_err(|_| {
//                     ConnectorXError::cannot_produce::<NaiveDateTime>(Some(s.into()))
//                 })?,
//                 None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
//             },
//             SimpleQueryMessage::CommandComplete(c) => {
//                 panic!("get command: {}", c);
//             }
//             _ => {
//                 panic!("what?");
//             }
//         };
//         val
//     }
// }

// impl<'r> ProduceTy<'r, Option<NaiveDateTime>> for CellRef<'r> {
//     fn produce(&self) -> Option<NaiveDateTime> {
//         let (ridx, cidx) = self.next_cell();
//         let val = match &self.rows[ridx] {
//             SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
//                 Some(s) => Some(
//                     NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map_err(|_| {
//                         ConnectorXError::cannot_produce::<Option<NaiveDateTime>>(Some(s.into()))
//                     })?,
//                 ),
//                 None => None,
//             },
//             SimpleQueryMessage::CommandComplete(c) => {
//                 panic!("get command: {}", c);
//             }
//             _ => {
//                 panic!("what?");
//             }
//         };
//         val
//     }
// }

// impl<'r> ProduceTy<'r, DateTime<Utc>> for CellRef<'r> {
//     fn produce(&self) -> DateTime<Utc> {
//         let (ridx, cidx) = self.next_cell();
//         let val = match &self.rows[ridx] {
//             SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
//                 Some(s) => {
//                     let time_string = format!("{}:00", s).to_owned();
//                     let slice: &str = &time_string[..];
//                     let time: DateTime<FixedOffset> =
//                         DateTime::parse_from_str(slice, "%Y-%m-%d %H:%M:%S%:z").unwrap();

//                     time.with_timezone(&Utc)
//                 }
//                 None => throw!(anyhow!("Cannot parse NULL in non-NULL column.")),
//             },
//             SimpleQueryMessage::CommandComplete(c) => {
//                 panic!("get command: {}", c);
//             }
//             _ => {
//                 panic!("what?");
//             }
//         };
//         val
//     }
// }

// impl<'r> ProduceTy<'r, Option<DateTime<Utc>>> for CellRef<'r> {
//     fn produce(&self) -> Option<DateTime<Utc>> {
//         let (ridx, cidx) = self.next_cell();
//         let val = match &self.rows[ridx] {
//             SimpleQueryMessage::Row(row) => match row.try_get(cidx)? {
//                 Some(s) => {
//                     let time_string = format!("{}:00", s).to_owned();
//                     let slice: &str = &time_string[..];
//                     let time: DateTime<FixedOffset> =
//                         DateTime::parse_from_str(slice, "%Y-%m-%d %H:%M:%S%:z").unwrap();

//                     Some(time.with_timezone(&Utc))
//                 }
//                 None => None,
//             },
//             SimpleQueryMessage::CommandComplete(c) => {
//                 panic!("get command: {}", c);
//             }
//             _ => {
//                 panic!("what?");
//             }
//         };
//         val
//     }
// }
