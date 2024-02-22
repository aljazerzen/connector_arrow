use arrow::datatypes::*;
use hex::decode;
use postgres::{SimpleQueryMessage, SimpleQueryRow};

use crate::api::{ArrowValue, Statement};
use crate::types::{ArrowType, FixedSizeBinaryType};
use crate::util::{collect_rows_to_arrow, transport, ArrowReader, CellReader};
use crate::{errors::ConnectorError, util::RowsReader};

use super::{types, PostgresError, PostgresStatement, ProtocolSimple};

impl<'conn> Statement<'conn> for PostgresStatement<'conn, ProtocolSimple> {
    type Reader<'stmt> = ArrowReader where Self: 'stmt;

    fn start(&mut self, _params: &[&dyn ArrowValue]) -> Result<Self::Reader<'_>, ConnectorError> {
        let stmt = &self.stmt;
        let schema = types::pg_stmt_to_arrow(stmt)?;

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

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
        Ok(self.rows.next().and_then(|message| match message {
            SimpleQueryMessage::Row(row) => Some(PostgresCellReader { row, next_col: 0 }),
            _ => None,
        }))
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

impl<'c> transport::Produce<'c> for CellRef<'c> {}

fn err_null() -> ConnectorError {
    ConnectorError::DataSchemaMismatch("NULL in non-nullable column".into())
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

impl<'r> transport::ProduceTy<'r, BooleanType> for CellRef<'r> {
    fn produce(self) -> Result<bool, ConnectorError> {
        transport::ProduceTy::<BooleanType>::produce_opt(self)?.ok_or_else(err_null)
    }

    fn produce_opt(self) -> Result<Option<bool>, ConnectorError> {
        let s = self.0.get(self.1);
        s.map(parse_bool).transpose()
    }
}

macro_rules! impl_produce_numeric {
    ($($t: ty,)+) => {
        $(
            impl<'r> transport::ProduceTy<'r, $t> for CellRef<'r> {
                fn produce(self) -> Result<<$t as ArrowType>::Native, ConnectorError> {
                    transport::ProduceTy::<$t>::produce_opt(self)?.ok_or_else(err_null)
                }

                fn produce_opt(self) -> Result<Option<<$t as ArrowType>::Native>, ConnectorError> {
                    let s = self.0.get(self.1);

                    Ok(match s {
                        Some(s) => Some(
                            s.parse::<<$t as ArrowType>::Native>()
                                .map_err(|_| ConnectorError::DataSchemaMismatch(format!("bad numeric encoding: {s}")))?,
                        ),
                        None => None,
                    })
                }
            }
        )+
    };
}

impl_produce_numeric!(
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
);

crate::impl_produce_unsupported!(
    CellRef<'r>,
    (
        UInt8Type,
        UInt16Type,
        UInt32Type,
        UInt64Type,
        Float16Type,
        TimestampSecondType,
        TimestampMillisecondType,
        TimestampMicrosecondType,
        TimestampNanosecondType,
        Date32Type,
        Date64Type,
        Time32SecondType,
        Time32MillisecondType,
        Time64MicrosecondType,
        Time64NanosecondType,
        IntervalYearMonthType,
        IntervalDayTimeType,
        IntervalMonthDayNanoType,
        DurationSecondType,
        DurationMillisecondType,
        DurationMicrosecondType,
        DurationNanosecondType,
        BinaryType,
        FixedSizeBinaryType,
        Decimal128Type,
        Decimal256Type,
    )
);

impl<'r> transport::ProduceTy<'r, Utf8Type> for CellRef<'r> {
    fn produce(self) -> Result<String, ConnectorError> {
        let val = self.0.get(self.1).unwrap().to_string();
        Ok(val)
    }

    fn produce_opt(self) -> Result<Option<String>, ConnectorError> {
        Ok(self.0.get(self.1).map(|x| x.to_string()))
    }
}

impl<'r> transport::ProduceTy<'r, LargeUtf8Type> for CellRef<'r> {
    fn produce(self) -> Result<String, ConnectorError> {
        let val = self.0.get(self.1).unwrap().to_string();
        Ok(val)
    }

    fn produce_opt(self) -> Result<Option<String>, ConnectorError> {
        Ok(self.0.get(self.1).map(|x| x.to_string()))
    }
}

impl<'r> transport::ProduceTy<'r, LargeBinaryType> for CellRef<'r> {
    fn produce(self) -> Result<Vec<u8>, ConnectorError> {
        transport::ProduceTy::<LargeBinaryType>::produce_opt(self)?.ok_or_else(err_null)
    }

    fn produce_opt(self) -> Result<Option<Vec<u8>>, ConnectorError> {
        let s = self.0.get(self.1);

        Ok(match s {
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
        })
    }
}

// fn rem_first_and_last(value: &str) -> &str {
//     let mut chars = value.chars();
//     chars.next();
//     chars.next_back();
//     chars.as_str()
// }

// fn parse_array<F, T>(val: Option<&str>, item_parser: F) -> Result<Option<Vec<T>>, ConnectorError>
// where
//     F: Fn(&str) -> Result<T, ConnectorError>,
// {
//     Ok(match val {
//         None | Some("") => None,
//         Some("{}") => Some(vec![]),
//         Some(s) => Some(
//             rem_first_and_last(s)
//                 .split(',')
//                 .map(item_parser)
//                 .collect::<Result<Vec<T>, ConnectorError>>()?,
//         ),
//     })
// }

// macro_rules! impl_simple_vec_produce {
//     ($($t: ty,)+) => {
//         $(
//             impl<'r> ProduceTy<'r, Vec<$t>> for CellRef<'r> {
//                 fn produce(self) -> Result<Vec<$t>, ConnectorError> {
//                     self.produce_opt()?.ok_or_else(err_null)
//                 }

//                 fn produce_opt(self) -> Result<Option<Vec<$t>>, ConnectorError> {
//                     let s = self.0.get(self.1);

//                     parse_array(
//                         s,
//                         |token| token.parse::<$t>().map_err(|e| ConnectorError::DataSchemaMismatch(e.to_string()))
//                     )
//                 }
//             }
//         )+
//     };
// }
// impl_simple_vec_produce!(i16, i32, i64, f32, f64, Decimal, String,);

// impl<'r> ProduceTy<'r, Vec<bool>> for CellRef<'r> {
//     fn produce(self) -> Result<Vec<bool>, ConnectorError> {
//         self.produce_opt()?.ok_or_else(err_null)
//     }

//     fn produce_opt(self) -> Result<Option<Vec<bool>>, ConnectorError> {
//         let s = self.0.get(self.1);

//         parse_array(s, parse_bool)
//     }
// }

// impl<'r> ProduceTy<'r, NaiveDate> for CellRef<'r> {
//     fn produce(self) -> NaiveDate {
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
//     fn produce(self) -> Option<NaiveDate> {
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
//     fn produce(self) -> NaiveTime {
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
//     fn produce(self) -> Option<NaiveTime> {
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
//     fn produce(self) -> NaiveDateTime {
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
//     fn produce(self) -> Option<NaiveDateTime> {
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
//     fn produce(self) -> DateTime<Utc> {
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
//     fn produce(self) -> Option<DateTime<Utc>> {
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
