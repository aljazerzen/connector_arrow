use arrow::{datatypes::*, record_batch::RecordBatch};
use mysql::prelude::*;
use pac_cell::PacCell;

use crate::api::{ResultReader, Statement};
use crate::impl_produce_unsupported;
use crate::types::{ArrowType, FixedSizeBinaryType, NullType};
use crate::util::transport::ProduceTy;
use crate::util::{self, transport::Produce};
use crate::ConnectorError;

pub struct MySQLStatement<'conn, Q: Queryable> {
    pub(super) stmt: mysql::Statement,
    pub(super) queryable: &'conn mut Q,
}

impl<'conn, C: Queryable> Statement<'conn> for MySQLStatement<'conn, C> {
    type Reader<'stmt> = MySQLQueryResult<'stmt>
    where
        Self: 'stmt;

    fn start<'p, I>(&mut self, _params: I) -> Result<Self::Reader<'_>, ConnectorError>
    where
        I: IntoIterator<Item = &'p dyn crate::api::ArrowValue>,
    {
        // TODO: params

        let query_result = self.queryable.exec_iter(&self.stmt, ())?;

        // PacCell is needed so we can return query_result and result_set that mutably borrows query result.
        let pac = PacCell::try_new(query_result, |qr| -> Result<_, ConnectorError> {
            let result_set = qr.iter().ok_or(ConnectorError::NoResultSets)?;
            let schema = super::types::get_result_schema(&result_set)?;
            Ok(MySQLResultReader { result_set, schema })
        })?;
        Ok(MySQLQueryResult(pac))
    }
}

pub struct MySQLQueryResult<'stmt>(
    PacCell<
        mysql::QueryResult<'stmt, 'stmt, 'stmt, mysql::Binary>, // parent
        MySQLResultReader<'stmt>,                               // child
    >,
);

impl<'stmt> ResultReader<'stmt> for MySQLQueryResult<'stmt> {
    fn get_schema(&mut self) -> Result<arrow::datatypes::SchemaRef, ConnectorError> {
        Ok(self.0.with_mut(|x| x.schema.clone()))
    }
}

impl<'stmt> Iterator for MySQLQueryResult<'stmt> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.with_mut(|reader| {
            let schema = reader.schema.clone();
            util::next_batch_from_rows(&schema, reader, 1024).transpose()
        })
    }
}

struct MySQLResultReader<'stmt> {
    result_set: mysql::ResultSet<'stmt, 'stmt, 'stmt, 'stmt, mysql::Binary>,
    schema: SchemaRef,
}

impl<'s> util::RowsReader<'s> for MySQLResultReader<'s> {
    type CellReader<'row> = MySQLCellReader
    where
        Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
        let row = self.result_set.next().transpose()?;
        Ok(row.map(|row| MySQLCellReader { row, cell: 0 }))
    }
}

struct MySQLCellReader {
    row: mysql::Row,
    cell: usize,
}

impl<'a> util::CellReader<'a> for MySQLCellReader {
    type CellRef<'cell> = MySQLCellRef<'cell>
    where
        Self: 'cell;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        let r = MySQLCellRef {
            row: &mut self.row,
            cell: self.cell,
        };
        self.cell += 1;

        Some(r)
    }
}

#[derive(Debug)]
struct MySQLCellRef<'a> {
    row: &'a mut mysql::Row,
    cell: usize,
}

impl<'r> Produce<'r> for MySQLCellRef<'r> {}

macro_rules! impl_produce_ty {
    ($p: ty, ($($t: ty,)+)) => {
        $(
            impl<'r> ProduceTy<'r, $t> for $p {
                fn produce(self) -> Result<<$t as ArrowType>::Native, ConnectorError> {
                    Ok(self.row.take(self.cell).unwrap())
                }
                fn produce_opt(self) -> Result<Option<<$t as ArrowType>::Native>, ConnectorError> {
                    let res = self.row.take_opt(self.cell).unwrap();
                    match res {
                        Ok(v) => Ok(Some(v)),
                        Err(mysql::FromValueError(mysql::Value::NULL)) => Ok(None),
                        Err(mysql::FromValueError(v)) => Err(ConnectorError::from(mysql::Error::FromValueError(v)))
                    }
                }
            }
        )+
    };
}

impl_produce_ty!(
    MySQLCellRef<'r>,
    (
        BooleanType,
        Int8Type,
        Int16Type,
        Int32Type,
        Int64Type,
        UInt8Type,
        UInt16Type,
        UInt32Type,
        UInt64Type,
        Float32Type,
        Float64Type,
        BinaryType,
    )
);

impl_produce_unsupported!(
    MySQLCellRef<'r>,
    (
        NullType,
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
        LargeUtf8Type,
        LargeBinaryType,
        FixedSizeBinaryType,
        Decimal128Type,
        Decimal256Type,
    )
);

impl<'r> ProduceTy<'r, Utf8Type> for MySQLCellRef<'r> {
    fn produce(self) -> Result<String, ConnectorError> {
        ProduceTy::<Utf8Type>::produce_opt(self).and_then(|x| {
            x.ok_or_else(|| ConnectorError::DataSchemaMismatch("unexpected NULL".into()))
        })
    }
    fn produce_opt(self) -> Result<Option<String>, ConnectorError> {
        let res: mysql::Value = self.row.take(self.cell).unwrap();
        match res {
            mysql::Value::NULL => Ok(None),
            mysql::Value::Bytes(_) => Ok(Some(
                String::from_value_opt(res).map_err(|x| ConnectorError::MySQL(x.into()))?,
            )),
            mysql::Value::Int(_) => todo!(),
            mysql::Value::UInt(_) => todo!(),
            mysql::Value::Float(_) => todo!(),
            mysql::Value::Double(_) => todo!(),
            mysql::Value::Date(year, month, day, hour, minutes, seconds, micro_seconds) => {
                // TODO: converting to timestamp
                // let date_time = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32).unwrap().and_hms_opt(hour as u32, minutes as u32, seconds as u32).unwrap();
                // let date_time: chrono::DateTime<chrono::Utc> = chrono::DateTime::from_naive_utc_and_offset(date_time, ???);
                // let timestamp_sec = date_time.timestamp();
                // let timestamp_micro = timestamp_sec * 1_000_000 + micro_seconds as i64;
                // Ok(timestamp_micro)

                Ok(Some(format!(
                    "{year:04}-{month:02}-{day:02}T{hour:02}:{minutes:02}:{seconds:02}.{micro_seconds:06}"
                )))
            }
            mysql::Value::Time(_, _, _, _, _, _) => todo!(),
        }
    }
}
