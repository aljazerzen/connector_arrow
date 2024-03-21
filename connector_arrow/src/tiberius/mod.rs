mod types;

use arrow::{datatypes::*, record_batch::RecordBatch};
use futures::{AsyncRead, AsyncWrite, StreamExt};
use std::sync::Arc;
use tiberius::QueryStream;
use tokio::runtime::Runtime;

use crate::api::{unimplemented, Connector, ResultReader, Statement};
use crate::impl_produce_unsupported;
use crate::types::{ArrowType, FixedSizeBinaryType, NullType};
use crate::util::transport::ProduceTy;
use crate::util::{self, transport::Produce};
use crate::ConnectorError;

pub struct TiberiusConnection<S: AsyncRead + AsyncWrite + Unpin + Send> {
    pub rt: Arc<Runtime>,
    pub client: tiberius::Client<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> TiberiusConnection<S> {
    pub fn new(rt: Arc<Runtime>, client: tiberius::Client<S>) -> Self {
        TiberiusConnection { rt, client }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connector for TiberiusConnection<S> {
    type Stmt<'conn> = TiberiusStatement<'conn, S> where Self: 'conn;

    type Append<'conn> = unimplemented::Appender where Self: 'conn;

    fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        Ok(TiberiusStatement {
            conn: self,
            query: query.to_string(),
        })
    }

    fn append<'a>(&'a mut self, _table_name: &str) -> Result<Self::Append<'a>, ConnectorError> {
        Ok(unimplemented::Appender {})
    }

    fn type_db_into_arrow(ty: &str) -> Option<DataType> {
        Some(match ty {
            "null" | "intn" => DataType::Null,
            "bit" => DataType::Boolean,
            "tinyint" => DataType::UInt8,
            "smallint" => DataType::Int16,
            "int" => DataType::Int32,
            "bigint" => DataType::Int64,
            "float" => DataType::Float32,
            "real" => DataType::Float64,
            _ => return None,
        })
    }

    fn type_arrow_into_db(_ty: &DataType) -> Option<String> {
        None
    }
}

pub struct TiberiusStatement<'conn, S: AsyncRead + AsyncWrite + Unpin + Send> {
    conn: &'conn mut TiberiusConnection<S>,
    query: String,
}

impl<'conn, S: AsyncRead + AsyncWrite + Unpin + Send> Statement<'conn>
    for TiberiusStatement<'conn, S>
{
    type Reader<'stmt> = TiberiusResultReader<'stmt>
    where
        Self: 'stmt;

    fn start<'p, I>(&mut self, _params: I) -> Result<Self::Reader<'_>, ConnectorError>
    where
        I: IntoIterator<Item = &'p dyn crate::api::ArrowValue>,
    {
        // TODO: params

        let mut stream = self
            .conn
            .rt
            .block_on(self.conn.client.query(&self.query, &[]))?;

        // get columns
        let columns = self.conn.rt.block_on(stream.columns())?;
        let schema = types::get_result_schema(columns)?;
        self.conn.rt.block_on(stream.next());

        Ok(TiberiusResultReader {
            schema,
            stream: TiberiusStream {
                rt: self.conn.rt.clone(),
                stream,
            },
        })
    }
}

pub struct TiberiusResultReader<'stmt> {
    schema: SchemaRef,
    stream: TiberiusStream<'stmt>,
}

struct TiberiusStream<'stmt> {
    rt: Arc<Runtime>,
    stream: QueryStream<'stmt>,
}

impl<'stmt> ResultReader<'stmt> for TiberiusResultReader<'stmt> {
    fn get_schema(&mut self) -> Result<arrow::datatypes::SchemaRef, ConnectorError> {
        Ok(self.schema.clone())
    }
}

impl<'stmt> Iterator for TiberiusResultReader<'stmt> {
    type Item = Result<RecordBatch, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        util::next_batch_from_rows(&self.schema, &mut self.stream, 1024).transpose()
    }
}

impl<'s> util::RowsReader<'s> for TiberiusStream<'s> {
    type CellReader<'row> = TiberiusCellReader
    where
        Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::CellReader<'_>>, ConnectorError> {
        let item = self.rt.block_on(self.stream.next());

        // are we done?
        let Some(item) = item else { return Ok(None) };

        // are there more result sets?
        let row = match item? {
            tiberius::QueryItem::Row(row) => row,
            tiberius::QueryItem::Metadata(metadata) => {
                dbg!(metadata);
                // yes, this there are
                return Err(ConnectorError::MultipleResultSets);
            }
        };

        Ok(Some(TiberiusCellReader { row, cell: 0 }))
    }
}

struct TiberiusCellReader {
    row: tiberius::Row,
    cell: usize,
}

impl<'a> util::CellReader<'a> for TiberiusCellReader {
    type CellRef<'cell> = TiberiusCellRef<'cell>
    where
        Self: 'cell;

    fn next_cell(&mut self) -> Option<Self::CellRef<'_>> {
        let r = TiberiusCellRef {
            row: &mut self.row,
            cell: self.cell,
        };
        self.cell += 1;
        Some(r)
    }
}

#[derive(Debug)]
struct TiberiusCellRef<'a> {
    row: &'a mut tiberius::Row,
    cell: usize,
}

impl<'r> Produce<'r> for TiberiusCellRef<'r> {}

macro_rules! impl_produce_ty {
    ($p: ty, ($($t: ty,)+)) => {
        $(
            impl<'r> ProduceTy<'r, $t> for $p {
                fn produce(self) -> Result<<$t as ArrowType>::Native, ConnectorError> {
                    Ok(self.row.get(self.cell).unwrap())
                }
                fn produce_opt(self) -> Result<Option<<$t as ArrowType>::Native>, ConnectorError> {
                    Ok(self.row.get(self.cell))
                }
            }
        )+
    };
}

impl_produce_ty!(
    TiberiusCellRef<'r>,
    (
        BooleanType,
        Int16Type,
        Int32Type,
        Int64Type,
        UInt8Type,
        Float32Type,
        Float64Type,
    )
);

impl_produce_unsupported!(
    TiberiusCellRef<'r>,
    (
        NullType,
        Int8Type,
        UInt16Type,
        Float16Type,
        UInt32Type,
        UInt64Type,
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
        Utf8Type,
        BinaryType,
    )
);
