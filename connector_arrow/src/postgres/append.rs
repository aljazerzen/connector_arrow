use std::sync::Arc;

use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use bytes::BytesMut;
use itertools::{zip_eq, Itertools};
use postgres::binary_copy::BinaryCopyInWriter;
use postgres::types::{to_sql_checked, IsNull, ToSql};
use postgres::{Client, CopyInWriter};

use crate::api::Append;
use crate::types::{FixedSizeBinaryType, NullType};
use crate::util::transport::{Consume, ConsumeTy};
use crate::util::ArrayCellRef;
use crate::{impl_consume_unsupported, ConnectorError};

use super::PostgresError;

pub struct PostgresAppender<'c> {
    writer: Writer<'c>,
}

impl<'conn> PostgresAppender<'conn> {
    pub fn new(client: &'conn mut Client, table_name: &str) -> Result<Self, ConnectorError> {
        let query = format!("COPY BINARY \"{table_name}\" FROM stdin");
        let writer = client.copy_in(&query).map_err(PostgresError::Postgres)?;
        let writer = Writer::Uninitialized(writer);
        Ok(Self { writer })
    }
}

enum Writer<'c> {
    Uninitialized(CopyInWriter<'c>),
    Invalid,
    Initialized { writer: BinaryCopyInWriter<'c> },
}

impl<'c> Writer<'c> {
    fn as_binary(
        &mut self,
        schema: SchemaRef,
    ) -> Result<&mut BinaryCopyInWriter<'c>, ConnectorError> {
        if let Writer::Uninitialized(_) = self {
            // replace plain writer with a new binary one
            let Writer::Uninitialized(w) = std::mem::replace(self, Writer::Invalid) else {
                unreachable!();
            };

            // types don't really matter
            // they are used only for client-side checking of match between the
            // declared type and passed value.
            // Because our ToSql::accepts returns true
            let types = vec![postgres::types::Type::VOID; schema.fields().len()];

            *self = Writer::Initialized {
                writer: BinaryCopyInWriter::new(w, &types),
            }
        }

        // return binary writer
        let Writer::Initialized { writer } = self else {
            unreachable!();
        };
        Ok(writer)
    }

    fn finish(mut self) -> Result<u64, ConnectorError> {
        let schema = Arc::new(Schema::new(vec![] as Vec<Field>));
        self.as_binary(schema)?;
        match self {
            Writer::Initialized { writer: w, .. } => {
                Ok(w.finish().map_err(PostgresError::Postgres)?)
            }
            Writer::Uninitialized(_) | Writer::Invalid => unreachable!(),
        }
    }
}

impl<'conn> Append<'conn> for PostgresAppender<'conn> {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        let writer = self.writer.as_binary(batch.schema())?;

        let schema = batch.schema();
        let mut row = zip_eq(batch.columns(), schema.fields())
            .map(|(array, field)| ArrayCellRef {
                array,
                field,
                row_number: 0,
            })
            .collect_vec();

        for row_number in 0..batch.num_rows() {
            for cell in &mut row {
                cell.row_number = row_number;
            }

            writer.write_raw(&row).map_err(PostgresError::Postgres)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<(), ConnectorError> {
        self.writer.finish()?;
        Ok(())
    }
}

impl<'a> ToSql for ArrayCellRef<'a> {
    fn to_sql(
        &self,
        _ty: &postgres::types::Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        if self.array.is_null(self.row_number) || matches!(self.field.data_type(), DataType::Null) {
            return Ok(IsNull::Yes);
        }
        crate::util::transport::transport(self.field, self, out)?;
        Ok(IsNull::No)
    }

    fn accepts(_: &postgres::types::Type) -> bool
    where
        Self: Sized,
    {
        // we don't need type validation, arrays cannot contain wrong types
        true
    }

    to_sql_checked!();
}

impl Consume for BytesMut {}

macro_rules! impl_consume_ty {
    ($ArrTy: ty, $to_sql: ident) => {
        impl ConsumeTy<$ArrTy> for BytesMut {
            fn consume(&mut self, value: <$ArrTy as crate::types::ArrowType>::Native) {
                postgres_protocol::types::$to_sql(value, self);
            }

            fn consume_null(&mut self) {}
        }
    };
}

macro_rules! impl_consume_ref_ty {
    ($ArrTy: ty, $to_sql: ident) => {
        impl ConsumeTy<$ArrTy> for BytesMut {
            fn consume(&mut self, value: <$ArrTy as crate::types::ArrowType>::Native) {
                postgres_protocol::types::$to_sql(&value, self);
            }

            fn consume_null(&mut self) {}
        }
    };
}

impl ConsumeTy<NullType> for BytesMut {
    fn consume(&mut self, _: ()) {}

    fn consume_null(&mut self) {}
}

impl_consume_ty!(BooleanType, bool_to_sql);
impl_consume_ty!(Int8Type, char_to_sql);
impl_consume_ty!(Int16Type, int2_to_sql);
impl_consume_ty!(Int32Type, int4_to_sql);
impl_consume_ty!(Int64Type, int8_to_sql);
// impl_consume_ty!(UInt8Type,  );
// impl_consume_ty!(UInt16Type,  );
impl_consume_ty!(UInt32Type, oid_to_sql);
// impl_consume_ty!(UInt64Type,  );
// impl_consume_ty!(Float16Type,  );
impl_consume_ty!(Float32Type, float4_to_sql);
impl_consume_ty!(Float64Type, float8_to_sql);
// impl_consume_ty!(TimestampSecondType,  );
// impl_consume_ty!(TimestampMillisecondType,  );
impl_consume_ty!(TimestampMicrosecondType, timestamp_to_sql);
// impl_consume_ty!(TimestampNanosecondType,  );
// impl_consume_ty!(Date32Type, date_to_sql);
// impl_consume_ty!(Date64Type, date_to_sql);
// impl_consume_ty!(Time32SecondType,  );
// impl_consume_ty!(Time32MillisecondType,  );
impl_consume_ty!(Time64MicrosecondType, time_to_sql);
// impl_consume_ty!(Time64NanosecondType,  );
// impl_consume_ty!(IntervalYearMonthType,  );
// impl_consume_ty!(IntervalDayTimeType,  );
// impl_consume_ty!(IntervalMonthDayNanoType,  );
// impl_consume_ty!(DurationSecondType,  );
// impl_consume_ty!(DurationMillisecondType,  );
// impl_consume_ty!(DurationMicrosecondType,  );
// impl_consume_ty!(DurationNanosecondType,  );
impl_consume_ref_ty!(BinaryType, bytea_to_sql);
impl_consume_ref_ty!(LargeBinaryType, bytea_to_sql);
impl_consume_ref_ty!(FixedSizeBinaryType, bytea_to_sql);
impl_consume_ref_ty!(Utf8Type, text_to_sql);
impl_consume_ref_ty!(LargeUtf8Type, text_to_sql);
// impl_consume_ty!(Decimal128Type,  );
// impl_consume_ty!(Decimal256Type,  );

impl_consume_unsupported!(
    BytesMut,
    (
        UInt8Type,
        UInt16Type,
        UInt64Type,
        Float16Type,
        TimestampSecondType,
        TimestampMillisecondType,
        TimestampNanosecondType,
        Date32Type,
        Date64Type,
        Time32SecondType,
        Time32MillisecondType,
        Time64NanosecondType,
        IntervalYearMonthType,
        IntervalDayTimeType,
        IntervalMonthDayNanoType,
        DurationSecondType,
        DurationMillisecondType,
        DurationMicrosecondType,
        DurationNanosecondType,
        Decimal128Type,
        Decimal256Type,
    )
);
