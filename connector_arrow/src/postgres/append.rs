use std::sync::Arc;

use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use bytes::BytesMut;
use itertools::{zip_eq, Itertools};
use postgres::binary_copy::BinaryCopyInWriter;
use postgres::types::{to_sql_checked, IsNull, ToSql};
use postgres::{Client, CopyInWriter};
use postgres_protocol::types as postgres_proto;

use crate::api::Append;
use crate::types::{FixedSizeBinaryType, NullType};
use crate::util::escape::escaped_ident;
use crate::util::transport::{Consume, ConsumeTy};
use crate::util::ArrayCellRef;
use crate::{impl_consume_unsupported, ConnectorError};

use super::PostgresError;

pub struct PostgresAppender<'c> {
    writer: Writer<'c>,
}

impl<'conn> PostgresAppender<'conn> {
    pub fn new(client: &'conn mut Client, table_name: &str) -> Result<Self, ConnectorError> {
        let query = format!("COPY BINARY {} FROM stdin", escaped_ident(table_name));
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
    ($ArrTy: ty, $to_sql: expr) => {
        impl_consume_ty!($ArrTy, $to_sql, std::convert::identity);
    };

    ($ArrTy: ty, $to_sql: expr, $conversion: expr) => {
        impl ConsumeTy<$ArrTy> for BytesMut {
            fn consume(
                &mut self,
                _ty: &DataType,
                value: <$ArrTy as crate::types::ArrowType>::Native,
            ) {
                $to_sql(($conversion)(value), self);
            }

            fn consume_null(&mut self, _ty: &DataType) {}
        }
    };
}

macro_rules! impl_consume_ref_ty {
    ($ArrTy: ty, $to_sql: expr) => {
        impl ConsumeTy<$ArrTy> for BytesMut {
            fn consume(
                &mut self,
                _ty: &DataType,
                value: <$ArrTy as crate::types::ArrowType>::Native,
            ) {
                $to_sql(&value, self);
            }

            fn consume_null(&mut self, _ty: &DataType) {}
        }
    };
}

impl ConsumeTy<NullType> for BytesMut {
    fn consume(&mut self, _ty: &DataType, _: ()) {}

    fn consume_null(&mut self, _ty: &DataType) {}
}

impl_consume_ty!(BooleanType, postgres_proto::bool_to_sql);
impl_consume_ty!(Int8Type, postgres_proto::int2_to_sql, i16::from);
impl_consume_ty!(Int16Type, postgres_proto::int2_to_sql);
impl_consume_ty!(Int32Type, postgres_proto::int4_to_sql);
impl_consume_ty!(Int64Type, postgres_proto::int8_to_sql);
impl_consume_ty!(UInt8Type, postgres_proto::int2_to_sql, i16::from);
impl_consume_ty!(UInt16Type, postgres_proto::int4_to_sql, i32::from);
impl_consume_ty!(UInt32Type, postgres_proto::int8_to_sql, i64::from);
impl_consume_ty!(Float16Type, postgres_proto::float4_to_sql, f32::from);
impl_consume_ty!(Float32Type, postgres_proto::float4_to_sql);
impl_consume_ty!(Float64Type, postgres_proto::float8_to_sql);
impl_consume_ty!(TimestampSecondType, postgres_proto::int8_to_sql);
impl_consume_ty!(TimestampMillisecondType, postgres_proto::int8_to_sql);
impl_consume_ty!(TimestampMicrosecondType, postgres_proto::int8_to_sql);
impl_consume_ty!(TimestampNanosecondType, postgres_proto::int8_to_sql);
impl_consume_ty!(Date32Type, postgres_proto::int4_to_sql);
impl_consume_ty!(Date64Type, postgres_proto::int8_to_sql);
impl_consume_ty!(Time32SecondType, postgres_proto::int4_to_sql);
impl_consume_ty!(Time32MillisecondType, postgres_proto::int4_to_sql);
impl_consume_ty!(Time64MicrosecondType, postgres_proto::int8_to_sql);
impl_consume_ty!(Time64NanosecondType, postgres_proto::int8_to_sql);
impl_consume_ty!(DurationSecondType, postgres_proto::int8_to_sql);
impl_consume_ty!(DurationMillisecondType, postgres_proto::int8_to_sql);
impl_consume_ty!(DurationMicrosecondType, postgres_proto::int8_to_sql);
impl_consume_ty!(DurationNanosecondType, postgres_proto::int8_to_sql);

// impl_consume_ty!(IntervalYearMonthType,  );
// impl_consume_ty!(IntervalDayTimeType,  );
// impl_consume_ty!(IntervalMonthDayNanoType,  );

impl_consume_ref_ty!(BinaryType, postgres_proto::bytea_to_sql);
impl_consume_ref_ty!(LargeBinaryType, postgres_proto::bytea_to_sql);
impl_consume_ref_ty!(FixedSizeBinaryType, postgres_proto::bytea_to_sql);
impl_consume_ref_ty!(Utf8Type, postgres_proto::text_to_sql);
impl_consume_ref_ty!(LargeUtf8Type, postgres_proto::text_to_sql);

impl ConsumeTy<UInt64Type> for BytesMut {
    fn consume(&mut self, _ty: &DataType, value: u64) {
        // this is inefficient, we'd need a special u64_to_sql function
        super::decimal::i128_to_sql(value as i128, 0, self)
    }

    fn consume_null(&mut self, _ty: &DataType) {}
}

impl ConsumeTy<Decimal128Type> for BytesMut {
    fn consume(&mut self, ty: &DataType, value: i128) {
        let DataType::Decimal128(_, scale) = ty else {
            unreachable!()
        };

        super::decimal::i128_to_sql(value, *scale, self)
    }

    fn consume_null(&mut self, _ty: &DataType) {}
}

impl ConsumeTy<Decimal256Type> for BytesMut {
    fn consume(&mut self, ty: &DataType, value: i256) {
        let DataType::Decimal256(_, scale) = ty else {
            unreachable!()
        };

        super::decimal::i256_to_sql(value, *scale, self)
    }

    fn consume_null(&mut self, _ty: &DataType) {}
}

impl_consume_unsupported!(
    BytesMut,
    (
        IntervalYearMonthType,
        IntervalDayTimeType,
        IntervalMonthDayNanoType,
    )
);
