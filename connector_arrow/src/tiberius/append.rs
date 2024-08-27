use std::borrow::Cow;
use std::sync::Arc;

use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use futures::{AsyncRead, AsyncWrite};
use itertools::{zip_eq, Itertools};
use tiberius::numeric::Numeric;
use tiberius::{BulkLoadRequest, Client, ColumnData, TokenRow};
use tokio::runtime::Runtime;

use crate::api::Append;
use crate::types::{FixedSizeBinaryType, NullType};
use crate::util::escape::escaped_ident;
use crate::util::transport::{Consume, ConsumeTy};
use crate::util::ArrayCellRef;
use crate::{impl_consume_unsupported, ConnectorError};

pub struct TiberiusAppender<'c, S: AsyncRead + AsyncWrite + Unpin + Send> {
    rt: Arc<Runtime>,
    bulk_load: BulkLoadRequest<'c, S>,
}

impl<'conn, S: AsyncRead + AsyncWrite + Unpin + Send> TiberiusAppender<'conn, S> {
    pub fn new(
        rt: Arc<Runtime>,
        client: &'conn mut Client<S>,
        table_name: &str,
    ) -> Result<Self, ConnectorError> {
        let table_name = escaped_ident(table_name).to_string();

        // Tiberius requires table_name to be 'conn, but does not really use it as such.
        // We convert our '_ into 'conn here.
        let table_name: &'conn str = unsafe { std::mem::transmute::<_, _>(table_name.as_str()) };

        let bulk_load = client.bulk_insert(table_name);
        let bulk_load = rt.block_on(bulk_load)?;

        Ok(Self { rt, bulk_load })
    }
}

impl<'conn, S: AsyncRead + AsyncWrite + Unpin + Send> Append<'conn> for TiberiusAppender<'conn, S> {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        let schema = batch.schema();
        let mut row_ref = zip_eq(batch.columns(), schema.fields())
            .map(|(array, field)| ArrayCellRef {
                array,
                field,
                row_number: 0,
            })
            .collect_vec();

        for row_number in 0..batch.num_rows() {
            let mut tb_row = TokenRow::with_capacity(row_ref.len());
            for cell_ref in &mut row_ref {
                cell_ref.row_number = row_number;

                crate::util::transport::transport(cell_ref.field, &*cell_ref, &mut tb_row)?;
            }

            let f = self.bulk_load.send(tb_row);
            self.rt.block_on(f)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<(), ConnectorError> {
        let res = self.bulk_load.finalize();
        self.rt.block_on(res)?;
        Ok(())
    }
}

impl Consume for TokenRow<'static> {}

macro_rules! impl_consume_ty {
    ($ArrTy: ty, $variant: ident) => {
        impl_consume_ty!($ArrTy, $variant, std::convert::identity);
    };

    ($ArrTy: ty, $variant: ident, $conversion: expr) => {
        impl ConsumeTy<$ArrTy> for TokenRow<'static> {
            fn consume(
                &mut self,
                _ty: &DataType,
                value: <$ArrTy as crate::types::ArrowType>::Native,
            ) {
                self.push(ColumnData::$variant(Some(($conversion)(value))))
            }

            fn consume_null(&mut self) {
                self.push(ColumnData::$variant(None))
            }
        }
    };
}

impl ConsumeTy<NullType> for TokenRow<'static> {
    fn consume(&mut self, _ty: &DataType, _: ()) {
        self.push(ColumnData::U8(None))
    }

    fn consume_null(&mut self) {
        self.push(ColumnData::U8(None))
    }
}

impl_consume_ty!(BooleanType, Bit);
impl_consume_ty!(Int8Type, I16, i16::from);
impl_consume_ty!(Int16Type, I16);
impl_consume_ty!(Int32Type, I32);
impl_consume_ty!(Int64Type, I64);
impl_consume_ty!(UInt8Type, U8);
impl_consume_ty!(UInt16Type, I32, i32::from);
impl_consume_ty!(UInt32Type, I64, i64::from);
impl_consume_ty!(UInt64Type, Numeric, u64_to_numeric);
impl_consume_ty!(Float16Type, F32, f32::from);
impl_consume_ty!(Float32Type, F32);
impl_consume_ty!(Float64Type, F64);
impl_consume_ty!(Utf8Type, String, Cow::from);
impl_consume_ty!(LargeUtf8Type, String, Cow::from);

impl_consume_unsupported!(
    TokenRow<'static>,
    (
        Decimal128Type,
        Decimal256Type,
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
        DurationSecondType,
        DurationMillisecondType,
        DurationMicrosecondType,
        DurationNanosecondType,
        IntervalYearMonthType,
        IntervalDayTimeType,
        IntervalMonthDayNanoType,
        BinaryType,
        LargeBinaryType,
        FixedSizeBinaryType,
    )
);

fn u64_to_numeric(val: u64) -> Numeric {
    Numeric::new_with_scale(i128::from(val), 0)
}
