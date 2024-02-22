use arrow::datatypes::*;
use std::any::Any;

use crate::api::ArrowValue;
use crate::types::{FixedSizeBinaryType, NullType};
use crate::util::transport::{Produce, ProduceTy};
use crate::{impl_produce_unsupported, ConnectorError};

impl<'r> Produce<'r> for &'r dyn ArrowValue {}

macro_rules! impl_arrow_value {
    ($native: ty, $dyn_ty: expr, $stat_ty: ty, $const: ident) => {
        const $const: DataType = $dyn_ty;

        impl $crate::api::sealed::Sealed for $native {}

        impl ArrowValue for $native {
            fn get_data_type(&self) -> &DataType {
                &$const
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }
        impl<'r> ProduceTy<'r, $stat_ty> for &'r dyn ArrowValue {
            fn produce(
                self,
            ) -> Result<<$stat_ty as $crate::types::ArrowType>::Native, ConnectorError> {
                Ok(self.as_any().downcast_ref::<$native>().unwrap().clone())
            }
            fn produce_opt(
                self,
            ) -> Result<Option<<$stat_ty as $crate::types::ArrowType>::Native>, ConnectorError>
            {
                Ok(Some(
                    self.as_any().downcast_ref::<$native>().unwrap().clone(),
                ))
            }
        }
    };
}

impl_arrow_value!(bool, DataType::Boolean, BooleanType, BOOLEAN);
impl_arrow_value!(i8, DataType::Int8, Int8Type, INT8);
impl_arrow_value!(i16, DataType::Int16, Int16Type, INT16);
impl_arrow_value!(i32, DataType::Int32, Int32Type, INT32);
impl_arrow_value!(i64, DataType::Int64, Int64Type, INT64);
impl_arrow_value!(u8, DataType::UInt8, UInt8Type, UINT8);
impl_arrow_value!(u16, DataType::UInt16, UInt16Type, UINT16);
impl_arrow_value!(u32, DataType::UInt32, UInt32Type, UINT32);
impl_arrow_value!(u64, DataType::UInt64, UInt64Type, UINT64);
impl_arrow_value!(f32, DataType::Float32, Float32Type, FLOAT32);
impl_arrow_value!(f64, DataType::Float64, Float64Type, FLOAT64);
impl_arrow_value!(Vec<u8>, DataType::Binary, BinaryType, BINARY);
impl_arrow_value!(String, DataType::Utf8, Utf8Type, UTF8);

impl_produce_unsupported!(
    &'r dyn ArrowValue,
    (
        NullType,
        Float16Type,
        // BinaryType,
        // Utf8Type,
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
        LargeBinaryType,
        FixedSizeBinaryType,
        LargeUtf8Type,
        Decimal128Type,
        Decimal256Type,
    )
);
