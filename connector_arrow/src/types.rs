use arrow::datatypes::*;

/// For a given arrow type, this trait associates:
/// - the Rust type that represents it (i.e. [Int8Type]),
/// - its Rust-native representation (i.e. [i8]), which might not be unique.
pub trait ArrowType {
    type Native: Sized;
}

// arrow crate does not define null type (as it contains no data),
// but we need it for [Consume]
pub struct NullType;

// arrow crate does not define fixed-sized binary array type
pub struct FixedSizeBinaryType;

impl ArrowType for NullType {
    type Native = ();
}
impl ArrowType for BooleanType {
    type Native = bool;
}
macro_rules! impl_arrow_primitive_type {
    ($($t: ty,)+) => {
        $(
            impl ArrowType for $t {
                type Native = <Self as ArrowPrimitiveType>::Native;
            }
        )+
    };
}
impl_arrow_primitive_type!(
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    UInt8Type,
    UInt16Type,
    UInt32Type,
    UInt64Type,
    Float16Type,
    Float32Type,
    Float64Type,
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
    Decimal128Type,
    Decimal256Type,
);
impl ArrowType for BinaryType {
    type Native = Vec<u8>;
}
impl ArrowType for LargeBinaryType {
    type Native = Vec<u8>;
}
impl ArrowType for FixedSizeBinaryType {
    type Native = Vec<u8>;
}
impl ArrowType for Utf8Type {
    type Native = String;
}
impl ArrowType for LargeUtf8Type {
    type Native = String;
}
