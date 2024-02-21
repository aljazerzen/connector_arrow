use arrow::array::*;
use arrow::datatypes::*;
use half::f16;
use rand::Rng;
use std::ops::Neg;
use std::sync::Arc;

use super::spec::*;

pub fn generate_batch<R: Rng>(
    column_specs: ArrowGenSpec,
    rng: &mut R,
) -> (SchemaRef, Vec<RecordBatch>) {
    let mut arrays = Vec::new();
    let mut fields = Vec::new();
    for column in column_specs {
        let array = generate_array(&column.data_type, &column.values, rng);
        arrays.push(array);

        let field = Field::new(column.field_name, column.data_type, column.is_nullable);
        fields.push(field);
    }
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    if batch.num_rows() == 0 {
        (schema, vec![])
    } else {
        (schema, vec![batch])
    }
}

fn count_values(values: &[ValuesSpec]) -> usize {
    values.iter().map(|v| v.repeat).sum()
}

#[macro_export]
macro_rules! gen_array {
    ($Values: expr, $Builder: expr, $Low: expr, $High: expr, $Unit: expr, $RandomUniform: expr) => {{
        let mut builder = $Builder;
        for value in $Values {
            for _ in 0..value.repeat {
                match value.gen_process {
                    ValueGenProcess::Null => builder.append_null(),
                    ValueGenProcess::Low => builder.append_value($Low),
                    ValueGenProcess::High => builder.append_value($High),
                    ValueGenProcess::Unit => builder.append_value($Unit),
                    ValueGenProcess::RandomUniform => builder.append_value($RandomUniform),
                }
            }
        }
        Arc::new(builder.finish()) as ArrayRef
    }};
}

fn generate_array<R: Rng>(data_type: &DataType, values: &[ValuesSpec], rng: &mut R) -> ArrayRef {
    let capacity = count_values(values);
    match data_type {
        DataType::Null => {
            let mut builder = NullBuilder::with_capacity(capacity);
            Arc::new(builder.finish()) as ArrayRef
        }
        DataType::Boolean => {
            gen_array![
                values,
                BooleanBuilder::with_capacity(capacity),
                false,
                true,
                false,
                rng.gen_bool(0.5)
            ]
        }
        DataType::Int8 => {
            gen_array![
                values,
                Int8Builder::with_capacity(capacity),
                i8::MIN,
                i8::MAX,
                0,
                rng.gen_range(i8::MIN..=i8::MAX)
            ]
        }
        DataType::Int16 => {
            gen_array![
                values,
                Int16Builder::with_capacity(capacity),
                i16::MIN,
                i16::MAX,
                0,
                rng.gen_range(i16::MIN..=i16::MAX)
            ]
        }
        DataType::Int32 => {
            gen_array![
                values,
                Int32Builder::with_capacity(capacity),
                i32::MIN,
                i32::MAX,
                0,
                rng.gen_range(i32::MIN..=i32::MAX)
            ]
        }
        DataType::Int64 => {
            gen_array![
                values,
                Int64Builder::with_capacity(capacity),
                i64::MIN,
                i64::MAX,
                0,
                rng.gen_range(i64::MIN..=i64::MAX)
            ]
        }
        DataType::UInt8 => {
            gen_array![
                values,
                UInt8Builder::with_capacity(capacity),
                u8::MIN,
                u8::MAX,
                0,
                rng.gen_range(u8::MIN..=u8::MAX)
            ]
        }
        DataType::UInt16 => {
            gen_array![
                values,
                UInt16Builder::with_capacity(capacity),
                u16::MIN,
                u16::MAX,
                0,
                rng.gen_range(u16::MIN..=u16::MAX)
            ]
        }
        DataType::UInt32 => {
            gen_array![
                values,
                UInt32Builder::with_capacity(capacity),
                u32::MIN,
                u32::MAX,
                0,
                rng.gen_range(u32::MIN..=u32::MAX)
            ]
        }
        DataType::UInt64 => {
            gen_array![
                values,
                UInt64Builder::with_capacity(capacity),
                u64::MIN,
                u64::MAX,
                0,
                rng.gen_range(u64::MIN..=u64::MAX)
            ]
        }
        DataType::Float16 => {
            gen_array![
                values,
                Float16Builder::with_capacity(capacity),
                f16::MIN,
                f16::MAX,
                f16::ZERO,
                f16::from_bits(rng.gen_range(u16::MIN..=u16::MAX))
            ]
        }
        DataType::Float32 => {
            gen_array![
                values,
                Float32Builder::with_capacity(capacity),
                f32::MIN,
                f32::MAX,
                f32::ZERO,
                rng.gen::<f32>() // TODO: this is standard instead of uniform
            ]
        }
        DataType::Float64 => {
            gen_array![
                values,
                Float64Builder::with_capacity(capacity),
                f64::MIN,
                f64::MAX,
                f64::ZERO,
                rng.gen::<f64>() // TODO: this is standard instead of uniform
            ]
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            gen_array![
                values,
                TimestampNanosecondBuilder::with_capacity(capacity)
                    .with_data_type(data_type.clone()),
                i64::MIN,
                i64::MAX,
                i64::ZERO,
                rng.gen_range(i64::MIN..=i64::MAX)
            ]
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            gen_array![
                values,
                TimestampMicrosecondBuilder::with_capacity(capacity)
                    .with_data_type(data_type.clone()),
                i64::MIN,
                i64::MAX,
                i64::ZERO,
                rng.gen_range(i64::MIN..=i64::MAX)
            ]
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            gen_array![
                values,
                TimestampMillisecondBuilder::with_capacity(capacity)
                    .with_data_type(data_type.clone()),
                i64::MIN,
                i64::MAX,
                i64::ZERO,
                rng.gen_range(i64::MIN..=i64::MAX)
            ]
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            gen_array![
                values,
                TimestampSecondBuilder::with_capacity(capacity).with_data_type(data_type.clone()),
                i64::MIN,
                i64::MAX,
                i64::ZERO,
                rng.gen_range(i64::MIN..=i64::MAX)
            ]
        }
        DataType::Date32 => {
            gen_array![
                values,
                Date32Builder::with_capacity(capacity),
                i32::MIN,
                i32::MAX,
                0,
                rng.gen_range(i32::MIN..=i32::MAX)
            ]
        }
        DataType::Date64 => {
            gen_array![
                values,
                Date64Builder::with_capacity(capacity),
                i64::MIN,
                i64::MAX,
                0,
                rng.gen_range(i64::MIN..=i64::MAX)
            ]
        }
        DataType::Time32(_) => {
            let array = gen_array![
                values,
                PrimitiveBuilder::<Int32Type>::with_capacity(capacity),
                i32::MIN,
                i32::MAX,
                0,
                rng.gen_range(i32::MIN..=i32::MAX)
            ];
            arrow::compute::cast(&array, data_type).unwrap()
        }
        DataType::Time64(_) => {
            let array = gen_array![
                values,
                PrimitiveBuilder::<Int64Type>::with_capacity(capacity),
                i64::MIN,
                i64::MAX,
                0,
                rng.gen_range(i64::MIN..=i64::MAX)
            ];
            arrow::compute::cast(&array, data_type).unwrap()
        }
        DataType::Duration(_) => {
            let array = gen_array![
                values,
                PrimitiveBuilder::<Int64Type>::with_capacity(capacity),
                i64::MIN,
                i64::MAX,
                0,
                rng.gen_range(i64::MIN..=i64::MAX)
            ];
            arrow::compute::cast(&array, data_type).unwrap()
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            gen_array![
                values,
                IntervalYearMonthBuilder::with_capacity(capacity),
                i32::MIN,
                i32::MAX,
                0,
                rng.gen_range(i32::MIN..=i32::MAX)
            ]
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            gen_array![
                values,
                IntervalMonthDayNanoBuilder::with_capacity(capacity),
                i128::MIN,
                i128::MAX,
                0,
                rng.gen_range(i128::MIN..=i128::MAX)
            ]
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            gen_array![
                values,
                IntervalDayTimeBuilder::with_capacity(capacity),
                i64::MIN,
                i64::MAX,
                0,
                rng.gen_range(i64::MIN..=i64::MAX)
            ]
        }
        DataType::Binary => todo!(),
        DataType::FixedSizeBinary(_) => todo!(),
        DataType::LargeBinary => todo!(),
        DataType::Utf8 => todo!(),
        DataType::LargeUtf8 => todo!(),
        DataType::List(_) => todo!(),
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::LargeList(_) => todo!(),
        DataType::Struct(_) => todo!(),
        DataType::Union(_, _) => todo!(),
        DataType::Dictionary(_, _) => todo!(),
        DataType::Decimal128(precision, _) => {
            let max = if *precision == 38 {
                999_99999_99999_99999_99999_99999_99999_99999i128
            } else {
                10i128.pow(*precision as u32) - 1
            };
            let min = -max;
            gen_array![
                values,
                Decimal128Builder::with_capacity(capacity).with_data_type(data_type.clone()),
                min,
                max,
                0,
                rng.gen_range(min..=max)
            ]
        }
        DataType::Decimal256(precision, _) => {
            let max = i256::from_i128(10i128)
                .pow_wrapping(*precision as u32)
                .sub_checked(i256::from_i128(1))
                .unwrap();

            let min = max.neg();
            let (min_low, min_high) = min.to_parts();
            let (max_low, max_high) = max.to_parts();

            let (min_low, max_low) = if min_low < max_low {
                (min_low, max_low)
            } else {
                (max_low, min_low)
            };
            let (min_high, max_high) = if min_high < max_high {
                (min_high, max_high)
            } else {
                (max_high, min_high)
            };

            gen_array![
                values,
                Decimal256Builder::with_capacity(capacity).with_data_type(data_type.clone()),
                min,
                max,
                i256::ZERO,
                i256::from_parts(
                    rng.gen_range(min_low..=max_low),
                    rng.gen_range(min_high..=max_high)
                )
            ]
        }
        DataType::Map(_, _) => todo!(),
        DataType::RunEndEncoded(_, _) => todo!(),
    }
}
