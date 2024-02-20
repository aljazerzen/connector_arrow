use arrow::array::*;
use arrow::datatypes::*;
use half::f16;
use rand::Rng;
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
    ($Values: expr, $Builder: ty, $Low: expr, $High: expr, $RandomUniform: expr) => {{
        let mut builder = <$Builder>::with_capacity(count_values($Values));
        for value in $Values {
            for _ in 0..value.repeat {
                match value.gen_process {
                    ValueGenProcess::Null => builder.append_null(),
                    ValueGenProcess::Low => builder.append_value($Low),
                    ValueGenProcess::High => builder.append_value($High),
                    ValueGenProcess::RandomUniform => builder.append_value($RandomUniform),
                }
            }
        }
        Arc::new(builder.finish()) as ArrayRef
    }};
}

fn generate_array<R: Rng>(data_type: &DataType, values: &[ValuesSpec], rng: &mut R) -> ArrayRef {
    match data_type {
        DataType::Null => {
            let mut builder = NullBuilder::with_capacity(count_values(values));
            Arc::new(builder.finish()) as ArrayRef
        }
        DataType::Boolean => {
            gen_array![values, BooleanBuilder, false, true, rng.gen_bool(0.5)]
        }
        DataType::Int8 => {
            gen_array![
                values,
                Int8Builder,
                i8::MIN,
                i8::MAX,
                rng.gen_range(i8::MIN..=i8::MAX)
            ]
        }
        DataType::Int16 => {
            gen_array![
                values,
                Int16Builder,
                i16::MIN,
                i16::MAX,
                rng.gen_range(i16::MIN..=i16::MAX)
            ]
        }
        DataType::Int32 => {
            gen_array![
                values,
                Int32Builder,
                i32::MIN,
                i32::MAX,
                rng.gen_range(i32::MIN..=i32::MAX)
            ]
        }
        DataType::Int64 => {
            gen_array![
                values,
                Int64Builder,
                i64::MIN,
                i64::MAX,
                rng.gen_range(i64::MIN..=i64::MAX)
            ]
        }
        DataType::UInt8 => {
            gen_array![
                values,
                UInt8Builder,
                u8::MIN,
                u8::MAX,
                rng.gen_range(u8::MIN..=u8::MAX)
            ]
        }
        DataType::UInt16 => {
            gen_array![
                values,
                UInt16Builder,
                u16::MIN,
                u16::MAX,
                rng.gen_range(u16::MIN..=u16::MAX)
            ]
        }
        DataType::UInt32 => {
            gen_array![
                values,
                UInt32Builder,
                u32::MIN,
                u32::MAX,
                rng.gen_range(u32::MIN..=u32::MAX)
            ]
        }
        DataType::UInt64 => {
            gen_array![
                values,
                UInt64Builder,
                u64::MIN,
                u64::MAX,
                rng.gen_range(u64::MIN..=u64::MAX)
            ]
        }
        DataType::Float16 => {
            gen_array![
                values,
                Float16Builder,
                f16::MIN,
                f16::MAX,
                f16::from_bits(rng.gen_range(u16::MIN..=u16::MAX))
            ]
        }
        DataType::Float32 => {
            gen_array![
                values,
                Float32Builder,
                f32::MIN,
                f32::MAX,
                rng.gen::<f32>() // TODO: this is standard instead of uniform
            ]
        }
        DataType::Float64 => {
            gen_array![
                values,
                Float64Builder,
                f64::MIN,
                f64::MAX,
                rng.gen::<f64>() // TODO: this is standard instead of uniform
            ]
        }
        DataType::Timestamp(_, _) => {
            let array = gen_array![
                values,
                TimestampMicrosecondBuilder,
                i64::MIN,
                i64::MAX,
                rng.gen_range(i64::MIN..=i64::MAX)
            ];
            arrow::compute::cast(&array, data_type).unwrap()
        }
        DataType::Date32 => {
            gen_array![
                values,
                Date32Builder,
                i32::MIN,
                i32::MAX,
                rng.gen_range(i32::MIN..=i32::MAX)
            ]
        }
        DataType::Date64 => {
            gen_array![
                values,
                Date64Builder,
                i64::MIN,
                i64::MAX,
                rng.gen_range(i64::MIN..=i64::MAX)
            ]
        }
        DataType::Time32(_) => {
            let array = gen_array![
                values,
                PrimitiveBuilder<Int32Type>,
                i32::MIN,
                i32::MAX,
                rng.gen_range(i32::MIN..=i32::MAX)
            ];
            arrow::compute::cast(&array, data_type).unwrap()
        }
        DataType::Time64(_) => {
            let array = gen_array![
                values,
                PrimitiveBuilder<Int64Type>,
                i64::MIN,
                i64::MAX,
                rng.gen_range(i64::MIN..=i64::MAX)
            ];
            arrow::compute::cast(&array, data_type).unwrap()
        }
        DataType::Duration(_) => {
            let array = gen_array![
                values,
                PrimitiveBuilder<Int64Type>,
                i64::MIN,
                i64::MAX,
                rng.gen_range(i64::MIN..=i64::MAX)
            ];
            arrow::compute::cast(&array, data_type).unwrap()
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            gen_array![
                values,
                IntervalYearMonthBuilder,
                i32::MIN,
                i32::MAX,
                rng.gen_range(i32::MIN..=i32::MAX)
            ]
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            gen_array![
                values,
                IntervalMonthDayNanoBuilder,
                i128::MIN,
                i128::MAX,
                rng.gen_range(i128::MIN..=i128::MAX)
            ]
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            gen_array![
                values,
                IntervalDayTimeBuilder,
                i64::MIN,
                i64::MAX,
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
        DataType::Decimal128(_, _) => todo!(),
        DataType::Decimal256(_, _) => todo!(),
        DataType::Map(_, _) => todo!(),
        DataType::RunEndEncoded(_, _) => todo!(),
    }
}
