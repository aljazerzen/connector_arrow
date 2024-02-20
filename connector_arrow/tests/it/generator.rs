use arrow::array::*;
use arrow::datatypes::*;
use half::f16;
use rand::Rng;
use std::sync::Arc;

pub fn generate_batch<R: Rng>(column_specs: Vec<ColumnSpec>, rng: &mut R) -> RecordBatch {
    let mut arrays = Vec::new();
    let mut fields = Vec::new();
    for column in column_specs {
        let array = generate_array(&column.data_type, &column.values, rng);
        arrays.push(array);

        let field = Field::new(column.field_name, column.data_type, column.is_nullable);
        fields.push(field);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).unwrap()
}

pub fn spec_simple() -> Vec<ColumnSpec> {
    domains_to_batch_spec(&[DataType::Null, DataType::Boolean], &[false, true])
}

pub fn spec_numeric() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
        ],
        &[false, true],
    )
}

pub fn spec_timestamp() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+07:30"))),
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+07:30"))),
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("+07:30"))),
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("+07:30"))),
        ],
        &[true],
    )
}
pub fn spec_date() -> Vec<ColumnSpec> {
    domains_to_batch_spec(&[DataType::Date32, DataType::Date64], &[true])
}
pub fn spec_time() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Microsecond),
        ],
        &[true],
    )
}
pub fn spec_duration() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Second),
        ],
        &[true],
    )
}
pub fn spec_interval() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Interval(IntervalUnit::DayTime),
        ],
        &[true],
    )
}

pub fn domains_to_batch_spec(
    data_types_domain: &[DataType],
    is_nullable_domain: &[bool],
) -> Vec<ColumnSpec> {
    let value_gen_process_domain = [
        ValueGenProcess::Low,
        ValueGenProcess::High,
        ValueGenProcess::Null,
        ValueGenProcess::RandomUniform,
    ];

    let mut columns = Vec::new();
    for data_type in data_types_domain {
        for is_nullable in is_nullable_domain {
            let is_nullable = *is_nullable;
            if matches!(data_type, &DataType::Null) && !is_nullable {
                continue;
            }

            let mut field_name = data_type.to_string();
            if is_nullable {
                field_name += "_null";
            }
            let mut col = ColumnSpec {
                field_name,
                data_type: data_type.clone(),
                is_nullable,
                values: Vec::new(),
            };

            for gen_process in value_gen_process_domain {
                col.values.push(ValuesSpec {
                    gen_process: if matches!(gen_process, ValueGenProcess::Null) && !is_nullable {
                        ValueGenProcess::RandomUniform
                    } else {
                        gen_process
                    },
                    repeat: 1,
                });
            }
            columns.push(col);
        }
    }
    columns
}

#[derive(Clone, Copy)]
enum ValueGenProcess {
    Null,
    Low,
    High,
    RandomUniform,
}

struct ValuesSpec {
    gen_process: ValueGenProcess,
    repeat: usize,
}

pub struct ColumnSpec {
    field_name: String,
    is_nullable: bool,
    data_type: DataType,
    values: Vec<ValuesSpec>,
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
