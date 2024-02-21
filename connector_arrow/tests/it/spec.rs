use arrow::datatypes::*;
use std::sync::Arc;

pub type ArrowGenSpec = Vec<ColumnSpec>;

#[derive(Clone, Copy)]
pub enum ValueGenProcess {
    Null,
    Low,
    High,
    Unit,
    RandomUniform,
}

const VALUE_GEN_PROCESS_ALL: [ValueGenProcess; 5] = [
    ValueGenProcess::Low,
    ValueGenProcess::High,
    ValueGenProcess::Unit,
    ValueGenProcess::Null,
    ValueGenProcess::RandomUniform,
];

pub struct ValuesSpec {
    pub gen_process: ValueGenProcess,
    pub repeat: usize,
}

pub struct ColumnSpec {
    pub field_name: String,
    pub is_nullable: bool,
    pub data_type: DataType,
    pub values: Vec<ValuesSpec>,
}

pub fn all_types() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Null,
            DataType::Boolean,
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
            // DataType::Timestamp(TimeUnit::Nanosecond, None),
            // DataType::Timestamp(TimeUnit::Microsecond, None),
            // DataType::Timestamp(TimeUnit::Millisecond, None),
            // DataType::Timestamp(TimeUnit::Second, None),
            // DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+07:30"))),
            // DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+07:30"))),
            // DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("+07:30"))),
            // DataType::Timestamp(TimeUnit::Second, Some(Arc::from("+07:30"))),
            // DataType::Time32(TimeUnit::Millisecond),
            // DataType::Time32(TimeUnit::Second),
            // DataType::Time64(TimeUnit::Nanosecond),
            // DataType::Time64(TimeUnit::Microsecond),
            // DataType::Duration(TimeUnit::Nanosecond),
            // DataType::Duration(TimeUnit::Microsecond),
            // DataType::Duration(TimeUnit::Millisecond),
            // DataType::Duration(TimeUnit::Second),
            // DataType::Interval(IntervalUnit::YearMonth),
            // DataType::Interval(IntervalUnit::MonthDayNano),
            // DataType::Interval(IntervalUnit::DayTime),
        ],
        &[false, true],
        &[ValueGenProcess::High],
    )
}

pub fn empty() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[DataType::Null, DataType::Int64, DataType::Float64],
        &[false, true],
        &[],
    )
}

pub fn null_bool() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[DataType::Null, DataType::Boolean],
        &[false, true],
        &VALUE_GEN_PROCESS_ALL,
    )
}

pub fn int() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ],
        &[false, true],
        &VALUE_GEN_PROCESS_ALL,
    )
}

pub fn uint() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ],
        &[false, true],
        &VALUE_GEN_PROCESS_ALL,
    )
}

pub fn float() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[DataType::Float16, DataType::Float32, DataType::Float64],
        &[false, true],
        &VALUE_GEN_PROCESS_ALL,
    )
}

pub fn decimal() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Decimal128(15, 4),
            DataType::Decimal128(Decimal128Type::MAX_PRECISION, 0),
            DataType::Decimal128(Decimal128Type::MAX_PRECISION, Decimal128Type::MAX_SCALE),
            DataType::Decimal256(45, 12),
            DataType::Decimal256(Decimal256Type::MAX_PRECISION, 0),
            DataType::Decimal256(Decimal256Type::MAX_PRECISION, Decimal256Type::MAX_SCALE),
        ],
        &[false, true],
        &[
            ValueGenProcess::Low,
            ValueGenProcess::High,
            ValueGenProcess::Unit,
            ValueGenProcess::Null,
            ValueGenProcess::RandomUniform,
        ],
    )
}

#[allow(dead_code)]
pub fn timestamp() -> Vec<ColumnSpec> {
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
        &[
            ValueGenProcess::Low,
            ValueGenProcess::High,
            ValueGenProcess::Unit,
            ValueGenProcess::Null,
            ValueGenProcess::RandomUniform,
        ],
    )
}

#[allow(dead_code)]
pub fn date() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[DataType::Date32, DataType::Date64],
        &[true],
        &VALUE_GEN_PROCESS_ALL,
    )
}

#[allow(dead_code)]
pub fn time() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Microsecond),
        ],
        &[true],
        &VALUE_GEN_PROCESS_ALL,
    )
}

#[allow(dead_code)]
pub fn duration() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Second),
        ],
        &[true],
        &VALUE_GEN_PROCESS_ALL,
    )
}

#[allow(dead_code)]
pub fn interval() -> Vec<ColumnSpec> {
    domains_to_batch_spec(
        &[
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Interval(IntervalUnit::DayTime),
        ],
        &[true],
        &VALUE_GEN_PROCESS_ALL,
    )
}

fn domains_to_batch_spec(
    data_types_domain: &[DataType],
    is_nullable_domain: &[bool],
    value_gen_process_domain: &[ValueGenProcess],
) -> Vec<ColumnSpec> {
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
                        *gen_process
                    },
                    repeat: 1,
                });
            }
            columns.push(col);
        }
    }
    columns
}
