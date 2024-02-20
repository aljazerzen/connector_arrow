use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use half::f16;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Copy)]
enum ValueGenProcess {
    Null,
    Low,
    High,
    RandomUniform,
}

struct ValuesDesc {
    gen_process: ValueGenProcess,
    repeat: usize,
}

struct ColumnDesc {
    field_name: String,
    is_nullable: bool,
    data_type: DataType,
    values: Vec<ValuesDesc>,
}

fn count_values(values: &[ValuesDesc]) -> usize {
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

fn generate_array<R: Rng>(data_type: &DataType, values: &[ValuesDesc], rng: &mut R) -> ArrayRef {
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
        DataType::Timestamp(_, _) => todo!(),
        DataType::Date32 => todo!(),
        DataType::Date64 => todo!(),
        DataType::Time32(_) => todo!(),
        DataType::Time64(_) => todo!(),
        DataType::Duration(_) => todo!(),
        DataType::Interval(_) => todo!(),
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

fn generate_batch<R: Rng>(columns_desc: Vec<ColumnDesc>, rng: &mut R) -> RecordBatch {
    let mut arrays = Vec::new();
    let mut fields = Vec::new();
    for column in columns_desc {
        let array = generate_array(&column.data_type, &column.values, rng);
        arrays.push(array);

        let field = Field::new(column.field_name, column.data_type, column.is_nullable);
        fields.push(field);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).unwrap()
}

fn numeric() -> Vec<ColumnDesc> {
    let data_types_domain = [
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
        // DataType::Float16,
        DataType::Float32,
        DataType::Float64,
    ];
    let is_nullable_domain = [false, true];
    let value_gen_process_domain = [
        ValueGenProcess::Low,
        ValueGenProcess::High,
        ValueGenProcess::Null,
        ValueGenProcess::RandomUniform,
    ];

    let mut columns = Vec::new();
    for data_type in &data_types_domain {
        for is_nullable in is_nullable_domain {
            if matches!(data_type, &DataType::Null) && !is_nullable {
                continue;
            }

            let mut field_name = data_type.to_string();
            if is_nullable {
                field_name += "_null";
            }
            let mut col = ColumnDesc {
                field_name,
                data_type: data_type.clone(),
                is_nullable,
                values: Vec::new(),
            };

            for gen_process in value_gen_process_domain {
                col.values.push(ValuesDesc {
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

fn write_parquet_to_file(batch: RecordBatch, file_name: &str) {
    let path = Path::new("../connector_arrow/tests/data/file").with_file_name(file_name);

    let mut file = File::create(path).unwrap();

    let schema = batch.schema();
    let mut writer =
        parquet::arrow::arrow_writer::ArrowWriter::try_new(&mut file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn main() {
    let mut rng = rand_chacha::ChaCha8Rng::from_seed([0; 32]);

    write_parquet_to_file(generate_batch(numeric(), &mut rng), "numeric.parquet");
}
