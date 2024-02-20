use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowType, *};
use postgres::types::Type as PgType;

use crate::errors::ConnectorError;

pub fn pg_stmt_to_arrow(
    stmt: &postgres::Statement,
) -> Result<Arc<arrow::datatypes::Schema>, ConnectorError> {
    let fields: Vec<_> = stmt
        .columns()
        .iter()
        .map(|col| Field::new(col.name(), pg_ty_to_arrow(col.type_()), true))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

pub fn pg_ty_to_arrow(ty: &PgType) -> ArrowType {
    match ty.name() {
        "int2" => ArrowType::Int16,
        "int4" => ArrowType::Int32,
        "int8" => ArrowType::Int64,
        "float4" => ArrowType::Float32,
        "float8" => ArrowType::Float64,
        "numeric" => ArrowType::Float64,
        // "_bool" => ArrowType::Bool,
        // "_int2" => ArrowType::Int2Array,
        // "_int4" => ArrowType::Int4Array,
        // "_int8" => ArrowType::Int8Array,
        // "_float4" => ArrowType::Float4Array,
        // "_float8" => ArrowType::Float8Array,
        // "_numeric" => ArrowType::NumericArray,
        // "_varchar" => ArrowType::VarcharArray,
        // "_text" => ArrowType::TextArray,
        "bool" => ArrowType::Boolean,
        "text" | "citext" | "ltree" | "lquery" | "ltxtquery" | "name" => ArrowType::LargeUtf8,
        "bpchar" => ArrowType::LargeUtf8,
        "varchar" => ArrowType::LargeUtf8,
        "bytea" => ArrowType::LargeBinary,
        // "time" => ArrowType::Time,
        // "timestamp" => ArrowType::Timestamp,
        // "timestamptz" => ArrowType::TimestampTz,
        // "date" => ArrowType::Date,
        // "uuid" => ArrowType::UUID,
        // "json" => ArrowType::JSON,
        // "jsonb" => ArrowType::JSONB,
        // "hstore" => ArrowType::HSTORE,
        _ => unimplemented!("{}", ty.name()),
    }
}

pub(crate) fn arrow_ty_to_pg(data_type: &ArrowType) -> &'static str {
    match data_type {
        // there is no Null type in PostgreSQL, so we fallback to some other type that is nullable
        ArrowType::Null => "INT2",

        ArrowType::Boolean => "BOOLEAN",
        ArrowType::Int8 => "INT2",
        ArrowType::Int16 => "INT2",
        ArrowType::Int32 => "INT4",
        ArrowType::Int64 => "INT8",
        ArrowType::UInt8 => "INT2",
        ArrowType::UInt16 => "INT4",
        ArrowType::UInt32 => "INT8",
        // ArrowType::UInt64 => "DECIMAL(20, 0)",
        ArrowType::Float16 => "FLOAT4",
        ArrowType::Float32 => "FLOAT4",
        ArrowType::Float64 => "FLOAT8",
        ArrowType::Timestamp(_, None) => "TIMESTAMP",
        ArrowType::Timestamp(_, Some(_)) => "TIMESTAMPTZ",
        // ArrowType::Date32 => ,
        // ArrowType::Date64 => ,
        // ArrowType::Time32(_) => ,
        // ArrowType::Time64(_) => ,
        ArrowType::Duration(_) => "INTERNAL",
        // ArrowType::Interval(_) => ,
        ArrowType::Binary => "BYTEA",
        ArrowType::FixedSizeBinary(_) => "BYTEA",
        ArrowType::LargeBinary => "BYTEA",
        ArrowType::Utf8 => "TEXT",
        ArrowType::LargeUtf8 => "TEXT",
        // ArrowType::List(_) => ,
        // ArrowType::FixedSizeList(_, _) => ,
        // ArrowType::LargeList(_) => ,
        // ArrowType::Struct(_) => ,
        // ArrowType::Union(_, _) => ,
        // ArrowType::Dictionary(_, _) => ,
        // ArrowType::Decimal128(_, _) => ,
        // ArrowType::Decimal256(_, _) => ,
        // ArrowType::Map(_, _) => ,
        // ArrowType::RunEndEncoded(_, _) => ,
        _ => unimplemented!("data type: {data_type}"),
    }
}
