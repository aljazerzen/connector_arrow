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
        "char" => ArrowType::LargeUtf8,
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

pub(crate) fn arrow_ty_to_pg(data_type: &ArrowType) -> PgType {
    match data_type {
        // there is no Null type in PostgreSQL, so we fallback to some other type that is nullable
        ArrowType::Null => PgType::INT2,

        ArrowType::Boolean => PgType::BOOL,
        ArrowType::Int8 => PgType::CHAR,
        ArrowType::Int16 => PgType::INT2,
        ArrowType::Int32 => PgType::INT4,
        ArrowType::Int64 => PgType::INT8,
        // ArrowType::UInt8 => PgType::,
        // ArrowType::UInt16 => PgType::,
        ArrowType::UInt32 => PgType::OID,
        // ArrowType::UInt64 => PgType::,
        // ArrowType::Float16 => PgType::,
        ArrowType::Float32 => PgType::FLOAT4,
        ArrowType::Float64 => PgType::FLOAT8,
        ArrowType::Timestamp(_, None) => PgType::TIMESTAMP,
        ArrowType::Timestamp(_, Some(_)) => PgType::TIMESTAMPTZ,
        // ArrowType::Date32 => PgType::,
        // ArrowType::Date64 => PgType::,
        // ArrowType::Time32(_) => PgType::,
        // ArrowType::Time64(_) => PgType::,
        ArrowType::Duration(_) => PgType::INTERNAL,
        // ArrowType::Interval(_) => PgType::,
        ArrowType::Binary => PgType::BYTEA,
        ArrowType::FixedSizeBinary(_) => PgType::BYTEA,
        ArrowType::LargeBinary => PgType::BYTEA,
        ArrowType::Utf8 => PgType::TEXT,
        ArrowType::LargeUtf8 => PgType::TEXT,
        // ArrowType::List(_) => PgType::,
        // ArrowType::FixedSizeList(_, _) => PgType::,
        // ArrowType::LargeList(_) => PgType::,
        // ArrowType::Struct(_) => PgType::,
        // ArrowType::Union(_, _) => PgType::,
        // ArrowType::Dictionary(_, _) => PgType::,
        // ArrowType::Decimal128(_, _) => PgType::,
        // ArrowType::Decimal256(_, _) => PgType::,
        // ArrowType::Map(_, _) => PgType::,
        // ArrowType::RunEndEncoded(_, _) => PgType::,
        _ => unimplemented!("data type: {data_type}"),
    }
}
