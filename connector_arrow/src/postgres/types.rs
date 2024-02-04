use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowType, Field, Schema};
use postgres::types::Type as PgType;

use crate::errors::ConnectorError;

pub fn convert_schema(
    stmt: &postgres::Statement,
) -> Result<Arc<arrow::datatypes::Schema>, ConnectorError> {
    let fields: Vec<_> = stmt
        .columns()
        .iter()
        .map(|col| Field::new(col.name(), convert_type(col.type_()), true))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

pub fn convert_type(ty: &PgType) -> ArrowType {
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
