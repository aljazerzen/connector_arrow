use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowType, *};
use postgres::types::Type as PgType;

use crate::{api::Connector, errors::ConnectorError};

use super::{PostgresConnection, ProtocolExtended};

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
        "text" => ArrowType::LargeUtf8,
        "varchar" => ArrowType::LargeUtf8,
        "bytea" => ArrowType::LargeBinary,
        name => PostgresConnection::<ProtocolExtended>::type_db_into_arrow(name)
            .unwrap_or_else(|| unimplemented!("{name}")),
    }
}

pub(crate) fn arrow_ty_to_pg(data_type: &ArrowType) -> String {
    match data_type {
        // there is no Null type in PostgreSQL, so we fallback to some other type that is nullable
        ArrowType::Null => "INT2".into(),

        ArrowType::Boolean => "BOOLEAN".into(),
        ArrowType::Int8 => "INT2".into(),
        ArrowType::Int16 => "INT2".into(),
        ArrowType::Int32 => "INT4".into(),
        ArrowType::Int64 => "INT8".into(),
        ArrowType::UInt8 => "INT2".into(),
        ArrowType::UInt16 => "INT4".into(),
        ArrowType::UInt32 => "INT8".into(),
        ArrowType::UInt64 => "NUMERIC(20, 0)".into(),
        ArrowType::Float16 => "FLOAT4".into(),
        ArrowType::Float32 => "FLOAT4".into(),
        ArrowType::Float64 => "FLOAT8".into(),

        ArrowType::Timestamp(_, _) => "INT8".into(),
        ArrowType::Date32 => "INT4".into(),
        ArrowType::Date64 => "INT8".into(),
        ArrowType::Time32(_) => "INT4".into(),
        ArrowType::Time64(_) => "INT8".into(),
        ArrowType::Duration(_) => "INT8".into(),
        // ArrowType::Interval(_) => ,
        ArrowType::Binary => "BYTEA".into(),
        ArrowType::FixedSizeBinary(_) => "BYTEA".into(),
        ArrowType::LargeBinary => "BYTEA".into(),
        ArrowType::Utf8 => "TEXT".into(),
        ArrowType::LargeUtf8 => "TEXT".into(),
        // ArrowType::List(_) => ,
        // ArrowType::FixedSizeList(_, _) => ,
        // ArrowType::LargeList(_) => ,
        // ArrowType::Struct(_) => ,
        // ArrowType::Union(_, _) => ,
        // ArrowType::Dictionary(_, _) => ,
        ArrowType::Decimal128(precision, scale) => format!("NUMERIC({precision}, {scale})"),
        ArrowType::Decimal256(precision, scale) => format!("NUMERIC({precision}, {scale})"),
        // ArrowType::Map(_, _) => ,
        // ArrowType::RunEndEncoded(_, _) => ,
        _ => unimplemented!("data type: {data_type}"),
    }
}
