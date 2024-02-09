use arrow::datatypes::DataType;
use rusqlite::types::Type;

use crate::ConnectorError;

pub fn ty_to_arrow(ty: Type) -> Option<DataType> {
    match ty {
        Type::Integer => Some(DataType::Int64),
        Type::Real => Some(DataType::Float64),
        Type::Text => Some(DataType::LargeUtf8),
        Type::Blob => Some(DataType::LargeBinary),
        Type::Null => None,
    }
}

pub fn decl_ty_to_arrow(decl_ty: &str, col: &str, table: &str) -> Result<DataType, ConnectorError> {
    // SQLite does not have a "required" column type, only "suggest" column type,
    // known a column type affinity. This function takes this affinity and tries to
    // match it to [DataType], but:
    // a) one could make a column with affinity TEXT, but then store INTEGER in it,
    // b) one could declare a column with arbitrary data type that does not map into any affinity,
    // c) NUMERIC affinity can either be INT or REAL

    // See: https://sqlite.org/datatype3.html#determination_of_column_affinity
    let ty = decl_ty.to_ascii_uppercase();
    if ty.contains("INT") {
        return Ok(DataType::Int64);
    }

    if ty.contains("CHAR") || ty.contains("CLOB") || ty.contains("TEXT") {
        return Ok(DataType::LargeUtf8);
    }

    if ty.contains("BLOB") {
        return Ok(DataType::LargeBinary);
    }

    if ty.contains("REAL") || ty.contains("FLOA") || ty.contains("DOUB") {
        return Ok(DataType::Float64);
    }

    Err(ConnectorError::IncompatibleSchema {
        table_name: table.to_string(),
        message: format!("column `{col}` was declared as `{ty}`, which results in `NUMERIC` affinity, which is not supported."),
        hint: Some("Supported types are INTEGER, REAL, TEXT and BLOB".to_string())
    })
}

pub fn ty_from_arrow(ty: &DataType) -> Option<Type> {
    match ty {
        DataType::Null => Some(Type::Null),
        DataType::Boolean => Some(Type::Integer),
        DataType::Int8 => Some(Type::Integer),
        DataType::Int16 => Some(Type::Integer),
        DataType::Int32 => Some(Type::Integer),
        DataType::Int64 => Some(Type::Integer),
        DataType::UInt8 => Some(Type::Integer),
        DataType::UInt16 => Some(Type::Integer),
        DataType::UInt32 => Some(Type::Integer),
        DataType::UInt64 => None,
        DataType::Float16 => Some(Type::Real),
        DataType::Float32 => Some(Type::Real),
        DataType::Float64 => Some(Type::Real),
        DataType::Timestamp(_, _) => None,
        DataType::Date32 => None,
        DataType::Date64 => None,
        DataType::Time32(_) => None,
        DataType::Time64(_) => None,
        DataType::Duration(_) => None,
        DataType::Interval(_) => None,
        DataType::Binary => Some(Type::Blob),
        DataType::FixedSizeBinary(_) => Some(Type::Blob),
        DataType::LargeBinary => Some(Type::Blob),
        DataType::Utf8 => Some(Type::Text),
        DataType::LargeUtf8 => Some(Type::Text),
        DataType::List(_) => None,
        DataType::FixedSizeList(_, _) => None,
        DataType::LargeList(_) => None,
        DataType::Struct(_) => None,
        DataType::Union(_, _) => None,
        DataType::Dictionary(_, _) => None,
        DataType::Decimal128(_, _) => None,
        DataType::Decimal256(_, _) => None,
        DataType::Map(_, _) => None,
        DataType::RunEndEncoded(_, _) => None,
    }
}
