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
        message: format!("column `{col}` was declared with type `{ty}`, which results in `NUMERIC` affinity, which is not supported."),
        hint: Some("Supported types are INTEGER, REAL, TEXT and BLOB".to_string())
    })
}

pub fn ty_from_arrow(ty: &DataType) -> &'static str {
    match ty {
        DataType::Null => "NULL",
        DataType::Boolean => "INTEGER",
        DataType::Int8 => "INTEGER",
        DataType::Int16 => "INTEGER",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "INTEGER",
        DataType::UInt8 => "INTEGER",
        DataType::UInt16 => "INTEGER",
        DataType::UInt32 => "INTEGER",
        DataType::UInt64 => "TEXT",
        DataType::Float16 => "REAL",
        DataType::Float32 => "REAL",
        DataType::Float64 => "REAL",
        DataType::Timestamp(_, _) => unimplemented!(),
        DataType::Date32 => unimplemented!(),
        DataType::Date64 => unimplemented!(),
        DataType::Time32(_) => unimplemented!(),
        DataType::Time64(_) => unimplemented!(),
        DataType::Duration(_) => unimplemented!(),
        DataType::Interval(_) => unimplemented!(),
        DataType::Binary => "BLOB",
        DataType::FixedSizeBinary(_) => "BLOB",
        DataType::LargeBinary => "BLOB",
        DataType::Utf8 => "TEXT",
        DataType::LargeUtf8 => "TEXT",
        DataType::List(_) => unimplemented!(),
        DataType::FixedSizeList(_, _) => unimplemented!(),
        DataType::LargeList(_) => unimplemented!(),
        DataType::Struct(_) => unimplemented!(),
        DataType::Union(_, _) => unimplemented!(),
        DataType::Dictionary(_, _) => unimplemented!(),
        DataType::Decimal128(_, _) => unimplemented!(),
        DataType::Decimal256(_, _) => unimplemented!(),
        DataType::Map(_, _) => unimplemented!(),
        DataType::RunEndEncoded(_, _) => unimplemented!(),
    }
}
