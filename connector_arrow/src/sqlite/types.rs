use arrow::datatypes::DataType;

use crate::ConnectorError;

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
