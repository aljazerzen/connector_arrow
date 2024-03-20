use std::sync::Arc;

use arrow::datatypes::*;
use mysql::{consts::ColumnType, prelude::Protocol};

use crate::ConnectorError;

pub fn get_result_schema<'a, P: Protocol>(
    result: &mysql::ResultSet<'a, 'a, 'a, 'a, P>,
) -> Result<SchemaRef, ConnectorError> {
    let mut fields = Vec::new();
    for column in result.columns().as_ref() {
        let ty = match column.column_type() {
            ColumnType::MYSQL_TYPE_NULL => DataType::Null,

            ColumnType::MYSQL_TYPE_TINY => DataType::Int8,
            ColumnType::MYSQL_TYPE_SHORT => DataType::Int16,
            ColumnType::MYSQL_TYPE_LONG => DataType::Int32,
            ColumnType::MYSQL_TYPE_LONGLONG => DataType::Int64,

            ColumnType::MYSQL_TYPE_FLOAT => DataType::Float32,
            ColumnType::MYSQL_TYPE_DOUBLE => DataType::Float64,

            ColumnType::MYSQL_TYPE_INT24 => todo!(),

            ColumnType::MYSQL_TYPE_TIMESTAMP => todo!(),
            ColumnType::MYSQL_TYPE_DATE => todo!(),
            ColumnType::MYSQL_TYPE_TIME => todo!(),
            ColumnType::MYSQL_TYPE_DATETIME => todo!(),
            ColumnType::MYSQL_TYPE_YEAR => todo!(),
            ColumnType::MYSQL_TYPE_NEWDATE => todo!(),

            ColumnType::MYSQL_TYPE_TIMESTAMP2 => todo!(),
            ColumnType::MYSQL_TYPE_DATETIME2 => todo!(),
            ColumnType::MYSQL_TYPE_TIME2 => todo!(),
            ColumnType::MYSQL_TYPE_TYPED_ARRAY => todo!(),

            ColumnType::MYSQL_TYPE_NEWDECIMAL => todo!(),
            ColumnType::MYSQL_TYPE_DECIMAL => todo!(),

            ColumnType::MYSQL_TYPE_VARCHAR => DataType::Utf8,
            ColumnType::MYSQL_TYPE_VAR_STRING => DataType::Utf8,
            ColumnType::MYSQL_TYPE_STRING => DataType::Utf8,
            ColumnType::MYSQL_TYPE_JSON => DataType::Utf8,

            ColumnType::MYSQL_TYPE_ENUM => todo!(),
            ColumnType::MYSQL_TYPE_SET => todo!(),

            ColumnType::MYSQL_TYPE_BIT => todo!(),
            ColumnType::MYSQL_TYPE_TINY_BLOB => DataType::Binary,
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB => DataType::Binary,
            ColumnType::MYSQL_TYPE_LONG_BLOB => DataType::Binary,
            ColumnType::MYSQL_TYPE_BLOB => DataType::Binary,

            ColumnType::MYSQL_TYPE_GEOMETRY => todo!(),
            ColumnType::MYSQL_TYPE_UNKNOWN => todo!(),
        };

        fields.push(Field::new(column.name_str(), ty, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}
