use std::sync::Arc;

use arrow::datatypes::*;
use mysql::consts::{ColumnFlags, ColumnType};
use mysql::prelude::Protocol;

use crate::api::Connector;
use crate::ConnectorError;

pub fn get_result_schema<'a, P: Protocol>(
    result: &mysql::ResultSet<'a, 'a, 'a, 'a, P>,
) -> Result<SchemaRef, ConnectorError> {
    let mut fields = Vec::new();
    for column in result.columns().as_ref() {
        let is_unsigned = !(column.flags() & ColumnFlags::UNSIGNED_FLAG).is_empty();
        let is_not_null = !(column.flags() & ColumnFlags::NOT_NULL_FLAG).is_empty();

        let db_ty = get_name_of_column_type(&column.column_type(), is_unsigned);
        fields.push(create_field(
            column.name_str().to_string(),
            db_ty,
            !is_not_null,
        ));
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn create_field(name: String, db_ty: &str, nullable: bool) -> Field {
    let data_type = super::MySQLConnection::<mysql::Conn>::type_db_into_arrow(db_ty);
    let data_type = data_type.unwrap_or_else(|| todo!());

    Field::new(name, data_type, nullable)
}

fn get_name_of_column_type(col_ty: &ColumnType, unsigned: bool) -> &'static str {
    use ColumnType::*;

    match (col_ty, unsigned) {
        (MYSQL_TYPE_NULL, _) => "null",

        (MYSQL_TYPE_TINY, false) => "tinyint",
        (MYSQL_TYPE_TINY, true) => "tinyint unsigned",

        (MYSQL_TYPE_SHORT, false) => "smallint",
        (MYSQL_TYPE_SHORT, true) => "smallint unsigned",

        (MYSQL_TYPE_INT24, false) => "mediumint",
        (MYSQL_TYPE_INT24, true) => "mediumint unsigned",

        (MYSQL_TYPE_LONG, false) => "int",
        (MYSQL_TYPE_LONG, true) => "int unsigned",

        (MYSQL_TYPE_LONGLONG, false) => "bigint",
        (MYSQL_TYPE_LONGLONG, true) => "bigint unsigned",

        (MYSQL_TYPE_FLOAT, _) => "float",
        (MYSQL_TYPE_DOUBLE, _) => "double",

        (MYSQL_TYPE_TIMESTAMP, _) => "timestamp",
        (MYSQL_TYPE_DATE, _) => "date",
        (MYSQL_TYPE_TIME, _) => "time",
        (MYSQL_TYPE_DATETIME, _) => "datetime",
        (MYSQL_TYPE_YEAR, _) => "year",
        (MYSQL_TYPE_NEWDATE, _) => "newdate",

        (MYSQL_TYPE_TIMESTAMP2, _) => "timestamp2",
        (MYSQL_TYPE_DATETIME2, _) => "datetime2",
        (MYSQL_TYPE_TIME2, _) => "time2",
        (MYSQL_TYPE_TYPED_ARRAY, _) => "typed_array",

        (MYSQL_TYPE_NEWDECIMAL, _) => "newdecimal",
        (MYSQL_TYPE_DECIMAL, _) => "decimal",

        (MYSQL_TYPE_VARCHAR, _) => "varchar",
        (MYSQL_TYPE_VAR_STRING, _) => "var_string",
        (MYSQL_TYPE_STRING, _) => "string",
        (MYSQL_TYPE_JSON, _) => "json",

        (MYSQL_TYPE_ENUM, _) => "enum",
        (MYSQL_TYPE_SET, _) => "set",

        (MYSQL_TYPE_BIT, _) => "bit",
        (MYSQL_TYPE_TINY_BLOB, _) => "tiny_blob",
        (MYSQL_TYPE_MEDIUM_BLOB, _) => "medium_blob",
        (MYSQL_TYPE_LONG_BLOB, _) => "long_blob",
        (MYSQL_TYPE_BLOB, _) => "blob",

        (MYSQL_TYPE_GEOMETRY, _) => "geometry",
        (MYSQL_TYPE_UNKNOWN, _) => "unknown",
    }
}
