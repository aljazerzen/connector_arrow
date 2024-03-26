use std::sync::Arc;

use arrow::datatypes::*;
use mysql::consts::{ColumnFlags, ColumnType};
use mysql::prelude::Protocol;

use crate::api::Connector;
use crate::ConnectorError;

pub fn get_result_schema<'a, P: Protocol>(
    result: &mysql::ResultSet<'a, 'a, 'a, 'a, P>,
) -> Result<SchemaRef, ConnectorError> {
    dbg!("get_result_schema");

    let mut fields = Vec::new();
    for column in result.columns().as_ref() {
        let is_unsigned = !(column.flags() & ColumnFlags::UNSIGNED_FLAG).is_empty();
        let is_not_null = !(column.flags() & ColumnFlags::NOT_NULL_FLAG).is_empty();
        let is_blob = !(column.flags() & ColumnFlags::BLOB_FLAG).is_empty();
        let is_binary = !(column.flags() & ColumnFlags::BINARY_FLAG).is_empty();

        dbg!(column.name_str());
        dbg!(is_blob);
        dbg!(is_binary);

        let db_ty = get_name_of_column_type(&column.column_type(), is_unsigned, is_binary);
        dbg!(db_ty);
        fields.push(create_field(
            column.name_str().to_string(),
            db_ty,
            !is_not_null,
        ));
    }

    Ok(Arc::new(Schema::new(fields)))
}

pub fn create_field(name: String, db_ty: &str, nullable: bool) -> Field {
    let data_type = super::MySQLConnection::<mysql::Conn>::type_db_into_arrow(db_ty);
    let data_type = data_type.unwrap_or_else(|| todo!("db type: {db_ty}"));

    Field::new(name, data_type, nullable)
}

fn get_name_of_column_type(col_ty: &ColumnType, unsigned: bool, binary: bool) -> &'static str {
    use ColumnType::*;

    match (col_ty, unsigned, binary) {
        (MYSQL_TYPE_NULL, _, _) => "null",

        (MYSQL_TYPE_TINY, false, _) => "tinyint",
        (MYSQL_TYPE_TINY, true, _) => "tinyint unsigned",

        (MYSQL_TYPE_SHORT, false, _) => "smallint",
        (MYSQL_TYPE_SHORT, true, _) => "smallint unsigned",

        (MYSQL_TYPE_INT24, false, _) => "mediumint",
        (MYSQL_TYPE_INT24, true, _) => "mediumint unsigned",

        (MYSQL_TYPE_LONG, false, _) => "int",
        (MYSQL_TYPE_LONG, true, _) => "int unsigned",

        (MYSQL_TYPE_LONGLONG, false, _) => "bigint",
        (MYSQL_TYPE_LONGLONG, true, _) => "bigint unsigned",

        (MYSQL_TYPE_FLOAT, _, _) => "float",
        (MYSQL_TYPE_DOUBLE, _, _) => "double",

        (MYSQL_TYPE_TIMESTAMP, _, _) => "timestamp",
        (MYSQL_TYPE_DATE, _, _) => "date",
        (MYSQL_TYPE_TIME, _, _) => "time",
        (MYSQL_TYPE_DATETIME, _, _) => "datetime",
        (MYSQL_TYPE_YEAR, _, _) => "year",
        (MYSQL_TYPE_NEWDATE, _, _) => "newdate",

        (MYSQL_TYPE_TIMESTAMP2, _, _) => "timestamp2",
        (MYSQL_TYPE_DATETIME2, _, _) => "datetime2",
        (MYSQL_TYPE_TIME2, _, _) => "time2",
        (MYSQL_TYPE_TYPED_ARRAY, _, _) => "typed_array",

        (MYSQL_TYPE_NEWDECIMAL, _, _) => "newdecimal",
        (MYSQL_TYPE_DECIMAL, _, _) => "decimal",

        (MYSQL_TYPE_VARCHAR, _, _) => "varchar",
        (MYSQL_TYPE_JSON, _, _) => "json",

        (MYSQL_TYPE_ENUM, _, _) => "enum",
        (MYSQL_TYPE_SET, _, _) => "set",

        (MYSQL_TYPE_BIT, _, _) => "bit",

        (MYSQL_TYPE_TINY_BLOB, _, true) => "tinyblob",
        (MYSQL_TYPE_MEDIUM_BLOB, _, true) => "mediumblob",
        (MYSQL_TYPE_LONG_BLOB, _, true) => "longblob",
        (MYSQL_TYPE_BLOB, _, true) => "blob",
        (MYSQL_TYPE_VAR_STRING, _, true) => "varbinary",
        (MYSQL_TYPE_STRING, _, true) => "binary",

        (MYSQL_TYPE_TINY_BLOB, _, false) => "tinytext",
        (MYSQL_TYPE_MEDIUM_BLOB, _, false) => "mediumtext",
        (MYSQL_TYPE_LONG_BLOB, _, false) => "longtext",
        (MYSQL_TYPE_BLOB, _, false) => "text",
        (MYSQL_TYPE_VAR_STRING, _, false) => "varchar",
        (MYSQL_TYPE_STRING, _, false) => "char",

        (MYSQL_TYPE_GEOMETRY, _, _) => "geometry",
        (MYSQL_TYPE_UNKNOWN, _, _) => "unknown",
    }
}
