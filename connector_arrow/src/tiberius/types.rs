use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::*;
use tiberius::{Column, ColumnType};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

use crate::{api::Connector, ConnectorError};

pub fn get_result_schema(columns: Option<&[Column]>) -> Result<SchemaRef, ConnectorError> {
    let Some(columns) = columns else {
        return Err(ConnectorError::NoResultSets);
    };

    let mut fields = Vec::new();
    for column in columns {
        let db_ty = get_name_of_column_type(&column.column_type());

        fields.push(create_field(column.name(), db_ty, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

pub fn create_field(name: &str, db_ty: &str, nullable: bool) -> Field {
    let mut metadata = HashMap::new();

    let data_type = super::TiberiusConnection::<Compat<TcpStream>>::type_db_into_arrow(db_ty);

    // if we cannot map to an arrow type, map into a binary
    let data_type = data_type.unwrap_or_else(|| {
        metadata.insert(crate::api::METADATA_DB_TYPE.to_string(), db_ty.to_string());

        DataType::Binary
    });

    Field::new(name, data_type, nullable)
}

fn get_name_of_column_type(col_ty: &ColumnType) -> &'static str {
    use ColumnType::*;

    match col_ty {
        Null => "null",
        Bit | Bitn => "bit",

        Int1 => "tinyint",
        Int2 => "smallint",
        Int4 => "int",
        Int8 => "bigint",

        Intn => "intn",

        // TODO: this should not be hardcoded to 20
        // it should be a dynamic value that might differ for each row?
        Decimaln | Numericn => "decimal(20)",

        Float4 => "float(24)",
        Float8 | Floatn => "float(53)",

        Money => "money",
        Money4 => "money4",

        Datetime4 => "datetime4",
        Datetime => "datetime",
        Datetimen => "datetimen",
        Daten => "daten",
        Timen => "timen",
        Datetime2 => "datetime2",
        DatetimeOffsetn => "datetimeoffsetn",

        Guid => "guid",

        BigVarBin => "bigvarbin",
        BigBinary => "bigbinary",

        BigVarChar => "bigvarchar",
        BigChar => "bigchar",
        NVarchar => "nvarchar",
        NChar => "nchar",
        Text => "text",
        NText => "ntext",

        Xml => "xml",
        Udt => "udt",
        Image => "image",

        SSVariant => "ssvariant",
    }
}
