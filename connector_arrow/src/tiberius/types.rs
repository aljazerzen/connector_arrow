use std::sync::Arc;

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

        fields.push(create_field(column.name().to_string(), db_ty, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn create_field(name: String, db_ty: &str, nullable: bool) -> Field {
    let data_type = super::TiberiusConnection::<Compat<TcpStream>>::type_db_into_arrow(db_ty);
    let data_type = data_type.unwrap_or_else(|| todo!("database type: {}", db_ty));

    Field::new(name, data_type, nullable)
}

fn get_name_of_column_type(col_ty: &ColumnType) -> &'static str {
    use ColumnType::*;

    match col_ty {
        Null => "null",
        Bit => "bit",

        Int1 => "tinyint",
        Int2 => "smallint",
        Int4 => "int",
        Int8 => "bigint",

        Intn => "intn",
        Decimaln => "decimaln",
        Numericn => "numericn",

        Float4 => "float4",
        Float8 => "float8",
        Floatn => "floatn",

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

        Bitn => "bitn",
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
