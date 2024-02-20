use arrow::datatypes::{DataType, SchemaRef};
use itertools::Itertools;

use crate::api::{SchemaEdit, SchemaGet};
use crate::{ConnectorError, TableCreateError, TableDropError};

use super::DuckDBConnection;

impl SchemaGet for DuckDBConnection {
    fn table_list(&mut self) -> Result<Vec<String>, ConnectorError> {
        let query_tables = "SHOW TABLES;";
        let mut statement = self.inner.prepare(query_tables)?;
        let mut tables_res = statement.query([])?;

        let mut table_names = Vec::new();
        while let Some(row) = tables_res.next()? {
            let table_name: String = row.get(0)?;
            table_names.push(table_name);
        }
        Ok(table_names)
    }

    fn table_get(&mut self, name: &str) -> Result<arrow::datatypes::SchemaRef, ConnectorError> {
        let query_schema = format!("SELECT * FROM \"{name}\" WHERE FALSE;");
        let mut statement = self.inner.prepare(&query_schema)?;
        let results = statement.query_arrow([])?;

        Ok(results.get_schema())
    }
}

impl SchemaEdit for DuckDBConnection {
    fn table_create(&mut self, name: &str, schema: SchemaRef) -> Result<(), TableCreateError> {
        let column_defs = schema
            .fields()
            .iter()
            .map(|field| {
                let ty = ty_from_arrow(field.data_type());

                let is_nullable =
                    field.is_nullable() || matches!(field.data_type(), DataType::Null);
                let not_null = if is_nullable { "" } else { " NOT NULL" };

                format!("\"{}\" {}{}", field.name(), ty, not_null)
            })
            .join(",");

        let ddl = format!("CREATE TABLE \"{name}\" ({column_defs});");

        let res = self.inner.execute(&ddl, []);
        match res {
            Ok(_) => Ok(()),
            Err(e)
                if e.to_string().starts_with("Catalog Error: Table with name")
                    && e.to_string().contains("already exists!") =>
            {
                Err(TableCreateError::TableExists)
            }
            Err(e) => Err(TableCreateError::Connector(ConnectorError::DuckDB(e))),
        }
    }

    fn table_drop(&mut self, name: &str) -> Result<(), TableDropError> {
        // TODO: properly escape
        let ddl = format!("DROP TABLE \"{name}\";");

        let res = self.inner.execute(&ddl, []);

        match res {
            Ok(_) => Ok(()),
            Err(e)
                if e.to_string().starts_with("Catalog Error: Table with name ")
                    && e.to_string().contains("does not exist!") =>
            {
                Err(TableDropError::TableNonexistent)
            }
            Err(e) => Err(TableDropError::Connector(e.into())),
        }
    }
}

fn ty_from_arrow(data_type: &DataType) -> &'static str {
    match data_type {
        // there is no Null type in DuckDB, so we fallback to some other type that is nullable
        DataType::Null => "int64",

        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float16 => "REAL",
        DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE",
        DataType::Timestamp(_, _) => unimplemented!(),
        DataType::Date32 => unimplemented!(),
        DataType::Date64 => unimplemented!(),
        DataType::Time32(_) => unimplemented!(),
        DataType::Time64(_) => unimplemented!(),
        DataType::Duration(_) => unimplemented!(),
        DataType::Interval(_) => unimplemented!(),
        DataType::Binary => "blob",
        DataType::FixedSizeBinary(_) => "blob",
        DataType::LargeBinary => "blob",
        DataType::Utf8 => "text",
        DataType::LargeUtf8 => "text",
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
