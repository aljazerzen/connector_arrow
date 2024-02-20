use arrow::datatypes::{Field, Schema, SchemaRef};
use itertools::Itertools;
use std::sync::Arc;

use crate::api::{SchemaEdit, SchemaGet};
use crate::errors::{ConnectorError, TableCreateError, TableDropError};

use super::types::{self, ty_from_arrow};

impl SchemaGet for rusqlite::Connection {
    fn table_list(&mut self) -> Result<Vec<String>, ConnectorError> {
        let query_tables = "SELECT name FROM sqlite_master WHERE type = 'table';";
        let mut statement = self.prepare(query_tables)?;
        let mut tables_res = statement.query(())?;

        let mut table_names = Vec::new();
        while let Some(row) = tables_res.next()? {
            let table_name: String = row.get(0)?;
            table_names.push(table_name);
        }
        Ok(table_names)
    }

    fn table_get(
        &mut self,
        table_name: &str,
    ) -> Result<arrow::datatypes::SchemaRef, ConnectorError> {
        let query_columns = format!("PRAGMA table_info(\"{}\");", table_name);
        let mut statement = self.prepare(&query_columns)?;
        let mut columns_res = statement.query(())?;
        // contains columns: cid, name, type, notnull, dflt_value, pk

        let mut fields = Vec::new();
        while let Some(row) = columns_res.next()? {
            let name: String = row.get(1)?;
            let ty: String = row.get(2)?;
            let not_null: bool = row.get(3)?;

            let ty = types::decl_ty_to_arrow(&ty, &name, table_name)?;
            fields.push(Field::new(name, ty, !not_null));
        }

        Ok(Arc::new(Schema::new(fields)))
    }
}

impl SchemaEdit for rusqlite::Connection {
    fn table_create(&mut self, name: &str, schema: SchemaRef) -> Result<(), TableCreateError> {
        table_create(self, name, schema)
    }

    fn table_drop(&mut self, name: &str) -> Result<(), TableDropError> {
        table_drop(self, name)
    }
}

pub(crate) fn table_create(
    conn: &mut rusqlite::Connection,
    name: &str,
    schema: SchemaRef,
) -> Result<(), TableCreateError> {
    let column_defs = schema
        .fields()
        .iter()
        .map(|field| {
            let ty = ty_from_arrow(field.data_type()).expect("TODO: err message");

            let not_null = if field.is_nullable() { "" } else { " NOT NULL" };

            format!("\"{}\" {}{}", field.name(), ty, not_null)
        })
        .join(",");

    let ddl = format!("CREATE TABLE \"{name}\" ({column_defs});");

    let res = conn.execute(&ddl, ());
    match res {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().ends_with("already exists") => Err(TableCreateError::TableExists),
        Err(e) => Err(TableCreateError::Connector(ConnectorError::SQLite(e))),
    }
}

pub(crate) fn table_drop(
    conn: &mut rusqlite::Connection,
    name: &str,
) -> Result<(), TableDropError> {
    let ddl = format!("DROP TABLE \"{name}\";");

    let res = conn.execute(&ddl, ());
    match res {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().starts_with("no such table") => {
            Err(TableDropError::TableNonexistent)
        }
        Err(e) => Err(TableDropError::Connector(ConnectorError::SQLite(e))),
    }
}
