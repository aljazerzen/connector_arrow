use arrow::datatypes::{Field, Schema, SchemaRef};
use itertools::Itertools;
use std::sync::Arc;

use crate::api::{Connector, SchemaEdit, SchemaGet};
use crate::errors::{ConnectorError, TableCreateError, TableDropError};
use crate::util::escape::escaped_ident;

use super::types;
use super::SQLiteConnection;

impl SchemaGet for SQLiteConnection {
    fn table_list(&mut self) -> Result<Vec<String>, ConnectorError> {
        let query_tables = "SELECT name FROM sqlite_master WHERE type = 'table';";
        let mut statement = self.inner.prepare(query_tables)?;
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
        let query_columns = format!("PRAGMA table_info({});", escaped_ident(table_name));
        let mut statement = self.inner.prepare(&query_columns)?;
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

impl SchemaEdit for SQLiteConnection {
    fn table_create(&mut self, name: &str, schema: SchemaRef) -> Result<(), TableCreateError> {
        table_create(self, name, schema)
    }

    fn table_drop(&mut self, name: &str) -> Result<(), TableDropError> {
        table_drop(self, name)
    }
}

pub(crate) fn table_create(
    conn: &mut SQLiteConnection,
    name: &str,
    schema: SchemaRef,
) -> Result<(), TableCreateError> {
    let column_defs = schema
        .fields()
        .iter()
        .map(|field| {
            let ty = SQLiteConnection::type_arrow_into_db(field.data_type()).unwrap_or_default();

            let not_null = if field.is_nullable() { "" } else { " NOT NULL" };

            let name = escaped_ident(field.name());
            format!("{name} {ty}{not_null}")
        })
        .join(",");

    let ddl = format!("CREATE TABLE {} ({column_defs});", escaped_ident(name));

    let res = conn.inner.execute(&ddl, ());
    match res {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().ends_with("already exists") => Err(TableCreateError::TableExists),
        Err(e) => Err(TableCreateError::Connector(ConnectorError::SQLite(e))),
    }
}

pub(crate) fn table_drop(conn: &mut SQLiteConnection, name: &str) -> Result<(), TableDropError> {
    let ddl = format!("DROP TABLE {};", escaped_ident(name));

    let res = conn.inner.execute(&ddl, ());
    match res {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().starts_with("no such table") => {
            Err(TableDropError::TableNonexistent)
        }
        Err(e) => Err(TableDropError::Connector(ConnectorError::SQLite(e))),
    }
}
