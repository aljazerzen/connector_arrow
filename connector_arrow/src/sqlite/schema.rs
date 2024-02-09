use arrow::datatypes::{Field, Schema};
use itertools::Itertools;

use crate::errors::{ConnectorError, TableDropError};
use crate::{api::TableSchema, errors::TableCreateError};

use super::types::{self, ty_from_arrow};

pub fn table_list(conn: &mut rusqlite::Connection) -> Result<Vec<TableSchema>, ConnectorError> {
    // query table names
    let table_names = {
        let query_tables = "SELECT name FROM sqlite_master WHERE type = 'table';";
        let mut statement = conn.prepare(query_tables)?;
        let mut tables_res = statement.query(())?;

        let mut table_names = Vec::new();
        while let Some(row) = tables_res.next()? {
            let table_name: String = row.get(0)?;
            table_names.push(table_name);
        }
        table_names
    };

    // for each table
    let mut defs = Vec::with_capacity(table_names.len());
    for table_name in table_names {
        let query_columns = format!("PRAGMA table_info(\"{}\");", table_name);
        let mut statement = conn.prepare(&query_columns)?;
        let mut columns_res = statement.query(())?;
        // contains columns: cid, name, type, notnull, dflt_value, pk

        let mut fields = Vec::new();
        while let Some(row) = columns_res.next()? {
            let name: String = row.get(1)?;
            let ty: String = row.get(2)?;
            let not_null: bool = row.get(3)?;

            let ty = types::decl_ty_to_arrow(&ty, &name, &table_name)?;
            fields.push(Field::new(name, ty, !not_null));
        }

        defs.push(TableSchema {
            name: table_name,
            schema: Schema::new(fields),
        })
    }

    Ok(defs)
}

pub(crate) fn table_create(
    conn: &mut rusqlite::Connection,
    name: &str,
    schema: std::sync::Arc<Schema>,
) -> Result<(), TableCreateError> {
    let column_defs = schema
        .fields()
        .iter()
        .map(|field| {
            let ty = ty_from_arrow(field.data_type()).expect("TODO: err message");

            let not_null = if field.is_nullable() { "" } else { "NOT NULL" };

            format!("{} {}{}", field.name(), ty, not_null)
        })
        .join(",");

    let ddl = format!("CREATE TABLE {name} ({column_defs});");

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
    let ddl = format!("DROP TABLE {name};");

    let res = conn.execute(&ddl, ());
    match res {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().starts_with("no such table") => {
            Err(TableDropError::TableNonexistent)
        }
        Err(e) => Err(TableDropError::Connector(ConnectorError::SQLite(e))),
    }
}
