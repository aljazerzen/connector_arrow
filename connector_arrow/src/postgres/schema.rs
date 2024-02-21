use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use itertools::Itertools;
use postgres::error::SqlState;
use postgres::types::Type;

use crate::api::{SchemaEdit, SchemaGet};
use crate::{ConnectorError, TableCreateError, TableDropError};

use super::PostgresError;

impl<P> SchemaGet for super::PostgresConnection<P> {
    fn table_list(&mut self) -> Result<Vec<String>, ConnectorError> {
        let query = "
            SELECT relname
            FROM pg_class
            JOIN pg_namespace ON (relnamespace = pg_namespace.oid)
            WHERE nspname = current_schema AND relkind = 'r'
        ";
        let rows = self.client.query(query, &[]).map_err(PostgresError::from)?;

        let table_names = rows.into_iter().map(|r| r.get(0)).collect_vec();
        Ok(table_names)
    }

    fn table_get(
        &mut self,
        table_name: &str,
    ) -> Result<arrow::datatypes::SchemaRef, ConnectorError> {
        let query = "
            SELECT attname, atttypid, attnotnull
            FROM pg_attribute
            JOIN pg_class ON (attrelid = pg_class.oid)
            JOIN pg_namespace ON (relnamespace = pg_namespace.oid)
            WHERE nspname = current_schema AND relname = $1 AND attnum > 0 AND atttypid > 0
            ORDER BY attnum;
        ";
        let res = self.client.query(query, &[&table_name.to_string()]);
        let rows = res.map_err(PostgresError::Postgres)?;

        let fields: Vec<_> = rows
            .into_iter()
            .map(|row| -> Result<_, ConnectorError> {
                let name: String = row.get(0);
                let typid: u32 = row.get(1);
                let notnull: bool = row.get(2);

                let ty =
                    Type::from_oid(typid).ok_or_else(|| ConnectorError::IncompatibleSchema {
                        table_name: table_name.to_string(),
                        message: format!("column `{name}` has unsupported type (oid = {typid})"),
                        hint: Some("Supported types are INTEGER, REAL, TEXT and BLOB".to_string()),
                    })?;
                let ty = super::types::pg_ty_to_arrow(&ty);

                Ok(Field::new(name, ty, !notnull))
            })
            .try_collect()?;

        Ok(Arc::new(Schema::new(fields)))
    }
}

impl<P> SchemaEdit for super::PostgresConnection<P> {
    fn table_create(&mut self, name: &str, schema: SchemaRef) -> Result<(), TableCreateError> {
        let column_defs = schema
            .fields()
            .iter()
            .map(|field| {
                let ty = super::types::arrow_ty_to_pg(field.data_type());

                let is_nullable =
                    field.is_nullable() || matches!(field.data_type(), DataType::Null);
                let not_null = if is_nullable { "" } else { " NOT NULL" };

                format!("\"{}\" {}{}", field.name(), ty, not_null)
            })
            .join(",");

        let ddl = format!("CREATE TABLE \"{name}\" ({column_defs});");

        let res = self.client.execute(&ddl, &[]);
        match res {
            Ok(_) => Ok(()),
            Err(e) if matches!(e.code(), Some(&SqlState::DUPLICATE_TABLE)) => {
                Err(TableCreateError::TableExists)
            }
            Err(e) => Err(TableCreateError::Connector(ConnectorError::Postgres(
                PostgresError::Postgres(e),
            ))),
        }
    }

    fn table_drop(&mut self, name: &str) -> Result<(), TableDropError> {
        let res = self.client.execute(&format!("DROP TABLE \"{name}\""), &[]);

        match res {
            Ok(_) => Ok(()),
            Err(e) if matches!(e.code(), Some(&SqlState::UNDEFINED_TABLE)) => {
                Err(TableDropError::TableNonexistent)
            }
            Err(err) => Err(TableDropError::Connector(ConnectorError::Postgres(
                PostgresError::Postgres(err),
            ))),
        }
    }
}
