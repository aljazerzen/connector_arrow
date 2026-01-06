use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use itertools::Itertools;
use postgres::error::SqlState;
use postgres::types::Type;

use crate::api::{Connector, SchemaEdit, SchemaGet};
use crate::postgres::PostgresConnection;
use crate::util::escape::escaped_ident;
use crate::{ConnectorError, TableCreateError, TableDropError};

use super::PostgresError;

impl SchemaGet for super::PostgresConnection {
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
                let not_null: bool = row.get(2);

                let ty = Type::from_oid(typid).ok_or(ConnectorError::NotSupported {
                    connector_name: "connector_arrow::postgres table_get",
                    feature: "custom types",
                })?;

                Ok(super::types::pg_field_to_arrow(name, &ty, !not_null))
            })
            .try_collect()?;

        Ok(Arc::new(Schema::new(fields)))
    }
}

impl SchemaEdit for super::PostgresConnection {
    fn table_create(&mut self, name: &str, schema: SchemaRef) -> Result<(), TableCreateError> {
        let column_defs = schema
            .fields()
            .iter()
            .map(|field| {
                let ty = PostgresConnection::type_arrow_into_db(field.data_type()).unwrap_or_else(
                    || {
                        unimplemented!("cannot store type {} in PostgreSQL", field.data_type());
                    },
                );

                let is_nullable =
                    field.is_nullable() || matches!(field.data_type(), DataType::Null);
                let not_null = if is_nullable { "" } else { " NOT NULL" };

                let name = escaped_ident(field.name());
                format!("{name} {ty}{not_null}",)
            })
            .join(",");

        let ddl = format!("CREATE TABLE {} ({column_defs});", escaped_ident(name));

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
        let ddl = format!("DROP TABLE {}", escaped_ident(name));
        let res = self.client.execute(&ddl, &[]);

        match res {
            Ok(_) => Ok(()),
            Err(e)
                if matches!(e.code(), Some(&SqlState::UNDEFINED_TABLE)) ||
                    // GlareDB will return such errors
                    e.as_db_error().is_some_and(|e| {
                        e.message().starts_with("Error during planning: Table ")
                            && e.message().ends_with(" does not exist")
                    }) =>
            {
                Err(TableDropError::TableNonexistent)
            }
            Err(e) if matches!(e.code(), Some(&SqlState::UNDEFINED_TABLE)) => {
                Err(TableDropError::TableNonexistent)
            }
            Err(err) => Err(TableDropError::Connector(ConnectorError::Postgres(
                PostgresError::Postgres(err),
            ))),
        }
    }
}
