use std::borrow::Cow;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use futures::{AsyncRead, AsyncWrite};
use itertools::Itertools;

use crate::api::{Connector, SchemaEdit, SchemaGet};
use crate::util::escape::escaped_ident;
use crate::{ConnectorError, TableCreateError, TableDropError};

impl<S: AsyncRead + AsyncWrite + Unpin + Send> SchemaGet for super::TiberiusConnection<S> {
    fn table_list(&mut self) -> Result<Vec<String>, ConnectorError> {
        let query = "
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE
                TABLE_CATALOG = DB_NAME() AND
                TABLE_SCHEMA = SCHEMA_NAME() AND
                TABLE_TYPE='BASE TABLE'
            ORDER BY TABLE_NAME
        ";
        let res = self.client.query(query, &[]);
        let res = self.rt.block_on(res)?;

        let res = res.into_first_result();
        let res = self.rt.block_on(res)?;

        let table_names = res
            .into_iter()
            .map(|r| r.get::<&str, _>(0).unwrap().to_string())
            .collect_vec();

        Ok(table_names)
    }

    fn table_get(
        &mut self,
        table_name: &str,
    ) -> Result<arrow::datatypes::SchemaRef, ConnectorError> {
        let query = "
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, NUMERIC_PRECISION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE
                TABLE_CATALOG = DB_NAME() AND
                TABLE_SCHEMA = SCHEMA_NAME() AND
                TABLE_NAME = @P1
            ORDER BY ORDINAL_POSITION;
        ";
        let params: [&dyn tiberius::ToSql; 1] = [&table_name.to_string()];
        let res = self.client.query(query, &params);
        let res = self.rt.block_on(res)?;

        let res = res.into_first_result();
        let res = self.rt.block_on(res)?;

        let fields: Vec<_> = res
            .into_iter()
            .map(|row| -> Result<_, ConnectorError> {
                let name: &str = row.get(0).unwrap();
                let data_type: &str = row.get(1).unwrap();
                let is_nullable: bool = row.get::<&str, _>(2).unwrap() != "NO";
                let numeric_precision: Option<u8> = row.get(3);

                let db_type_name = if let Some(numeric_precision) = numeric_precision {
                    Cow::from(format!("{data_type}({numeric_precision})"))
                } else {
                    Cow::from(data_type)
                };

                Ok(super::types::create_field(name, &db_type_name, is_nullable))
            })
            .try_collect()?;

        Ok(Arc::new(Schema::new(fields)))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> SchemaEdit for super::TiberiusConnection<S> {
    fn table_create(&mut self, name: &str, schema: SchemaRef) -> Result<(), TableCreateError> {
        let column_defs = schema
            .fields()
            .iter()
            .map(|field| {
                let ty_name = Self::type_arrow_into_db(field.data_type()).unwrap_or_else(|| {
                    unimplemented!("cannot store type {} in MS SQL Server", field.data_type());
                });

                let is_nullable =
                    field.is_nullable() || matches!(field.data_type(), DataType::Null);
                let not_null = if is_nullable { "" } else { " NOT NULL" };

                let name = escaped_ident(field.name());
                format!("{name} {ty_name}{not_null}",)
            })
            .join(",");

        let ddl = format!("CREATE TABLE {} ({column_defs});", escaped_ident(name));

        let res = self.client.execute(&ddl, &[]);
        let res = self.rt.block_on(res);

        match res {
            Ok(_) => Ok(()),
            Err(tiberius::error::Error::Server(e)) if e.code() == 2714 => {
                Err(TableCreateError::TableExists)
            }
            Err(e) => Err(TableCreateError::Connector(e.into())),
        }
    }

    fn table_drop(&mut self, name: &str) -> Result<(), TableDropError> {
        let ddl = format!("DROP TABLE {}", escaped_ident(name));
        let res = self.client.execute(&ddl, &[]);
        let res = self.rt.block_on(res);

        match res {
            Ok(_) => Ok(()),
            Err(tiberius::error::Error::Server(e)) if e.code() == 3701 => {
                Err(TableDropError::TableNonexistent)
            }
            Err(e) => Err(TableDropError::Connector(e.into())),
        }
    }
}
