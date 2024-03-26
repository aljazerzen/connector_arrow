use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use itertools::Itertools;
use mysql::prelude::Queryable;

use crate::{
    api::{Connector, SchemaEdit, SchemaGet},
    mysql::MySQLConnection,
    util::escape::escaped_ident_bt,
    ConnectorError, TableCreateError, TableDropError,
};

impl<C: Queryable> SchemaGet for super::MySQLConnection<C> {
    fn table_list(&mut self) -> Result<Vec<String>, crate::ConnectorError> {
        let mut results = self.conn.exec_iter("SHOW TABLES;", ())?;
        let result = results.iter().ok_or(crate::ConnectorError::NoResultSets)?;

        let table_names = result
            .into_iter()
            .map(|r_row| r_row.map(|row| row.get::<String, _>(0).unwrap()))
            .collect::<Result<Vec<String>, _>>()?;

        Ok(table_names)
    }

    fn table_get(
        &mut self,
        name: &str,
    ) -> Result<arrow::datatypes::SchemaRef, crate::ConnectorError> {
        let mut results = self
            .conn
            .exec_iter(format!("DESCRIBE {};", escaped_ident_bt(name)), ())?;
        let result = results.iter().ok_or(crate::ConnectorError::NoResultSets)?;

        let fields = result
            .into_iter()
            .map(|r_row| {
                r_row.map(|row| {
                    let name = row.get::<String, _>(0).unwrap();
                    let ty = row.get::<String, _>(1).unwrap();
                    let nullable = row.get::<String, _>(2).unwrap() == "YES";

                    super::types::create_field(name, &ty, nullable)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Arc::new(Schema::new(fields)))
    }
}

impl<C: Queryable> SchemaEdit for super::MySQLConnection<C> {
    fn table_create(
        &mut self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
    ) -> Result<(), TableCreateError> {
        let column_defs = schema
            .fields()
            .iter()
            .map(|field| {
                let ty = MySQLConnection::<mysql::Conn>::type_arrow_into_db(field.data_type())
                    .unwrap_or_else(|| {
                        unimplemented!("cannot store arrow type {} in MySQL", field.data_type());
                    });

                let is_nullable =
                    field.is_nullable() || matches!(field.data_type(), DataType::Null);
                let not_null = if is_nullable { "" } else { " NOT NULL" };

                let name = escaped_ident_bt(field.name());
                format!("{name} {ty}{not_null}",)
            })
            .join(",");

        let ddl = format!("CREATE TABLE {} ({column_defs});", escaped_ident_bt(name));

        let res = self.conn.query_drop(&ddl);
        match res {
            Ok(_) => Ok(()),
            Err(mysql::Error::MySqlError(e)) if e.code == 1050 => {
                Err(TableCreateError::TableExists)
            }
            Err(e) => Err(TableCreateError::Connector(ConnectorError::MySQL(e))),
        }
    }

    fn table_drop(&mut self, name: &str) -> Result<(), TableDropError> {
        let res = self
            .conn
            .query_drop(format!("DROP TABLE {}", escaped_ident_bt(name)));
        match res {
            Ok(_) => Ok(()),
            Err(mysql::Error::MySqlError(e)) if e.code == 1051 => {
                Err(TableDropError::TableNonexistent)
            }
            Err(e) => Err(TableDropError::Connector(ConnectorError::MySQL(e))),
        }
    }
}
