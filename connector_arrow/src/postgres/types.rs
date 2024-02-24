use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowType, *};
use postgres::types::Type as PgType;

use crate::{api::Connector, errors::ConnectorError};

use super::{PostgresConnection, ProtocolExtended};

pub fn pg_stmt_to_arrow(
    stmt: &postgres::Statement,
) -> Result<Arc<arrow::datatypes::Schema>, ConnectorError> {
    let fields: Vec<_> = stmt
        .columns()
        .iter()
        .map(|col| Field::new(col.name(), pg_ty_to_arrow(col.type_()), true))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

pub fn pg_ty_to_arrow(ty: &PgType) -> ArrowType {
    PostgresConnection::<ProtocolExtended>::type_db_into_arrow(ty.name())
        .unwrap_or_else(|| unimplemented!("{}", ty.name()))
}
