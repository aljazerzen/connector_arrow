use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use postgres::types::Type;

use crate::{api::Connector, errors::ConnectorError};

use super::{PostgresConnection, ProtocolExtended};

pub fn pg_stmt_to_arrow(
    stmt: &postgres::Statement,
) -> Result<Arc<arrow::datatypes::Schema>, ConnectorError> {
    let fields: Vec<_> = stmt
        .columns()
        .iter()
        .map(|col| pg_field_to_arrow(col.name().to_string(), col.type_(), true))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

pub fn pg_field_to_arrow(name: String, db_ty: &Type, nullable: bool) -> Field {
    let mut metadata = HashMap::new();

    let data_type = PostgresConnection::<ProtocolExtended>::type_db_into_arrow(db_ty.name());

    // if we cannot map to an arrow type, map into a binary
    let data_type = data_type.unwrap_or_else(|| {
        metadata.insert(
            crate::api::METADATA_DB_TYPE.to_string(),
            db_ty.name().to_string(),
        );

        DataType::Binary
    });

    Field::new(name, data_type, nullable).with_metadata(metadata)
}
