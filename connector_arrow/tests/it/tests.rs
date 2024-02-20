use std::path::Path;

use arrow::util::pretty::pretty_format_batches;
use connector_arrow::{
    api::{Connection, ResultReader, SchemaEdit, SchemaGet, Statement},
    TableCreateError, TableDropError,
};
use rand::SeedableRng;

use crate::util::{load_into_table, query_table};
use crate::{
    generator::{generate_batch, ColumnSpec},
    util::read_parquet,
};

#[track_caller]
pub fn query_01<C: Connection>(conn: &mut C) {
    let query = "SELECT 1 as a, NULL as b";
    let results = connector_arrow::query_one(conn, &query).unwrap();

    similar_asserts::assert_eq!(
        pretty_format_batches(&results).unwrap().to_string(),
        "+---+---+\n\
         | a | b |\n\
         +---+---+\n\
         | 1 |   |\n\
         +---+---+"
    );
}

pub fn roundtrip_of_parquet<C>(conn: &mut C, file_name: &str, table_name: &str)
where
    C: Connection + SchemaEdit,
{
    let file_path = Path::new("./tests/data/a").with_file_name(file_name);

    let (schema_file, batches_file) = read_parquet(&file_path).unwrap();
    let (schema_file, batches_file) =
        load_into_table(conn, schema_file, batches_file, table_name).unwrap();
    let (schema_query, batches_query) = query_table(conn, table_name).unwrap();
    similar_asserts::assert_eq!(schema_file, schema_query);
    similar_asserts::assert_eq!(batches_file, batches_query);
}

pub fn roundtrip_of_generated<C>(conn: &mut C, table_name: &str, column_specs: Vec<ColumnSpec>)
where
    C: Connection + SchemaEdit,
{
    let mut rng = rand_chacha::ChaCha8Rng::from_seed([0; 32]);
    let batch = generate_batch(column_specs, &mut rng);

    let (_, batches_file) = load_into_table(conn, batch.schema(), vec![batch], table_name).unwrap();

    let (_, batches_query) = query_table(conn, table_name).unwrap();

    similar_asserts::assert_eq!(batches_file, batches_query);
}

pub fn introspection<C>(conn: &mut C, file_name: &str, table_name: &str)
where
    C: Connection + SchemaEdit + SchemaGet,
{
    let file_path = Path::new("./tests/data/a").with_file_name(file_name);

    let (schema_file, batches_file) = read_parquet(&file_path).unwrap();
    let (schema_loaded, _) = load_into_table(conn, schema_file, batches_file, table_name).unwrap();

    let schema_introspection = conn.table_get(table_name).unwrap();
    similar_asserts::assert_eq!(schema_loaded, schema_introspection);
}

pub fn schema_edit<C>(conn: &mut C, file_name: &str, table_name: &str)
where
    C: Connection + SchemaEdit + SchemaGet,
{
    let file_path = Path::new("./tests/data/a").with_file_name(file_name);

    let (schema_file, batches_file) = read_parquet(&file_path).unwrap();
    let (schema, _) = load_into_table(conn, schema_file, batches_file, table_name).unwrap();

    let table_name2 = table_name.to_string() + "2";

    let _ignore = conn.table_drop(&table_name2);

    conn.table_create(&table_name2, schema.clone()).unwrap();
    assert!(matches!(
        conn.table_create(&table_name2, schema.clone()).unwrap_err(),
        TableCreateError::TableExists
    ));

    conn.table_drop(&table_name2).unwrap();
    assert!(matches!(
        conn.table_drop(&table_name2).unwrap_err(),
        TableDropError::TableNonexistent
    ));
}

pub fn streaming<C: Connection>(conn: &mut C) {
    let query = "
    WITH RECURSIVE t(n) AS (
        VALUES (1)
      UNION ALL
        SELECT n+1 FROM t WHERE n < 10000000
    )
    SELECT n FROM t;
    ";

    let mut stmt = conn.query(query).unwrap();

    // start reading
    let mut reader = stmt.start(()).unwrap();

    // get schema
    let schema = reader.get_schema().unwrap();
    similar_asserts::assert_eq!(
        schema.to_string(),
        r###"Field { name: "n", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }"###
    );

    // get a single batch
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1024);

    // drop the reader and don't call next
    // this should not load anymore batches
}
