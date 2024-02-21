#![allow(dead_code)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, RecordBatch};
use arrow::datatypes::{Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use connector_arrow::api::{Append, Connection, ResultReader, SchemaEdit, SchemaGet, Statement};
use connector_arrow::{util::coerce, TableCreateError, TableDropError};
use rand::SeedableRng;

use crate::util::{load_into_table, query_table};
use crate::{generator::generate_batch, spec::ArrowGenSpec};

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

pub fn query_02<C: Connection>(conn: &mut C) {
    let query = "SELECT
        CAST(45927858023429386042648415184323464939503124872489107431467725871003289085860801 as NUMERIC(100, 20)) as a,
        CAST(3.14 as NUMERIC(2, 0)) as b,
        CAST(0 as NUMERIC(3, 2)) as c
    ";
    let results = connector_arrow::query_one(conn, &query).unwrap();

    similar_asserts::assert_eq!(
        "+-------------------------------------------------------------------------------------------------------+---+------+\n\
         | a                                                                                                     | b | c    |\n\
         +-------------------------------------------------------------------------------------------------------+---+------+\n\
         | 45927858023429386042648415184323464939503124872489107431467725871003289085860801.00000000000000000000 | 3 | 0.00 |\n\
         +-------------------------------------------------------------------------------------------------------+---+------+",
         pretty_format_batches(&results).unwrap().to_string(),
    );
}

pub fn roundtrip<C>(conn: &mut C, table_name: &str, spec: ArrowGenSpec)
where
    C: Connection + SchemaEdit,
{
    let mut rng = rand_chacha::ChaCha8Rng::from_seed([0; 32]);
    let (schema, batches) = generate_batch(spec, &mut rng);

    load_into_table(conn, schema.clone(), &batches, table_name).unwrap();

    let (schema_coerced, batches_coerced) =
        coerce::coerce_batches(schema, &batches, C::coerce_type, Some(true)).unwrap();

    let (schema_query, batches_query) = query_table(conn, table_name).unwrap();

    similar_asserts::assert_eq!(schema_coerced, schema_query);
    similar_asserts::assert_eq!(batches_coerced, batches_query);
}

pub fn schema_get<C>(conn: &mut C, table_name: &str, spec: ArrowGenSpec)
where
    C: Connection + SchemaEdit + SchemaGet,
{
    let mut rng = rand_chacha::ChaCha8Rng::from_seed([0; 32]);
    let (schema, batches) = generate_batch(spec, &mut rng);

    load_into_table(conn, schema.clone(), &batches, table_name).unwrap();
    let schema = coerce::coerce_schema(schema, &C::coerce_type, Some(false));

    let schema_introspection = conn.table_get(table_name).unwrap();
    let schema_introspection = coerce::coerce_schema(schema_introspection, |_| None, Some(false));
    similar_asserts::assert_eq!(schema, schema_introspection);
}

pub fn schema_edit<C>(conn: &mut C, table_name: &str, spec: ArrowGenSpec)
where
    C: Connection + SchemaEdit + SchemaGet,
{
    let mut rng = rand_chacha::ChaCha8Rng::from_seed([0; 32]);
    let (schema, _) = generate_batch(spec, &mut rng);

    let table_name2 = table_name.to_string() + "2";

    let _ = conn.table_drop(&table_name2);

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

pub fn ident_escaping<C>(conn: &mut C, table_name_prefix: &str)
where
    C: Connection + SchemaEdit + SchemaGet,
{
    let gibberish = r#"--"'--''!""@"""$#'''%^&*()_+?><\"\""#;
    let table_name = table_name_prefix.to_string() + gibberish;
    let _ = conn.table_drop(&table_name);

    // table_create
    let field = Field::new(gibberish, arrow::datatypes::DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field]));
    conn.table_create(&table_name, schema.clone()).unwrap();

    // table_list
    let tables = conn.table_list().unwrap();
    assert!(tables.contains(&table_name));

    // table_get
    let schema_introspections = conn.table_get(&table_name).unwrap();
    similar_asserts::assert_eq!(
        schema.fields()[0].name(),
        schema_introspections.fields()[0].name()
    );

    // append
    let mut appender = conn.append(&table_name).unwrap();
    let batch = {
        let mut builder = Int64Builder::new();
        builder.append_value(1);
        let array = Arc::new(builder.finish()) as ArrayRef;
        RecordBatch::try_new(schema.clone(), vec![array]).unwrap()
    };
    appender.append(batch).unwrap();
}

#[allow(dead_code)]
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

    // This should be quick and not load the full result.
    // ... but I guess not - it takes a long time.
    // ... I don't know why. Maybe my impl is wrong, but I cannot find a reason why.
    // ... Maybe it is the postgres that hangs before returning the first result batch?
    // ... Maybe it tries to return the full result and not in batches?
}
