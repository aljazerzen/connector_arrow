# Connector Arrow

A flexible database client that converts data into Apache Arrow format across various databases.

This is achieved by defining API traits (i.e. `Connection`) and implementing them for objects used
by a various database client crates (i.e. `rusqlite::Connection`). `connector_arrow` treats
databases as "data stores for Arrow format," aligning with the philosophy of data interoperability.

[Documentation](https://docs.rs/connector_arrow)

## Key features

- **Query**: Query databases and retrieve results in Apache Arrow format.
- **Query Parameters**: Utilize Arrow type system for query parameters.
- **Temporal and Container Types**: Correctly handles temporal and container types.
- **Schema Introspection**: Query the database for schema of specific tables.
- **Schema Migration**: Basic schema migration commands.
- **Append**: Write `arrow::record_batch::RecordBatch` into database tables.

Based on [ConnectorX](https://github.com/sfu-db/connector-x), but focus on being a Rust library,
instead of a Python library. This means that this crate:

- uses minimal dependencies (it even disables default features),
- does not support multiple destinations, but only [arrow](https://crates.io/crates/arrow),
- does not include parallelism, but allows downstream crates to implement it themselves,
- does not include connection pooling, but allows downstream crates to implement it themselves.

Similar to [ADBC](https://arrow.apache.org/docs/format/ADBC.html), but written in pure, safe Rust,
without need for dynamic linking of C libraries.

## Support matrix

|  | SQLite | DuckDB | PostgreSQL |
| --- | --- | --- | --- |
| feature | `src_sqlite` | `src_duckdb` | `src_postgres` |
| dependency | [rusqlite](https://crates.io/crates/rusqlite) | [duckdb](https://crates.io/crates/duckdb) | [postgres](https://crates.io/crates/postgres) |
| query | x | x | x |
| query params |  |  |  |
| schema get | x | x | x |
| schema edit | x | x | x |
| append | x | x | x |
| roundtrip: null & bool | x | x | x |
| roundtrip: int | x | x | x |
| roundtrip: uint | x | x | x |
| roundtrip: float | x | x | x |
| roundtrip: decimal | x |  | x |
| roundtrip: timestamp | x | x |  |
| roundtrip: date | x |  |  |
| roundtrip: time | x |  |  |
| roundtrip: duration | x |  |  |
| roundtrip: interval |  |  |  |
| roundtrip: utf8 | x | x | x |
| roundtrip: binary | x | x | x |
| roundtrip: empty |  | x | x |
| containers |  |  |  |

None of the sources are enabled by default, use features to enable them.

## Type coercion

Converting relational data from and to Apache Arrow comes with an inherent problem: type system of
any database does not map to arrow type system with a one-to-one relation. In practice this means
that:

- when querying data, multiple database types might be mapped into a single arrow type (for example,
  in PostgreSQL, both `VARCHAR` and `TEXT` will be mapped into `LargeUtf8`),
- when pushing data, multiple arrow types might be mapped into a single database type (for example,
  in PostgreSQL, both `Utf8` and `LargeUtf8` will be mapped into `TEXT`).

As a consequence, a roundtrip of pushing data to a database and querying it back must convert at
least some types. We call this process "type coercion" and is documented by
`Connection::coerce_type`.

When designing the mapping, we:

- guarantee that the roundtrip will not lose information (i.e. numbers losing precision),
- prioritize correctness and predictability over storage efficiency.

For example, most databases don't support sorting `UInt8`. To get around that, we could:

1.  store it into `Int8` by bounding the value to range `0..127`, which would lose information.
2.  store it into `Int8` by subtracting 128 (i.e. just reinterpreting the bytes as `Int8`). This
    coercion would be efficient, as the new type would not be any larger than the original, but it
    would be confusing as value 0 would be converted to -128 and value 255 to 127.
3.  store it into `Int16`. This uses more space, but it does not lose information or change the
    meaning of the stored value. So `connector_arrow` coerces `UInt8 => Int16`, `UInt16 => Int32`,
    `UInt32 => Int64` and `UInt64 => Decimal128(20, 0)`.

## Dynamic vs static types

Another problem when converting between two type systems is a mismatch between static and dynamic
type parameters. For example, arrow type `Decimal128(20, 4)` defines precision and scale statically,
in the schema, which implies that it must the same for all values in the array. On the other hand,
PostgreSQL type `NUMERIC` has dynamic precision and scale, which means that each value may have a
different pair of parameters. PostgreSQL does allow specifying parameters statically with
`NUMERIC(20, 4)`, but that is only available for column definition and not for query results. Even
when selecting directly from a table, the result will only the information that this column is
`NUMERIC`.

This problem is even more prevalent with SQLite, which has a fully dynamic type system. This means
that any table or result column may contain multiple different types. It is possible the declare
table column types, but that information is not validated at all (i.e. you could set the type to
`PEANUT_BUTTER(42)`) and is only used to determine the [type
affinity](https://www.sqlite.org/datatype3.html#type_affinity) of the column.

This problem can be solved in the following ways:

1.  **Convert to some other type.** For PostgreSQL `NUMERIC`, that would mean conversion to a
    decimal textual representation, encoded as `Utf8`. This is generally slow and inconvenient to
    work with.
2.  **Buffer and infer.** If we are able receive and buffer all of the result data, we could infer
    the type information from the data. This is obviously not possible if we want to support
    streaming results, as it would defeat all of the benefits of streaming. It might happen that
    values don't have uniform types. In that case, we can reject the result entirely and inform the
    user to cast the data into a uniform type before returning the results. We can also try to cast
    the values to some uniform type, but is generally slow, error prone and that might not be
    possible.
3.  **Infer from the first batch.** If we don't want to rule out streaming, we can opt for buffering
    only the first batch of data and inferring the types from that. If any of the subsequent batches
    turns out to have different types, we again have the options: reject or cast.

At the moment, `connector_arrow` does not have a common way of solving this problem. Connector for
SQLite uses option 2 and other connectors don't support types with dynamic types parameters.

Preferred way of solving the problem is option 3: infer from the first batch and reject non-uniform
types. This option will result in more errors being presented to the users. We justify this decision
with observation that because conversion between the two type systems is non trivial, silent
conversion into a different type would mean unpredictable downstream behavior, which would require
user attention anyway. By catching the problem early, we have the option to provide informative
error messages and hints on how to specify the result type explicitly.
