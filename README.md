# Connector Arrow

A flexible database client that converts data into Apache Arrow format across various databases.

This is achieved by defining API traits (i.e. `Connection`) and implementing them for objects used
by a various database client crates (i.e. `rusqlite::Connection`). `connector_arrow` treats
databases as "data stores for Arrow format," aligning with the philosophy of data interoperability.

[Documentation](https://docs.rs/connector_arrow)

## Key features

- **Query**: Query databases and retrieve results in Apache Arrow format.
- **Query Parameters**: Utilize Arrow type system for query parameters (WIP).
- **Streaming**: Receive results as a stream of `arrow::record_batch::RecordBatch`.
- **Temporal and Container Types**: Correctly handles temporal and container types (WIP).
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
| Feature | `src_sqlite` | `src_duckdb` | `src_postgres` |
| Dependency | [rusqlite](https://crates.io/crates/rusqlite) | [duckdb](https://crates.io/crates/duckdb) | [postgres](https://crates.io/crates/postgres) |
| Query | x | x | x |
| Query params |  |  |  |
| Streaming |  |  | x |
| Temporal types |  | x |  |
| Container types |  | x |  |
| Schema get | x | x | x |
| Schema edit | x | x | x |
| Append | x | x | x |

None of the sources are enabled by default, use features to enable them.

## Types

When converting from non-arrow data sources (everything except DuckDB), only a subset of all arrows
types is produced. Here is a list of supported types:

- [x] Null
- [x] Boolean
- [x] Int8
- [x] Int16
- [x] Int32
- [x] Int64
- [x] UInt8
- [x] UInt16
- [x] UInt32
- [x] UInt64
- [x] Float16
- [x] Float32
- [x] Float64
- [x] Timestamp
- [x] Date32
- [x] Date64
- [x] Time32
- [x] Time64
- [x] Duration
- [x] Interval
- [x] Binary
- [x] FixedSizeBinary
- [x] LargeBinary
- [x] Utf8
- [x] LargeUtf8
- [ ] List
- [ ] FixedSizeList
- [ ] LargeList
- [ ] Struct
- [ ] Union
- [ ] Dictionary
- [ ] Decimal128
- [ ] Decimal256
- [ ] Map
- [ ] RunEndEncoded

This restriction mostly has to do with non-trivial mapping of Arrow type into Rust native types.
