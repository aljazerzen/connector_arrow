# Connector Arrow

An database client for many databases, exposing an interface that produces Apache Arrow.

[Documentation](https://docs.rs/connector_arrow)

Inspired by [ConnectorX](https://github.com/sfu-db/connector-x), with focus on being a Rust library, instead of a Python library.

To be more specific, this crate:

- does not support multiple destinations, but only [arrow](https://crates.io/crates/arrow),
- does not include parallelism, but allows downstream creates to implement it themselves,
- does not include connection pooling, but allows downstream creates to implement it themselves,
- uses minimal dependencies (it even disables default features).

None of the sources are enabled by default, use `src_` features to enable them.

## Support matrix

| Source             | Feature, dependency                                           | Querying | Streaming | Introspect | Query params | Writing | Tested |
| ------------------ | ------------------------------------------------------------- | -------- | --------- | ---------- | ------------ | ------- | ------ |
| SQLite             | `src_sqlite` [rusqlite](https://crates.io/crates/rusqlite)    | x        |           | x          |              |         | x      |
| DuckDB             | `src_duckdb` [duckdb](https://creates.io/crates/duckdb)       | x        |           |            |              |         | x      |
| PostgreSQL         | `src_postgres` [postgres](https://creates.io/crates/postgres) | x        | x         |            |              |         | x      |
| Redshift           | `src_postgres` [postgres](https://creates.io/crates/postgres) | x        | x         |            |              |         |        |
| MySQL              |                                                               |          |           |            |              |         |        |
| MariaDB            |                                                               |          |           |            |              |         |        |
| ClickHouse         |                                                               |          |           |            |              |         |        |
| SQL Server         |                                                               |          |           |            |              |         |        |
| Azure SQL Database |                                                               |          |           |            |              |         |        |
| Oracle             |                                                               |          |           |            |              |         |        |
| Big Query          |                                                               |          |           |            |              |         |        |

## Types

When converting non-arrow data sources (everything except DuckDB), only a subset of all possible arrows types is produced. Here is a list of what it is currently possible to produce:

- [ ] Null
- [x] Boolean
- [x] Int8
- [x] Int16
- [x] Int32
- [x] Int64
- [x] UInt8
- [x] UInt16
- [x] UInt32
- [x] UInt64
- [ ] Float16
- [x] Float32
- [x] Float64
- [ ] Timestamp
- [ ] Date32
- [ ] Date64
- [ ] Time32
- [ ] Time64
- [ ] Duration
- [ ] Interval
- [ ] Binary
- [ ] FixedSizeBinary
- [x] LargeBinary
- [ ] Utf8
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
