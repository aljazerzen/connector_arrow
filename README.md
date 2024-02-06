# Connector Arrow

An database client for many databases, exposing an interface that produces Apache Arrow.

[Documentation](https://docs.rs/connector_arrow)

Inspired by [ConnectorX](https://github.com/sfu-db/connector-x), with focus on being a Rust library, instead of a Python library.

To be more specific, this crate:

- does not support multiple destinations, but only [arrow](https://crates.io/crates/arrow),
- does not include parallelism, but allows downstream creates to implement it themselves,
- does not include connection pooling, but allows downstream creates to implement it themselves,
- uses minimal dependencies (it even disables default features).

## API features

- [x] Querying that returns `Vec<RecordBatch>`
- [x] Record batch streaming
- [ ] Database introspection
- [ ] Query parameters
- [ ] Writing to the data store

## Sources

None of the sources are enabled by default, use `src_` features to enable them:

- [x] SQLite (`src_sqlite`, using [rusqlite](https://crates.io/crates/rusqlite))
- [x] DuckDB (`src_duckdb`)
- [x] PostgreSQL (`src_postgres`)
- [x] Redshift (through postgres protocol, untested)
- [ ] MySQL
- [ ] MariaDB (through mysql protocol)
- [ ] ClickHouse (through mysql protocol)
- [ ] SQL Server
- [ ] Azure SQL Database (through mssql protocol)
- [ ] Oracle
- [ ] Big Query

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
