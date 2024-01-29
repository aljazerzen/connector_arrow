# Connector Arrow

Load data from many data sources into Apache Arrow, the fastest way.

Fork of [ConnectorX](https://github.com/sfu-db/connector-x), with focus on being a Rust library, instead of a Python library.

[Documentation](https://docs.rs/connector_arrow)

## How does Connector Arrow achieve a lightning speed while keeping the memory footprint low?

We observe that existing solutions more or less do data copy multiple times when downloading the data.
Additionally, implementing a data intensive application in Python brings additional cost.

Connector Arrow is written in Rust and follows "zero-copy" principle.
This allows it to make full use of the CPU by becoming cache and branch predictor friendly. Moreover, the architecture of Connector Arrow ensures the data will be copied exactly once, directly from the source to the destination.

## How does Connector Arrow download the data?

Upon receiving the query, e.g. `SELECT * FROM lineitem`, Connector Arrow will first issue a `LIMIT 1` query `SELECT * FROM lineitem LIMIT 1` to get the schema of the result set.

Then, if `partition_on` is specified, Connector Arrow will issue `SELECT MIN($partition_on), MAX($partition_on) FROM (SELECT * FROM lineitem)` to know the range of the partition column.
After that, the original query is split into partitions based on the min/max information, e.g. `SELECT * FROM (SELECT * FROM lineitem) WHERE $partition_on > 0 AND $partition_on < 10000`.
Connector Arrow will then run a count query to get the partition size (e.g. `SELECT COUNT(*) FROM (SELECT * FROM lineitem) WHERE $partition_on > 0 AND $partition_on < 10000`). If the partition
is not specified, the count query will be `SELECT COUNT(*) FROM (SELECT * FROM lineitem)`.

Finally, Connector Arrow will use the schema info as well as the count info to allocate memory and download data by executing the queries normally.

Once the downloading begins, there will be one thread for each partition so that the data are downloaded in parallel at the partition level. The thread will issue the query of the corresponding
partition to the database and then write the returned data to the destination row-wise or column-wise (depends on the database) in a streaming fashion.

## Sources

Supported:

- [x] PostgreSQL
- [x] SQLite

Partially-supported (not tested):

- [x] Mysql
- [x] Mariadb (through mysql protocol)
- [x] Redshift (through postgres protocol)
- [x] Clickhouse (through mysql protocol)
- [x] SQL Server
- [x] Azure SQL Database (through mssql protocol)
- [x] Oracle
- [x] Big Query

## Destinations

- [x] [arrow](https://crates.io/crates/arrow)
- [x] [arrow2](https://crates.io/crates/arrow2)
