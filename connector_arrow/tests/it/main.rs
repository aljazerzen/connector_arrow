mod generator;
mod spec;
mod tests;
mod util;

#[cfg(feature = "src_duckdb")]
mod test_duckdb;
#[cfg(feature = "src_postgres")]
mod test_postgres_extended;
#[cfg(feature = "src_postgres")]
mod test_postgres_simple;
#[cfg(feature = "src_sqlite")]
mod test_sqlite;
