mod generator;
mod spec;
mod tests;
mod util;

#[cfg(feature = "src_duckdb")]
mod test_duckdb;
#[cfg(feature = "src_mysql")]
mod test_mysql;
#[cfg(feature = "src_postgres")]
mod test_postgres;
#[cfg(feature = "src_rusqlite")]
mod test_sqlite;
#[cfg(feature = "src_tiberius")]
mod test_tiberius;
