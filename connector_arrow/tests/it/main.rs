#[cfg(feature = "src_dummy")]
mod test_arrow;
#[cfg(feature = "src_bigquery")]
mod test_bigquery;
#[cfg(feature = "src_csv")]
mod test_csv;
#[cfg(feature = "src_mssql")]
mod test_mssql;
#[cfg(feature = "src_mysql")]
mod test_mysql;
#[cfg(feature = "src_oracle")]
mod test_oracle;
#[cfg(feature = "dst_arrow2")]
mod test_polars;
#[cfg(feature = "src_postgres")]
mod test_postgres;
