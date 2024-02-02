export POSTGRES_URL := "postgres://root:root@localhost:5432/dummy"
export SQLITE_URL := "sqlite://dbs/sqlite.db"
export DUCKDB_URL := "../dbs/duckdb.db"


default:
    just --list

build-release:
    cargo build --release --features all

build-debug:
    cargo build --features all

features_test := "--features=src_csv,src_postgres,src_dummy,src_sqlite,src_duckdb"
test:
    cargo nextest run {{features_test}}
    cargo fmt --check
    cargo clippy {{features_test}} -- -D warnings

test-feature-gate:
    cargo check --features branch --no-default-features
    cargo check --features src_postgres
    cargo check --features src_mysql
    cargo check --features src_mssql
    cargo check --features src_sqlite
    cargo check --features src_oracle
    cargo check --features src_csv
    cargo check --features src_dummy
    cargo clippy -- -D warnings
    cargo clippy --features all -- -D warnings

