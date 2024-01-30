export POSTGRES_URL := "postgres://root:root@localhost:5432/dummy"
export SQLITE_URL := "sqlite://sqlite.db"

export MYSQL_HOST := "localhost"
export MYSQL_PORT := "3306"
export MYSQL_USER := "root"
export MYSQL_PASSWORD := "root"
export MYSQL_DB := "dummy"

export MSSQL_HOST := "localhost"
export MSSQL_USER := "sa"
export MSSQL_PASSWORD := "Wordpass123##"
export MSSQL_DB := "db"


default:
    just --list

build-release:
    cargo build --release --features all

build-debug:
    cargo build --features all

features_test := "--features=src_csv,src_postgres,src_dummy,src_sqlite"
test:
    cargo nextest run {{features_test}}
    cargo fmt --check
    cargo clippy {{features_test}} -- -D warnings

test-feature-gate:
    cargo check --features src_postgres
    cargo check --features src_mysql
    cargo check --features src_mssql
    cargo check --features src_sqlite
    cargo check --features src_oracle
    cargo check --features src_csv
    cargo check --features src_dummy
