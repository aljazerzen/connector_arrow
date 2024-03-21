export POSTGRES_URL := "postgres://user:pass@localhost:5432/db"
export MYSQL_URL := "mysql://root:pass@localhost:3306/db"
export TIBERIUS_URL := "tds://sa:passwordA1@localhost:1433"

default:
    just --list

test:
    just fmt-check
    cargo clippy --features=all -- -D warnings
    cargo nextest run --features=all
    just test-feature-gate
    cargo test --doc --features=all

# run tests, importants things first, for development
test-fast *ARGS:
    just fmt
    INSTA_FORCE_PASS=1 cargo nextest run --features=all --no-fail-fast {{ARGS}}
    cargo insta review
    cargo clippy --features=all

# test that all features work by themselves
test-feature-gate:
    cargo check --features src_postgres
    cargo check --features src_sqlite
    cargo check --features src_duckdb
    cargo check --features src_mysql
    cargo clippy -- -D warnings
    cargo clippy --features all -- -D warnings

# format source files
fmt:
    comrak --extension table,tasklist \
        --width 100 \
        --to commonmark \
        README.md --output README.md
    cargo fmt
    yamlfix $(fd --hidden '.yml')

# validate that source files are formatted
fmt-check:
    cargo fmt --check
    yamlfix --check $(fd --hidden '.yml')

