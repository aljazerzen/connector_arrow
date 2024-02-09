export POSTGRES_URL := "postgres://root:root@localhost:5432/dummy"

default:
    just --list

test:
    just fmt-check
    cargo clippy --features=all -- -D warnings
    cargo nextest run --features=all
    just test-feature-gate

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
    cargo clippy -- -D warnings
    cargo clippy --features all -- -D warnings

# format source files
fmt:
    cargo fmt
    yamlfix $(fd --hidden '.yml')

# validate that source files are formatted
fmt-check:
    cargo fmt --check
    yamlfix --check $(fd --hidden '.yml')

