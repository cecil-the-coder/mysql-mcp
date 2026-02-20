.PHONY: install-hooks check fmt clippy test build

install-hooks:
	pre-commit install

check:
	cargo check

fmt:
	cargo fmt

clippy:
	cargo clippy -- -D warnings

test:
	cargo test

build:
	cargo build --release
