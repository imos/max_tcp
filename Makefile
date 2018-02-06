usage:
	@echo 'Usage: make (build|test)'
.PHONY: usage

build:
	cargo build --release
.PHONY: build

test: cargo-test integration-test
.PHONY: test

cargo-test:
	cargo test --release
.PHONY: cargo-test

integration-test:
	bash ./integration_test.sh
.PHONY: integration-test
