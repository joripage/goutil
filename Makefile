.PHONY: all test test-race bench vet help

all: vet test test-race

help:
	@echo "Available targets:"
	@echo "  test       - Run all tests"
	@echo "  test-race  - Run all tests with race detector"
	@echo "  bench      - Run benchmarks for pkg/atomicstruct"
	@echo "  vet        - Run go vet"
	@echo "  all        - Run vet, test, and test-race"

test:
	go test ./...

test-race:
	go test -race ./...

bench:
	go test -bench=. -benchmem ./pkg/atomicstruct/...

vet:
	go vet ./...
