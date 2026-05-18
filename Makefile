.PHONY: all test test-race bench bench-atomicstruct bench-shardqueue vet help

all: vet test test-race

help:
	@echo "Available targets:"
	@echo "  test                - Run all tests"
	@echo "  test-race           - Run all tests with race detector"
	@echo "  bench               - Run benchmarks for all packages"
	@echo "  bench-atomicstruct  - Run benchmarks for pkg/atomicstruct"
	@echo "  bench-shardqueue    - Run benchmarks for pkg/shardqueue"
	@echo "  vet                 - Run go vet"
	@echo "  all                 - Run vet, test, and test-race"

test:
	go test ./...

test-race:
	go test -race ./...

bench: bench-atomicstruct bench-shardqueue

bench-atomicstruct:
	go test -bench=. -benchmem ./pkg/atomicstruct/...

bench-shardqueue:
	go test -bench=. -benchmem ./pkg/shardqueue/...

vet:
	go vet ./...
