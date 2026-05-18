# goutil

A small collection of focused, concurrency-oriented Go utilities. Each package
is self-contained and can be imported on its own.

```sh
go get github.com/joripage/goutil/pkg/<package>
```

For runnable usage examples of each package, see [`./cmd/<package>`](./cmd).

## Development

This repo includes a `Makefile` with common tasks:

```sh
make test        # go test ./...
make test-race   # go test -race ./...
make bench       # go test -bench=. -benchmem ./pkg/atomicstruct/...
make vet         # go vet ./...
make all         # vet + test + test-race
make help        # list available targets
```

On Windows without `make` installed, you can use one of:

- `choco install make` or `scoop install make`
- Or run the underlying `go` commands directly (see the [Makefile](./Makefile)).

## Packages

### [shardqueue](./pkg/shardqueue)

Sharded in-memory queue that preserves per-key ordering. Messages are routed
to one of N shards by a hash of their routing key, and each shard is drained
by a single worker — so events for the same key are processed in arrival
order. Useful for fan-out workloads where order matters per entity (e.g.
per-user, per-order) but not globally.

### [taskmanager](./pkg/taskmanager)

Run and manage long-lived, cancellable goroutines by ID. Starting a task with
an ID that is already in use cancels the previous one; tasks are cleaned up
on completion; panics inside a task are recovered. `GracefulShutdown` cancels
every running task and waits (with a timeout) for them to finish.

### [atomicstruct](./pkg/atomicstruct)

Generic, mutex-guarded wrapper around any value (`AtomicStruct[T]`). Gives
shared structs thread-safe `Get`/`Set`/`Read`/`Update` semantics, plus an
atomic check-then-act `UpdateWithCondition` for safe state transitions
without bespoke locking on every call site.
