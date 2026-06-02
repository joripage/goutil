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

#### Benchmark atomicstruct

Mutex-guarded access is a few nanoseconds and allocation-free on the hot path.

| Benchmark | ns/op | allocs/op |
| --- | ---: | ---: |
| `Get` | 11 | 0 |
| `Read` | 11 | 0 |
| `Update` | 21 | 0 |
| `Set` | 24 | 0 |
| `GetParallel` | 42 | 0 |
| `MixedParallel` | 37 | 0 |
| `UpdateParallel` | 58 | 0 |

### [wal](./pkg/wal)

Generic Write-Ahead Log — an append-only, length-framed binary log with
monotonic sequence numbers, group commit, and ack-after-flush durability. It
stores raw `[]byte` records tagged with a caller-defined `Kind`, with `crc32c`
integrity, automatic segment rotation, and tail-torn tolerance on replay.
Drop-in crash-safe persistence: `Append` returns a channel that closes once
the record is fsynced, and `ScanAll` replays every segment back in order.

#### Benchmark wal

fsync latency is storage-bound — the serial durable figure is a worst case.

| Benchmark | Payload | ns/op | allocs/op |
| --- | ---: | ---: | ---: |
| `EncodeFrame` | 64 B | 28 | 1 |
| `EncodeFrame` | 4 KiB | 151 | 1 |
| `ReadFrame` | 64 B | 48 | 2 |
| `ReadFrame` | 1 KiB | 77 | 2 |
| `Append` (enqueue) | 256 B | 280 | 4 |
| `AppendParallel` (enqueue) | 256 B | 264 | 4 |
| `AppendDurable` (append + fsync, serial) | 256 B | 4,122,000 | 5 |

The enqueue path is sub-microsecond; a serial append-then-fsync is ~4 ms/op
because every record pays a full `fsync`. Group commit closes that gap — under
concurrent load many records share one `fsync` per `FlushInterval` window.

