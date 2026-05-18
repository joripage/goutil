# shardqueue

A small package to shard messages into per-key ordered queues. Same routing
key → same shard → single worker → FIFO order per key, parallelism across keys.

## Install

```sh
go get github.com/joripage/goutil/pkg/shardqueue
```

Requires Go 1.18+.

## Usage

A runnable example with 4 patterns lives in [`cmd/shardqueue`](../../cmd/shardqueue).

```go
sq, err := shardqueue.NewShardQueue(numShard, queueSize)
if err != nil {
    log.Fatal(err)
}
sq.Start(func(msg interface{}) error {
    log.Println("process msg", msg)
    return nil
})

if err := sq.Shard("routing-key", payload); err != nil {
    // ErrNotStarted or ErrClosed
    log.Println(err)
}

// Stop closes every shard channel and waits for in-flight messages to
// finish processing before returning.
sq.Stop()
```

## API

| Method | Purpose |
| --- | --- |
| `NewShardQueue(numShard, queueSize int) (*Shardqueue, error)` | Construct a queue. Validates `numShard > 0`, `queueSize >= 0`. |
| `Start(fn processFunc)` | Spin up one worker per shard. Idempotent. |
| `Shard(routingKey, msg interface{}) error` | Route msg by key. Returns `ErrNotStarted` / `ErrClosed` when called outside the running window. |
| `Stop()` | Signal workers, drain buffered messages, wait for completion. Idempotent. |

Exported errors: `ErrInvalidNumShard`, `ErrInvalidQueueSize`, `ErrNotStarted`, `ErrClosed`.

## Guarantees

- **Ordering:** messages with the same routing key always land on the same
  shard, and each shard is processed by a single goroutine, so order is
  preserved per key.
- **Stop drains:** `Stop` blocks until every buffered message has been
  processed. Senders blocked on a full shard during `Stop` unblock with
  `ErrClosed` instead of panicking.
- **Idempotent lifecycle:** `Start` and `Stop` may each be called multiple
  times; only the first call has effect.

## Testing

```sh
go test -race ./...
```

## Notes — why FNV?

`fnv.New32a()` is one of the fastest hash functions in the Go stdlib and has
a good enough distribution for sharding. It is not cryptographically safe,
which is fine for in-process routing.

```text
BenchmarkFnv32a-8     195 ns/op
BenchmarkCRC32-8      230 ns/op
BenchmarkSHA1-8      1400 ns/op
BenchmarkSHA256-8    2000 ns/op
BenchmarkMD5-8       1100 ns/op
```

Swap to `xxhash` if you need faster, or `sha256` if you need cryptographic
safety:

| Hash | Speed | Distribution | Safe |
| --- | --- | --- | --- |
| `fnv.New32a()` | Fast | Good | No |
| `xxhash` | Very fast | Very good | No |
| `sha256` | Slow | Even | Yes |

See the [FNV hash function](https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function) on Wikipedia for the algorithm.
