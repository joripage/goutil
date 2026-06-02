# wal

A generic Write-Ahead Log: an append-only, length-framed binary log with
monotonic sequence numbers, group commit, and ack-after-flush durability. The
log is payload-agnostic — it stores raw `[]byte` records tagged with a
caller-defined `Kind`, so any project can drop it in for crash-safe persistence
without pulling in domain types.

## Install

```sh
go get github.com/joripage/goutil/pkg/wal
```

Requires Go 1.18+. Depends only on the standard library.

## Usage

A runnable example with 4 patterns lives in [`cmd/wal`](../../cmd/wal).

```go
// Caller defines what each record kind means — the WAL never interprets it.
const (
    KindOrder wal.Kind = 1
    KindTrade wal.Kind = 2
)

w, err := wal.Open(wal.Options{Dir: "./data/wal"})
if err != nil {
    log.Fatal(err)
}
defer w.Close()

// Append raw bytes. Returns the assigned seqNo and a channel that closes
// once the record is fsynced to stable storage.
seq, durable, err := w.Append(KindOrder, payload)
if err != nil {
    log.Println(err) // ErrUnavailable (fsync failed) or ErrClosed
}
<-durable // ack-after-flush: the record is now durable

// Replay everything back after a restart.
last, _, err := wal.ScanAll("./data/wal", func(f wal.Frame) error {
    switch f.Kind {
    case KindOrder:
        // decode f.Payload ...
    case KindTrade:
        // decode f.Payload ...
    }
    return nil
})
_ = last // highest seqNo seen — feed into Options.StartSeqNo to continue
```

## API

### Writer

| Method | Purpose |
| --- | --- |
| `Open(opts Options) (*Writer, error)` | Open/create the WAL rooted at `opts.Dir`. Starts the flusher goroutine. |
| `(*Writer) Append(kind Kind, payload []byte) (seqNo uint64, durable <-chan struct{}, err error)` | Queue one frame. `durable` closes after the record is fsynced. Caller must not mutate `payload` afterwards. |
| `(*Writer) Sync() error` | Force a flush + fsync of queued records and block until done (or halted). |
| `(*Writer) WaitDurable(seqNo uint64) error` | Block until `DurableSeqNo() >= seqNo`, or return the halt/close error. |
| `(*Writer) DurableSeqNo() uint64` | Highest seqNo proven on stable storage. |
| `(*Writer) NextSeqNo() uint64` | Seq the next `Append` will receive. |
| `(*Writer) FsyncStats() FsyncStats` | Snapshot of recent fsync timing (diagnostic). |
| `(*Writer) IsHalted() bool` / `HaltErr() error` | Terminal-failure state after an fsync error. |
| `(*Writer) Close() error` | Stop the flusher, drain + fsync, close the segment. Idempotent. |

### Reading / recovery

| Function | Purpose |
| --- | --- |
| `ScanAll(dir string, visit func(Frame) error) (uint64, []ScanResult, error)` | Iterate every segment in order, calling `visit` per frame. Returns the highest seqNo seen. |
| `ScanSegment(path string, visit func(Frame) error) (ScanResult, error)` | Scan one segment. |
| `OpenSegment(path string) (*SegmentReader, error)` | Stream frames out of one segment manually. |
| `EncodeFrame(buf []byte, seqNo uint64, kind Kind, payload []byte) []byte` | Encode one frame (low-level). |
| `ReadFrame(r io.Reader, dst []byte) (Frame, []byte, error)` | Decode one frame (low-level). |

### Options

| Field | Default | Meaning |
| --- | --- | --- |
| `Dir` | — | Directory holding segment files (created if missing). |
| `MaxSegmentBytes` | 128 MiB | Rotate the active segment at this size. |
| `FlushInterval` | 10 ms | Max group-commit window — fsync is forced at least this often. |
| `MaxBatchRecords` | 64 | Force a flush once this many records queue. |
| `StartSeqNo` | 0 → start at 1 | Seed the monotonic sequence — used by recovery to continue. |
| `Metrics` | nil (no-op) | Hook observing every fsync (`RecordWALFsync(d, err)`). |
| `DisableFsync` | false | Skip `file.Sync()` — see durability trade-off below. |

Exported errors: `ErrUnavailable`, `ErrClosed`, `ErrBadCRC`, `ErrTruncated`, `ErrBadPayloadLen`.

## On-disk format

Each record is a length-prefixed frame:

```text
┌────────────┬────────────┬────────────┬────────┬───────────────┐
│ length u32 │ crc32c u32 │ seqNo  u64 │ kind u8│ payload …     │
│   4 bytes  │   4 bytes  │   8 bytes  │  1 byte│ length bytes  │
└────────────┴────────────┴────────────┴────────┴───────────────┘
   17-byte header (FrameHeaderSize)
```

The CRC (Castagnoli / `crc32c`) covers `seqNo + kind + payload`. Length-prefixed
framing lets a reader resynchronise at frame boundaries even after a torn write.
Segment files are named `wal-000000.log`, `wal-000001.log`, … so a lexicographic
directory listing is also chronological.

## Guarantees

- **Ack-after-flush:** the `durable` channel from `Append` (and `WaitDurable`)
  only fires after `fsync` succeeds, so a closed channel means the record
  survives a crash. With `DisableFsync: true` this relaxes to ack-after-write.
- **Group commit:** many `Append` calls within a `FlushInterval` window share a
  single `fsync`, so throughput scales without one syscall per record.
- **Integrity:** every frame carries a `crc32c`; corruption is detected on read
  as `ErrBadCRC`.
- **Tail-torn tolerance:** a half-written frame at the end of the last segment
  (the expected result of `kill -9` mid-write) is tolerated on replay and flagged
  via `ScanResult.IsTailFailure()`. Corruption mid-stream is returned as an error.
- **Sticky halt:** once an `fsync` fails the writer halts permanently —
  subsequent `Append` calls return `ErrUnavailable` rather than silently losing
  data.

### `DisableFsync` trade-off

| Failure mode | Records survive? |
| --- | --- |
| Process crash (panic, OOM, kill) | ✅ (OS keeps the page cache) |
| Container restart | ✅ (same host/kernel) |
| Kernel panic / BSOD | ❌ |
| Power loss without UPS | ❌ |

The ✅ rows hold because a successful `write()` hands the bytes to the **kernel
page cache**, which outlives the process — a later read (even after a crash or
restart) still sees them. Durability is lost only when the kernel itself goes
down before writeback (panic / power loss), or if a "restart" moves to a
different host or discards the volume. Use only when raw throughput trumps
durability.

## Testing

```sh
go test -race ./...
```

## Benchmarks

```sh
go test -run '^$' -bench . -benchmem ./pkg/wal/
```

Measured on a 12th Gen Intel Core i7-12700 (Windows, `amd64`). Numbers are
indicative — fsync latency in particular is dominated by the host's storage.

| Benchmark | Payload | ns/op | Throughput | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| `EncodeFrame` | 64 B | 28 | 2.3 GB/s | 1 |
| `EncodeFrame` | 1 KiB | 54 | 19 GB/s | 1 |
| `EncodeFrame` | 4 KiB | 156 | 26 GB/s | 1 |
| `ReadFrame` | 64 B | 50 | 1.3 GB/s | 2 |
| `ReadFrame` | 1 KiB | 76 | 14 GB/s | 2 |
| `Append` (enqueue) | 64 B | 154 | — | 4 |
| `Append` (enqueue) | 256 B | 272 | — | 4 |
| `Append` (enqueue) | 1 KiB | 345 | — | 4 |
| `AppendParallel` (enqueue) | 256 B | 268 | — | 4 |
| `AppendDurable` (append + fsync, serial) | 256 B | ~1.6–4 M | — | 5 |

Key takeaway: the **enqueue** path (`Append` returning) is sub-microsecond, but
a **serial** `Append`-then-wait-for-fsync is on the order of milliseconds
because every record pays a full `fsync` (and it is storage-bound, so the
absolute figure swings widely between runs). Group commit is what closes that
gap — under concurrent load many records share one `fsync` within the
`FlushInterval` window, so durable throughput is orders of magnitude higher
than the serial figure suggests.

`Append` / `ReadFrame` allocations come from the per-record framed buffer and
the decoded payload; `EncodeFrame` is a single amortised allocation when its
buffer is reused.
