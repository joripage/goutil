package wal

import (
	"bytes"
	"strconv"
	"testing"
	"time"
)

// payloadSizes drives the size sweeps — small records dominate the framing
// overhead, large ones the memcpy/crc cost.
var payloadSizes = []int{64, 256, 1024, 4096}

func makePayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i)
	}
	return p
}

// newBenchWriter opens a writer in a temp dir with fsync disabled, so the
// measured cost is the framing + enqueue path rather than disk latency (which
// is hardware-dependent and covered separately by BenchmarkAppendDurable).
func newBenchWriter(b *testing.B, opts Options) *Writer {
	b.Helper()
	opts.Dir = b.TempDir()
	if opts.MaxBatchRecords == 0 {
		opts.MaxBatchRecords = 1024
	}
	w, err := Open(opts)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = w.Close() })
	return w
}

// --- Pure framing: encode -----------------------------------------------

func BenchmarkEncodeFrame(b *testing.B) {
	for _, n := range payloadSizes {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			payload := makePayload(n)
			var buf []byte // reused across iterations — amortised allocation
			b.SetBytes(int64(n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf = EncodeFrame(buf, uint64(i), kCmd, payload)
			}
		})
	}
}

// --- Pure framing: decode -----------------------------------------------

func BenchmarkReadFrame(b *testing.B) {
	for _, n := range payloadSizes {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			frame := EncodeFrame(nil, 1, kCmd, makePayload(n))
			r := bytes.NewReader(frame)
			var dst []byte // reused payload buffer
			b.SetBytes(int64(n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.Reset(frame)
				_, dst, _ = ReadFrame(r, dst)
			}
		})
	}
}

// --- Append (enqueue) path ----------------------------------------------
//
// Cost of Append itself: encode the frame + lock + append to the pending
// slice. fsync is disabled so the flusher just drains to the page cache.
func BenchmarkAppend(b *testing.B) {
	for _, n := range payloadSizes {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			w := newBenchWriter(b, Options{
				FlushInterval: 10 * time.Millisecond,
				DisableFsync:  true,
			})
			payload := makePayload(n)
			b.SetBytes(int64(n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := w.Append(kCmd, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Concurrent producers — the realistic write pattern. Many goroutines append
// while the single flusher batches them (group commit).
func BenchmarkAppendParallel(b *testing.B) {
	w := newBenchWriter(b, Options{
		FlushInterval: 10 * time.Millisecond,
		DisableFsync:  true,
	})
	payload := makePayload(256)
	b.SetBytes(256)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := w.Append(kCmd, payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// --- Full durability path -----------------------------------------------
//
// Append + wait for the record to be fsynced. This is disk-bound — it measures
// the real ack-after-flush latency on the host's storage, dominated by fsync.
// Group commit means concurrent load amortises the syscall; this serial form
// is the worst case (one fsync per record).
func BenchmarkAppendDurable(b *testing.B) {
	w := newBenchWriter(b, Options{FlushInterval: time.Millisecond})
	payload := makePayload(256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, durable, err := w.Append(kCmd, payload)
		if err != nil {
			b.Fatal(err)
		}
		<-durable
	}
}
