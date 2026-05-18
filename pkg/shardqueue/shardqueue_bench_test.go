package shardqueue

import (
	"strconv"
	"sync/atomic"
	"testing"
)

// noop process function so the measured cost is queue overhead, not user work.
func noop(msg interface{}) error { return nil }

func newBenchQueue(b *testing.B, numShard, queueSize int) *Shardqueue {
	b.Helper()
	sq, err := NewShardQueue(numShard, queueSize)
	if err != nil {
		b.Fatal(err)
	}
	sq.Start(noop)
	b.Cleanup(func() { sq.Stop() })
	return sq
}

// Serial benchmarks — per-call cost with one producer.

func BenchmarkShard_StringKey(b *testing.B) {
	sq := newBenchQueue(b, 8, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sq.Shard("routing-key", i)
	}
}

func BenchmarkShard_IntKey(b *testing.B) {
	sq := newBenchQueue(b, 8, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sq.Shard(i, i)
	}
}

func BenchmarkShard_BytesKey(b *testing.B) {
	sq := newBenchQueue(b, 8, 1024)
	key := []byte("routing-key")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sq.Shard(key, i)
	}
}

// Shard count sweep — shows how the dispatch cost scales as shards grow.
func BenchmarkShard_ShardCount(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			sq := newBenchQueue(b, n, 1024)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = sq.Shard(i, i)
			}
		})
	}
}

// Parallel benchmarks — many producers, the realistic case for shardqueue.

// Keys distributed across shards so workers drain in parallel.
func BenchmarkShardParallel_Distributed(b *testing.B) {
	sq := newBenchQueue(b, 16, 1024)
	var ctr atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := ctr.Add(1)
			_ = sq.Shard(i, i)
		}
	})
}

// Worst case: all producers push to the same key, hashing to one shard.
// Only one worker drains and every sender contends on its channel.
func BenchmarkShardParallel_HotKey(b *testing.B) {
	sq := newBenchQueue(b, 16, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = sq.Shard("hot", 1)
		}
	})
}

// Pure-function microbenchmarks.

func BenchmarkHashKeyToShard(b *testing.B) {
	key := []byte("routing-key")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hashKeyToShard(key, 16)
	}
}

func BenchmarkConvertKeyToBytes(b *testing.B) {
	b.Run("string", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = convertKeyToBytes("hello")
		}
	})
	b.Run("int", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = convertKeyToBytes(42)
		}
	})
	b.Run("bytes", func(b *testing.B) {
		key := []byte("hello")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = convertKeyToBytes(key)
		}
	})
}
