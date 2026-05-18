package shardqueue

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewShardQueue_Validation(t *testing.T) {
	if _, err := NewShardQueue(0, 10); !errors.Is(err, ErrInvalidNumShard) {
		t.Errorf("Expected ErrInvalidNumShard for numShard=0, got %v", err)
	}
	if _, err := NewShardQueue(-1, 10); !errors.Is(err, ErrInvalidNumShard) {
		t.Errorf("Expected ErrInvalidNumShard for numShard=-1, got %v", err)
	}
	if _, err := NewShardQueue(4, -1); !errors.Is(err, ErrInvalidQueueSize) {
		t.Errorf("Expected ErrInvalidQueueSize for queueSize=-1, got %v", err)
	}
	if _, err := NewShardQueue(4, 0); err != nil {
		t.Errorf("queueSize=0 (unbuffered) should be allowed, got %v", err)
	}
}

func TestShard_BeforeStart(t *testing.T) {
	sq, err := NewShardQueue(2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := sq.Shard("k", "v"); !errors.Is(err, ErrNotStarted) {
		t.Errorf("Expected ErrNotStarted, got %v", err)
	}
}

func TestShard_AfterStop(t *testing.T) {
	sq, _ := NewShardQueue(2, 4)
	sq.Start(func(msg interface{}) error { return nil })
	sq.Stop()
	if err := sq.Shard("k", "v"); !errors.Is(err, ErrClosed) {
		t.Errorf("Expected ErrClosed, got %v", err)
	}
}

func TestStop_DrainsBufferedMessages(t *testing.T) {
	sq, _ := NewShardQueue(4, 100)
	var processed atomic.Int32
	sq.Start(func(msg interface{}) error {
		time.Sleep(time.Millisecond) // simulate work
		processed.Add(1)
		return nil
	})

	const total = 200
	for i := 0; i < total; i++ {
		if err := sq.Shard(i, i); err != nil {
			t.Fatalf("Shard %d: %v", i, err)
		}
	}
	sq.Stop()

	if got := processed.Load(); got != total {
		t.Errorf("Stop did not drain queues: processed %d / %d", got, total)
	}
}

func TestStop_Idempotent(t *testing.T) {
	sq, _ := NewShardQueue(2, 4)
	sq.Start(func(msg interface{}) error { return nil })
	sq.Stop()
	// A second Stop must not panic (would happen if channels were closed twice).
	sq.Stop()
}

func TestStop_WithoutStart(t *testing.T) {
	sq, _ := NewShardQueue(2, 4)
	// Must not panic even though Start was never called.
	sq.Stop()
	if err := sq.Shard("k", "v"); !errors.Is(err, ErrClosed) {
		t.Errorf("Expected ErrClosed after Stop-without-Start, got %v", err)
	}
}

func TestShard_OrderingPerKey(t *testing.T) {
	sq, _ := NewShardQueue(8, 64)

	var mu sync.Mutex
	seen := map[string][]int{}
	sq.Start(func(msg interface{}) error {
		m := msg.(payload)
		mu.Lock()
		seen[m.key] = append(seen[m.key], m.seq)
		mu.Unlock()
		return nil
	})

	keys := []string{"alpha", "beta", "gamma", "delta"}
	const perKey = 50
	for _, k := range keys {
		for i := 0; i < perKey; i++ {
			if err := sq.Shard(k, payload{key: k, seq: i}); err != nil {
				t.Fatalf("Shard: %v", err)
			}
		}
	}
	sq.Stop()

	for _, k := range keys {
		got := seen[k]
		if len(got) != perKey {
			t.Errorf("key %s: expected %d msgs, got %d", k, perKey, len(got))
			continue
		}
		for i, v := range got {
			if v != i {
				t.Errorf("key %s: out-of-order at index %d (got %d)", k, i, v)
				break
			}
		}
	}
}

type payload struct {
	key string
	seq int
}

// Concurrent Shard during Stop must never panic. Some sends may return
// ErrClosed; the rest must be processed.
func TestShard_ConcurrentWithStop(t *testing.T) {
	sq, _ := NewShardQueue(4, 16)
	var processed atomic.Int32
	sq.Start(func(msg interface{}) error {
		processed.Add(1)
		return nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				_ = sq.Shard(id, j)
			}
		}(i)
	}

	time.Sleep(5 * time.Millisecond)
	sq.Stop()
	wg.Wait()
}
