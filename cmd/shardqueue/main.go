package main

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joripage/goutil/pkg/shardqueue"
)

type event struct {
	key string
	seq int
}

func main() {
	patternOrderingPerKey()
	patternThroughputAndDrain()
	patternErrorReturns()
	patternConcurrentProducers()
}

// --- Pattern 1: ordering preserved per key ------------------------------
//
// The headline feature. Messages with the same routing key always land on
// the same shard, and each shard has a single worker — so events for one
// key are delivered in arrival order, even though shards run in parallel.
func patternOrderingPerKey() {
	fmt.Println("Pattern 1: ordering preserved per key")

	sq, _ := shardqueue.NewShardQueue(4, 64)

	var mu sync.Mutex
	seen := map[string][]int{}

	sq.Start(func(msg interface{}) error {
		e := msg.(event)
		mu.Lock()
		seen[e.key] = append(seen[e.key], e.seq)
		mu.Unlock()
		return nil
	})

	keys := []string{"orderA", "orderB", "orderC", "orderD"}
	const perKey = 5
	for _, k := range keys {
		for i := 0; i < perKey; i++ {
			_ = sq.Shard(k, event{key: k, seq: i})
		}
	}
	sq.Stop()

	for _, k := range keys {
		got := seen[k]
		fmt.Printf("  %s: %v  in-order=%t\n", k, got, sort.IntsAreSorted(got))
	}
	fmt.Println()
}

// --- Pattern 2: throughput and Stop drain -------------------------------
//
// Send a burst of messages, then call Stop. Stop waits for every buffered
// message to be processed before returning — no need for sleeps or signal
// handlers to keep the process alive.
func patternThroughputAndDrain() {
	fmt.Println("Pattern 2: throughput + Stop drains buffered messages")

	const total = 10000
	sq, _ := shardqueue.NewShardQueue(8, 1024)

	var processed atomic.Int32
	begin := time.Now()
	sq.Start(func(msg interface{}) error {
		processed.Add(1)
		return nil
	})

	for i := 0; i < total; i++ {
		_ = sq.Shard(strconv.Itoa(i), i)
	}
	sq.Stop()

	fmt.Printf("  sent=%d processed=%d elapsed=%s\n\n",
		total, processed.Load(), time.Since(begin).Round(time.Millisecond))
}

// --- Pattern 3: error returns -------------------------------------------
//
// Shard returns ErrNotStarted before Start, and ErrClosed after Stop —
// instead of panicking or deadlocking the caller. This lets producers
// react to lifecycle changes (e.g. shed load, log, retry on a new queue).
func patternErrorReturns() {
	fmt.Println("Pattern 3: error returns for lifecycle violations")

	sq, _ := shardqueue.NewShardQueue(2, 4)

	err := sq.Shard("k", "v")
	fmt.Printf("  before Start: err=%v  is ErrNotStarted=%t\n",
		err, errors.Is(err, shardqueue.ErrNotStarted))

	sq.Start(func(msg interface{}) error { return nil })
	if err := sq.Shard("k", "v"); err != nil {
		fmt.Printf("  while running: unexpected err=%v\n", err)
	} else {
		fmt.Println("  while running: ok")
	}

	sq.Stop()
	err = sq.Shard("k", "v")
	fmt.Printf("  after Stop:   err=%v  is ErrClosed=%t\n\n",
		err, errors.Is(err, shardqueue.ErrClosed))
}

// --- Pattern 4: concurrent producers ------------------------------------
//
// Many goroutines push messages to the same queue simultaneously. The
// shard mutex serialises enqueues per-shard, and Stop still drains
// everything that was successfully sent. Run with -race to verify safety.
func patternConcurrentProducers() {
	fmt.Println("Pattern 4: concurrent producers")

	const producers = 50
	const perProducer = 200
	sq, _ := shardqueue.NewShardQueue(8, 512)

	var processed atomic.Int32
	sq.Start(func(msg interface{}) error {
		processed.Add(1)
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				_ = sq.Shard(strconv.Itoa(id), event{key: strconv.Itoa(id), seq: i})
			}
		}(p)
	}
	wg.Wait()
	sq.Stop()

	want := producers * perProducer
	fmt.Printf("  producers=%d, each sent %d => expected=%d, processed=%d\n",
		producers, perProducer, want, processed.Load())
}
