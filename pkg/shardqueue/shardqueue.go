package shardqueue

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"log"
	"math"
	"sync"
)

var (
	ErrInvalidNumShard  = errors.New("numShard must be > 0")
	ErrInvalidQueueSize = errors.New("queueSize must be >= 0")
	ErrNotStarted       = errors.New("shardqueue not started")
	ErrClosed           = errors.New("shardqueue closed")
)

type Shardqueue struct {
	numShard  int
	queueSize int
	queue     []chan interface{}
	done      chan struct{}
	wg        sync.WaitGroup

	mu      sync.RWMutex
	started bool
	closed  bool
}

type processFunc func(i interface{}) error

func NewShardQueue(numShard, queueSize int) (*Shardqueue, error) {
	if numShard <= 0 {
		return nil, ErrInvalidNumShard
	}
	if queueSize < 0 {
		return nil, ErrInvalidQueueSize
	}
	return &Shardqueue{
		numShard:  numShard,
		queueSize: queueSize,
		queue:     make([]chan interface{}, numShard),
		done:      make(chan struct{}),
	}, nil
}

// Start spins up one worker per shard. It is idempotent and safe to call
// multiple times concurrently; only the first call has effect.
func (sq *Shardqueue) Start(fn processFunc) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.started || sq.closed {
		return
	}
	sq.started = true
	for i := 0; i < sq.numShard; i++ {
		sq.queue[i] = make(chan interface{}, sq.queueSize)
		sq.wg.Add(1)
		go sq.shardWorker(i, sq.queue[i], fn)
	}
}

// Stop signals every worker to drain its remaining buffered messages and
// exit, then blocks until they do. It is idempotent.
//
// Shard channels are intentionally not closed: closing them would race with
// in-flight senders that already passed the closed-flag check and would
// panic with "send on closed channel". Workers exit by observing done.
func (sq *Shardqueue) Stop() {
	sq.mu.Lock()
	if sq.closed {
		sq.mu.Unlock()
		return
	}
	sq.closed = true
	close(sq.done)
	sq.mu.Unlock()
	sq.wg.Wait()
}

// Shard routes msg to the shard determined by routingKey. It returns
// ErrNotStarted before Start is called and ErrClosed once Stop has been
// invoked. Otherwise it blocks until the message is queued or Stop unblocks
// senders that were waiting on a full shard.
func (sq *Shardqueue) Shard(routingKey interface{}, msg interface{}) error {
	sq.mu.RLock()
	if sq.closed {
		sq.mu.RUnlock()
		return ErrClosed
	}
	if !sq.started {
		sq.mu.RUnlock()
		return ErrNotStarted
	}
	shard := hashKeyToShard(convertKeyToBytes(routingKey), sq.numShard)
	ch := sq.queue[shard]
	done := sq.done
	sq.mu.RUnlock()

	select {
	case ch <- msg:
		return nil
	case <-done:
		return ErrClosed
	}
}

func (sq *Shardqueue) shardWorker(id int, ch chan interface{}, fn processFunc) {
	defer sq.wg.Done()
	for {
		select {
		case msg := <-ch:
			if err := fn(msg); err != nil {
				log.Printf("Shard %d process error: %v", id, err)
			}
		case <-sq.done:
			// Stop requested: drain whatever is still buffered, then exit.
			for {
				select {
				case msg := <-ch:
					if err := fn(msg); err != nil {
						log.Printf("Shard %d process error: %v", id, err)
					}
				default:
					log.Printf("Shard %d done", id)
					return
				}
			}
		}
	}
}

func hashKeyToShard(key []byte, numShard int) int {
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(numShard))
}

func convertKeyToBytes(key interface{}) []byte {
	switch v := key.(type) {
	case []byte:
		return v

	case string:
		return []byte(v)

	case int:
		return intToBytes(int64(v))
	case int32:
		return intToBytes(int64(v))
	case int64:
		return intToBytes(v)

	case uint:
		return uintToBytes(uint64(v))
	case uint32:
		return uintToBytes(uint64(v))
	case uint64:
		return uintToBytes(v)

	case float64:
		return floatToBytes(v)
	case float32:
		return floatToBytes(float64(v))

	default:
		return []byte("defaultRoutingKey")
	}
}

func intToBytes(n int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}

func uintToBytes(n uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

func floatToBytes(f float64) []byte {
	bits := math.Float64bits(f)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}
