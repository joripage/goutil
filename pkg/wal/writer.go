package wal

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ErrUnavailable is returned by Append after a fsync failure has halted the
// writer. Callers typically map this to a "storage unavailable" reject.
var ErrUnavailable = errors.New("wal: unavailable (fsync failure)")

// ErrClosed is returned by Append after Close has run.
var ErrClosed = errors.New("wal: closed")

// Options configure a Writer.
type Options struct {
	// Dir is the directory under which segment files live, e.g.
	// `./data/wal/`. It is created if missing.
	Dir string

	// MaxSegmentBytes rotates the active segment when its size on disk reaches
	// this threshold. Zero falls back to DefaultMaxSegmentBytes (128 MiB).
	MaxSegmentBytes int64

	// FlushInterval bounds the group-commit window — fsync is forced at least
	// this often even when traffic is light. Zero falls back to
	// DefaultFlushInterval (10 ms), bounding the recovery-point objective.
	FlushInterval time.Duration

	// MaxBatchRecords forces a flush when this many records have queued, even
	// before FlushInterval expires — prevents starvation under burst load.
	// Zero falls back to DefaultMaxBatchRecords (64).
	MaxBatchRecords int

	// StartSeqNo seeds the monotonic sequence — used by recovery to continue
	// from where a snapshot/WAL combination left off. Zero means start at 1.
	StartSeqNo uint64

	// Metrics receives an observation on every fsync. nil ⇒ no metrics
	// emitted; the diagnostic counters on Writer stay accurate either way.
	Metrics Metrics

	// DisableFsync skips the per-batch file.Sync() call. Writes still go to
	// the OS page cache and reach disk eventually, so WAL files remain
	// complete across a graceful process restart. They do NOT survive kernel
	// panic / power loss / forced reboot — durability is RELAXED from
	// "ack-after-flush" to "ack-after-write" when this is true.
	//
	// Trade-off matrix:
	//   Process crash (panic, OOM, kill):  records survive  (OS flushes cache)
	//   Container restart:                 records survive  (OS flushes cache)
	//   Kernel panic / BSOD:               records LOST     (cache evicted)
	//   Power loss without UPS:            records LOST     (cache evicted)
	//
	// Use ONLY when raw throughput trumps durability — e.g. perf-bench runs
	// on Windows where FlushFileBuffers saturates the write-back cache and
	// stalls under steady load. Default false preserves the durability contract.
	DisableFsync bool
}

// Metrics is the WAL-side observability hook. The interface is declared here
// so the wal package stays dependency-free; pass any implementation, or leave
// Options.Metrics nil for a no-op.
type Metrics interface {
	// RecordWALFsync observes one fsync. err is nil on success; non-nil on
	// failure (the Writer is also about to halt, but the metric should
	// still record the failure).
	RecordWALFsync(d time.Duration, err error)
}

// nopMetrics is the no-op Metrics used when Options.Metrics is nil.
type nopMetrics struct{}

func (nopMetrics) RecordWALFsync(time.Duration, error) {}

// Defaults for unset Options fields.
const (
	DefaultMaxSegmentBytes = 128 << 20
	DefaultFlushInterval   = 10 * time.Millisecond
	DefaultMaxBatchRecords = 64
)

// Writer is the append-side of the WAL. It runs one flusher goroutine that
// owns the active segment file and does all writes / fsyncs / rotations.
type Writer struct {
	opts Options

	// Atomic counters & flags — readable without holding the mutex.
	seqNo      atomic.Uint64 // next seq to issue
	durableSeq atomic.Uint64 // last seq that has fsynced
	closed     atomic.Bool

	// Diagnostic counters for the last fsync. The flusher writes; readers load
	// them without coordination. They describe ONLY the most recent fsync — for
	// time-series visibility the caller should sample them periodically.
	lastFsyncNs   atomic.Int64 // wall time of the last fsync, ns
	lastBatchSize atomic.Int64 // records flushed in the last batch
	totalFsyncs   atomic.Uint64
	totalFsyncNs  atomic.Int64

	// haltErr captures the fatal IO/fsync error that puts the writer in the
	// halted state. Once set it is sticky — Append fails fast with
	// ErrUnavailable until the process restarts.
	haltErr atomic.Pointer[error]

	mu        sync.Mutex
	pending   []*record // unflushed batch, guarded by mu
	flushCh   chan struct{}
	stopCh    chan struct{}
	flushDone chan struct{}

	// durableCond broadcasts every time durableSeq advances OR the writer
	// halts. WaitDurable parks on it so consumers do not have to track per-
	// record channels for the common "wait until seqNo is on disk" case.
	durableMu   sync.Mutex
	durableCond *sync.Cond

	file       *os.File
	fileSize   int64
	segmentIdx uint64
}

// record is one in-flight WAL frame waiting for the next fsync to durable.
type record struct {
	seqNo   uint64
	framed  []byte
	durable chan struct{}
}

// Open creates (or opens) the WAL writer rooted at opts.Dir. The next segment
// to write is chosen by inspecting existing wal-NNNNNN.log files: a new one is
// created with the next index. Sequence numbers continue from opts.StartSeqNo
// (or 1 if zero) — recovery is the only intended caller that passes a non-zero
// value.
func Open(opts Options) (*Writer, error) {
	if opts.MaxSegmentBytes <= 0 {
		opts.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultFlushInterval
	}
	if opts.MaxBatchRecords <= 0 {
		opts.MaxBatchRecords = DefaultMaxBatchRecords
	}
	if opts.Metrics == nil {
		opts.Metrics = nopMetrics{}
	}
	if err := ensureDir(opts.Dir); err != nil {
		return nil, fmt.Errorf("wal: create dir %s: %w", opts.Dir, err)
	}

	w := &Writer{
		opts:      opts,
		flushCh:   make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		flushDone: make(chan struct{}),
	}
	w.durableCond = sync.NewCond(&w.durableMu)

	start := opts.StartSeqNo
	if start == 0 {
		start = 0 // first Append returns 1
	}
	w.seqNo.Store(start)
	w.durableSeq.Store(start)

	// Find the next segment index: max existing index + 1, or 0 for fresh dir.
	existing, err := listSegments(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("wal: scan dir: %w", err)
	}
	w.segmentIdx = 0
	if len(existing) > 0 {
		w.segmentIdx = existing[len(existing)-1] + 1
	}
	if err := w.openSegment(); err != nil {
		return nil, err
	}

	go w.flushLoop()
	return w, nil
}

func (w *Writer) openSegment() error {
	path := segmentPath(w.opts.Dir, w.segmentIdx)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open segment %s: %w", path, err)
	}
	w.file = f
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat segment: %w", err)
	}
	w.fileSize = fi.Size()
	return nil
}

// rotate closes the current segment, advances the index, and opens the next.
// Must be called from the flusher only (which serialises file access).
func (w *Writer) rotate() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: close on rotate: %w", err)
	}
	w.segmentIdx++
	return w.openSegment()
}

// NextSeqNo returns the seq that the next Append call will receive. It is
// read-only — callers must not assume a previously-returned value is still
// "next" if other appends have happened in between.
func (w *Writer) NextSeqNo() uint64 { return w.seqNo.Load() + 1 }

// DurableSeqNo returns the highest seqNo proven on stable storage so far.
func (w *Writer) DurableSeqNo() uint64 { return w.durableSeq.Load() }

// FsyncStats is a snapshot of the flusher's recent activity. Useful for a
// diagnostic dump to show whether fsync itself is the bottleneck.
type FsyncStats struct {
	LastFsync   time.Duration // wall time of the most recent fsync call
	LastBatchN  int64         // records flushed in the most recent batch
	TotalFsyncs uint64        // cumulative fsync count since Open
	AvgFsync    time.Duration // total fsync time / count
}

// FsyncStats returns a snapshot of the WAL flusher's recent timing. Safe to
// call concurrently with flushBatch (all loads are atomic).
func (w *Writer) FsyncStats() FsyncStats {
	n := w.totalFsyncs.Load()
	var avg time.Duration
	if n > 0 {
		avg = time.Duration(w.totalFsyncNs.Load() / int64(n))
	}
	return FsyncStats{
		LastFsync:   time.Duration(w.lastFsyncNs.Load()),
		LastBatchN:  w.lastBatchSize.Load(),
		TotalFsyncs: n,
		AvgFsync:    avg,
	}
}

// IsHalted reports whether the writer is in the post-fsync-fail terminal state.
func (w *Writer) IsHalted() bool { return w.haltErr.Load() != nil }

// HaltErr returns the underlying fsync error if halted, else nil.
func (w *Writer) HaltErr() error {
	if p := w.haltErr.Load(); p != nil {
		return *p
	}
	return nil
}

// Append queues one frame for the next fsync. It returns the assigned seqNo
// and a `durable` channel that closes once the frame is on stable storage —
// callers wait on it before releasing an ack-after-flush response. Callers
// MUST NOT mutate payload after the call; the writer owns the framed bytes.
func (w *Writer) Append(kind Kind, payload []byte) (seqNo uint64, durable <-chan struct{}, err error) {
	if w.closed.Load() {
		return 0, nil, ErrClosed
	}
	if w.haltErr.Load() != nil {
		return 0, nil, ErrUnavailable
	}

	seqNo = w.seqNo.Add(1)
	framed := EncodeFrame(nil, seqNo, kind, payload)
	rec := &record{seqNo: seqNo, framed: framed, durable: make(chan struct{})}

	w.mu.Lock()
	w.pending = append(w.pending, rec)
	full := len(w.pending) >= w.opts.MaxBatchRecords
	w.mu.Unlock()

	if full {
		// Non-blocking wake — the flusher coalesces if it's already running.
		select {
		case w.flushCh <- struct{}{}:
		default:
		}
	}
	return seqNo, rec.durable, nil
}

// Sync forces a flush + fsync of any queued records and blocks until it
// completes (or the writer is halted). Used by tests and by snapshot creation
// to checkpoint durably.
func (w *Writer) Sync() error {
	// Snapshot the tail before signalling so we know what to wait on. If the
	// pending slice is empty, return immediately — no work to do.
	w.mu.Lock()
	var last <-chan struct{}
	if n := len(w.pending); n > 0 {
		last = w.pending[n-1].durable
	}
	w.mu.Unlock()
	if last == nil {
		if err := w.HaltErr(); err != nil {
			return err
		}
		return nil
	}

	// Wake the flusher so it doesn't wait out the ticker.
	select {
	case w.flushCh <- struct{}{}:
	default:
	}
	<-last
	return w.HaltErr()
}

// Close stops the flusher, drains and fsyncs anything queued, and closes the
// active segment. It is safe to call once. Subsequent Append calls return
// ErrClosed.
func (w *Writer) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(w.stopCh)
	<-w.flushDone
	// Wake any WaitDurable parked beyond the durable horizon — they get ErrClosed.
	w.broadcastDurable()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// flushLoop runs the group-commit window. It exits after a final drain when
// Close is called.
func (w *Writer) flushLoop() {
	defer close(w.flushDone)
	ticker := time.NewTicker(w.opts.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			w.flushBatch()
			return
		case <-ticker.C:
			w.flushBatch()
		case <-w.flushCh:
			w.flushBatch()
			// Reset the ticker so the next periodic flush is a full interval
			// after this one — keeps RPO bound regardless of who triggered.
			ticker.Reset(w.opts.FlushInterval)
		}
	}
}

// flushBatch is the inner step: take the current pending slice, write every
// frame, fsync, then close durable channels and possibly rotate.
//
// On any error the writer is halted: durable channels stay OPEN so any caller
// blocked on Sync() unblocks via HaltErr(); future Append calls return
// ErrUnavailable.
func (w *Writer) flushBatch() {
	w.mu.Lock()
	if len(w.pending) == 0 {
		w.mu.Unlock()
		return
	}
	batch := w.pending
	w.pending = nil
	w.mu.Unlock()

	for _, r := range batch {
		n, err := w.file.Write(r.framed)
		if err != nil {
			w.halt(fmt.Errorf("wal: write segment: %w", err), batch)
			return
		}
		w.fileSize += int64(n)
	}
	// When DisableFsync is set, skip the kernel FlushFileBuffers call —
	// records stay in the OS page cache and reach disk on the OS schedule.
	// Still record a sample (duration ~0) so operators can see the flag is
	// taking effect via existing wal_fsync_* gauges.
	var (
		syncStart = time.Now()
		syncErr   error
	)
	if !w.opts.DisableFsync {
		syncErr = w.file.Sync()
	}
	syncDur := time.Since(syncStart)
	// Observe the fsync regardless of success — a halt event is exactly the
	// signal a "WAL fsync failing" alert is built to catch.
	w.opts.Metrics.RecordWALFsync(syncDur, syncErr)
	if syncErr != nil {
		w.halt(fmt.Errorf("wal: fsync: %w", syncErr), batch)
		return
	}
	syncNs := syncDur.Nanoseconds()
	w.lastFsyncNs.Store(syncNs)
	w.lastBatchSize.Store(int64(len(batch)))
	w.totalFsyncs.Add(1)
	w.totalFsyncNs.Add(syncNs)

	// Visible to readers after fsync: update durableSeq THEN close durables, so
	// any consumer that wakes on a closed channel and then re-reads DurableSeq
	// sees the new value.
	w.durableSeq.Store(batch[len(batch)-1].seqNo)
	w.broadcastDurable()
	for _, r := range batch {
		close(r.durable)
	}

	if w.fileSize >= w.opts.MaxSegmentBytes {
		if err := w.rotate(); err != nil {
			w.halt(err, nil)
			return
		}
	}
}

// halt enters the terminal failure state. It is sticky: only the FIRST halt
// captures the underlying error. The records that were in-flight when the
// failure happened and any later-queued records have their durable channels
// left OPEN — instead, the caller must check IsHalted/HaltErr after every Sync
// or after waiting on a durable channel with a timeout. We do close the durable
// channels of the failed batch right now so any caller already blocked on
// Sync() unblocks promptly and sees HaltErr().
func (w *Writer) halt(err error, failed []*record) {
	if w.haltErr.CompareAndSwap(nil, &err) {
		for _, r := range failed {
			// safe-close — flushBatch only enters here on the failure path.
			select {
			case <-r.durable:
			default:
				close(r.durable)
			}
		}
		w.broadcastDurable()
	}
}

// broadcastDurable wakes every goroutine parked in WaitDurable. Called after
// durableSeq advances OR after halt — both transitions must unblock waiters.
func (w *Writer) broadcastDurable() {
	w.durableMu.Lock()
	w.durableCond.Broadcast()
	w.durableMu.Unlock()
}

// WaitDurable blocks until DurableSeqNo() >= seqNo, or returns the halt error
// if the writer dies before reaching that seq. Returns ErrClosed if Close has
// been called and the seq is still not durable.
func (w *Writer) WaitDurable(seqNo uint64) error {
	if seqNo <= w.durableSeq.Load() {
		return nil
	}
	w.durableMu.Lock()
	defer w.durableMu.Unlock()
	for w.durableSeq.Load() < seqNo {
		if err := w.HaltErr(); err != nil {
			return err
		}
		if w.closed.Load() {
			return ErrClosed
		}
		w.durableCond.Wait()
	}
	return nil
}
