package wal

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestWriter(t *testing.T, opts Options) *Writer {
	t.Helper()
	dir := t.TempDir()
	opts.Dir = dir
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestAppendAssignsMonotonicSeq(t *testing.T) {
	w := newTestWriter(t, Options{})
	seq1, _, err := w.Append(kCmd, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	seq2, _, err := w.Append(kTrade, []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	seq3, _, err := w.Append(kExec, []byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	if !(seq1 == 1 && seq2 == 2 && seq3 == 3) {
		t.Fatalf("got %d %d %d, want 1 2 3", seq1, seq2, seq3)
	}
}

func TestSyncMarksDurable(t *testing.T) {
	w := newTestWriter(t, Options{FlushInterval: time.Hour}) // long ticker — force via Sync
	seq, durable, err := w.Append(kCmd, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	select {
	case <-durable:
	default:
		t.Fatal("durable not closed after Sync")
	}
	if w.DurableSeqNo() != seq {
		t.Fatalf("DurableSeqNo=%d, want %d", w.DurableSeqNo(), seq)
	}
}

func TestGroupCommitFireBatchSize(t *testing.T) {
	w := newTestWriter(t, Options{FlushInterval: time.Hour, MaxBatchRecords: 5})
	durables := make([]<-chan struct{}, 0, 5)
	for i := 0; i < 5; i++ {
		_, d, _ := w.Append(kCmd, []byte{byte(i)})
		durables = append(durables, d)
	}
	// Batch full → flusher fires without waiting for ticker.
	for i, d := range durables {
		select {
		case <-d:
		case <-time.After(time.Second):
			t.Fatalf("durable %d not closed within 1s", i)
		}
	}
}

func TestGroupCommitFireTicker(t *testing.T) {
	w := newTestWriter(t, Options{FlushInterval: 20 * time.Millisecond, MaxBatchRecords: 1024})
	_, d, _ := w.Append(kCmd, []byte("x"))
	select {
	case <-d:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("durable not closed within 200ms (ticker fail)")
	}
}

func TestRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxSegmentBytes: 256, FlushInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	// Each frame is 17 + len(payload). 6 records of 128B payload = 6*145 =
	// 870 bytes well over the 256-byte threshold → at least one rotation.
	for i := 0; i < 6; i++ {
		payload := make([]byte, 128)
		_, _, err := w.Append(kCmd, payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	// Close drains the flusher AND completes any pending rotation — Sync alone
	// races with the rotation step that runs after the durable-close.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 2 {
		t.Fatalf("expected ≥ 2 segment files after rotation, got %d", len(files))
	}
}

func TestRotationContinuesSequence(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxSegmentBytes: 100, FlushInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, _, _ = w.Append(kCmd, make([]byte, 100))
	}
	_ = w.Sync()
	lastSeq := w.NextSeqNo() - 1
	_ = w.Close()

	// Re-open: next seq should NOT collide; we let user pass StartSeqNo for that.
	w2, err := Open(Options{Dir: dir, StartSeqNo: lastSeq})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	seq, _, _ := w2.Append(kCmd, []byte("a"))
	if seq != lastSeq+1 {
		t.Fatalf("seq=%d, want %d", seq, lastSeq+1)
	}
	// And the new segment file index is one past the existing max.
	idx, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) < 2 {
		t.Fatalf("expected ≥ 2 segments after reopen, got %d", len(idx))
	}
}

func TestAppendRejectsOversizedPayload(t *testing.T) {
	w := newTestWriter(t, Options{})
	huge := make([]byte, MaxPayloadBytes+1)
	if _, _, err := w.Append(kCmd, huge); err != ErrPayloadTooLarge {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
	// A payload exactly at the limit is accepted.
	if _, _, err := w.Append(kCmd, make([]byte, MaxPayloadBytes)); err != nil {
		t.Fatalf("payload at limit should be accepted, got %v", err)
	}
}

func TestAppendAfterCloseFails(t *testing.T) {
	w := newTestWriter(t, Options{})
	_ = w.Close()
	if _, _, err := w.Append(kCmd, []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestConcurrentAppendsPreserveSeq(t *testing.T) {
	w := newTestWriter(t, Options{FlushInterval: 5 * time.Millisecond})
	var wg sync.WaitGroup
	const N = 200
	durables := make([]<-chan struct{}, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, d, err := w.Append(kCmd, []byte{byte(i)})
			if err != nil {
				t.Errorf("append: %v", err)
			}
			durables[i] = d
		}(i)
	}
	wg.Wait()
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	// All durables must be closed after Sync.
	for i, d := range durables {
		select {
		case <-d:
		case <-time.After(time.Second):
			t.Fatalf("durable %d not closed", i)
		}
	}
}

// TestRecordsLandInFile uses the Reader to verify written records are readable.
func TestRecordsLandInFile(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, FlushInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_, _, _ = w.Append(kCmd, []byte("c1"))
	_, _, _ = w.Append(kTrade, []byte("t1"))
	_, _, _ = w.Append(kExec, []byte("e1"))
	_ = w.Sync()
	_ = w.Close()

	// Read back via raw file scan.
	files, _ := os.ReadDir(dir)
	if len(files) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(files))
	}
	f, err := os.Open(filepath.Join(dir, files[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var (
		count int
		buf   []byte
	)
	for {
		fr, b, err := ReadFrame(f, buf)
		buf = b
		if err != nil {
			break
		}
		count++
		_ = fr
	}
	if count != 3 {
		t.Fatalf("expected 3 frames, got %d", count)
	}
}

func TestDisableFsyncStillMarksDurable(t *testing.T) {
	// With DisableFsync the file.Sync() syscall is skipped but records
	// still flow through the flusher → durable channel must still close on
	// every batch so an ack-after-WRITE path unblocks.
	w := newTestWriter(t, Options{
		FlushInterval: 2 * time.Millisecond,
		DisableFsync:  true,
	})
	_, durable, err := w.Append(kCmd, []byte("c1"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-durable:
	case <-time.After(time.Second):
		t.Fatal("durable not closed after one tick with DisableFsync=true")
	}
	if w.DurableSeqNo() != 1 {
		t.Fatalf("DurableSeqNo=%d, want 1", w.DurableSeqNo())
	}
}

func TestDisableFsyncRecordsStillLandInFile(t *testing.T) {
	// Critical guarantee: even without fsync, frames must reach the file —
	// the OS page cache + a clean Close() flushes them on graceful shutdown,
	// which is what replay relies on for process-restart recovery.
	dir := t.TempDir()
	w, err := Open(Options{
		Dir:           dir,
		FlushInterval: 2 * time.Millisecond,
		DisableFsync:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, _, _ = w.Append(kCmd, []byte("payload"))
	}
	_ = w.Sync()
	_ = w.Close()

	files, _ := os.ReadDir(dir)
	if len(files) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(files))
	}
	f, err := os.Open(filepath.Join(dir, files[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var (
		count int
		buf   []byte
	)
	for {
		fr, b, err := ReadFrame(f, buf)
		buf = b
		if err != nil {
			break
		}
		count++
		_ = fr
	}
	if count != 5 {
		t.Fatalf("expected 5 frames in file, got %d", count)
	}
}

// fakeFsyncMetrics counts fsync observations + their durations so a test
// can prove the Sync() call was actually skipped (every observed duration
// will be ~0 when DisableFsync=true).
type fakeFsyncMetrics struct {
	mu       sync.Mutex
	calls    int
	maxDurNs int64
}

func (m *fakeFsyncMetrics) RecordWALFsync(d time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if d.Nanoseconds() > m.maxDurNs {
		m.maxDurNs = d.Nanoseconds()
	}
	_ = err
}

func TestDisableFsyncSkipsKernelSyncCall(t *testing.T) {
	// Direct evidence that file.Sync() was bypassed: observed fsync duration
	// stays in the "near-zero" band. With DisableFsync=false a real fsync on
	// any disk we test on takes at least tens of microseconds; with the flag
	// on, we measure only the time around the conditional itself, which is
	// well under that bar.
	m := &fakeFsyncMetrics{}
	w := newTestWriter(t, Options{
		FlushInterval: 2 * time.Millisecond,
		DisableFsync:  true,
		Metrics:       m,
	})
	for i := 0; i < 20; i++ {
		_, _, _ = w.Append(kCmd, []byte("x"))
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls == 0 {
		t.Fatal("flusher never observed a batch — DisableFsync should still record metrics")
	}
	// 1ms ceiling — generous; real Windows FlushFileBuffers is routinely
	// 500µs+ even on idle disk. We expect << 1ms here because no fsync ran.
	const ceilingNs = int64(time.Millisecond)
	if m.maxDurNs >= ceilingNs {
		t.Fatalf("DisableFsync=true should make per-batch sync ~0; saw maxDur=%dns (>=1ms)", m.maxDurNs)
	}
}
