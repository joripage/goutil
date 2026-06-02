package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeNRecords helper: append n command records then Sync + Close.
func writeNRecords(t *testing.T, dir string, n int, opts Options) {
	t.Helper()
	opts.Dir = dir
	if opts.FlushInterval == 0 {
		opts.FlushInterval = 5 * time.Millisecond
	}
	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		_, _, err := w.Append(kCmd, []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestScanAllReadsEveryRecord(t *testing.T) {
	dir := t.TempDir()
	writeNRecords(t, dir, 10, Options{})

	var count int
	last, results, err := ScanAll(dir, func(f Frame) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 10 || last != 10 {
		t.Fatalf("got count=%d last=%d, want 10/10", count, last)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 segment result, got %d", len(results))
	}
}

func TestScanAllMidStreamCorruptionFails(t *testing.T) {
	dir := t.TempDir()
	// Two segments via small MaxSegmentBytes.
	writeNRecords(t, dir, 8, Options{MaxSegmentBytes: 100})

	// Corrupt the FIRST segment to simulate mid-stream damage.
	idx, _ := listSegments(dir)
	if len(idx) < 2 {
		t.Skipf("need ≥ 2 segments to test mid-stream corruption (got %d)", len(idx))
	}
	first := segmentPath(dir, idx[0])
	data, _ := os.ReadFile(first)
	// flip a bit in the middle.
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(first, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err := ScanAll(dir, func(Frame) error { return nil })
	if err == nil {
		t.Fatal("expected mid-stream corruption error, got nil")
	}
}

func TestScanAllTolerateTailTorn(t *testing.T) {
	dir := t.TempDir()
	writeNRecords(t, dir, 5, Options{})

	// Truncate the only segment by 3 bytes — simulates `kill -9` mid-write.
	idx, _ := listSegments(dir)
	path := segmentPath(dir, idx[len(idx)-1])
	fi, _ := os.Stat(path)
	if err := os.Truncate(path, fi.Size()-3); err != nil {
		t.Fatal(err)
	}

	// ScanAll should NOT return an error — tail-torn is tolerated.
	count := 0
	_, results, err := ScanAll(dir, func(Frame) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 4 {
		t.Fatalf("expected 4 good frames pre-torn-tail, got %d", count)
	}
	if !results[len(results)-1].IsTailFailure() {
		t.Fatal("expected tail failure flag on last segment")
	}
}

func TestSegmentReaderEmptyDir(t *testing.T) {
	dir := t.TempDir()
	last, _, err := ScanAll(dir, func(Frame) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if last != 0 {
		t.Fatalf("empty dir should give last=0, got %d", last)
	}
}

func TestSegmentReaderMissingDir(t *testing.T) {
	last, _, err := ScanAll(filepath.Join(t.TempDir(), "does-not-exist"), func(Frame) error { return nil })
	if err != nil {
		t.Fatalf("missing dir treated as empty: %v", err)
	}
	if last != 0 {
		t.Fatalf("missing dir should give last=0, got %d", last)
	}
}
