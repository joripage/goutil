package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/joripage/goutil/pkg/wal"
)

// Record kinds are defined by the caller — the WAL stores the tag byte but
// never interprets it. Here we pretend to be an order-matching service.
const (
	KindOrder wal.Kind = 1
	KindTrade wal.Kind = 2
)

func main() {
	patternAppendAndFlush()
	patternGroupCommit()
	patternReplay()
	patternCrashToleranceAndResume()
}

// tempDir makes an isolated WAL directory and a cleanup func, so each pattern
// runs against a fresh log.
func tempDir(name string) (string, func()) {
	dir, err := os.MkdirTemp("", "wal-example-"+name+"-")
	if err != nil {
		panic(err)
	}
	return dir, func() { _ = os.RemoveAll(dir) }
}

// --- Pattern 1: append + ack-after-flush --------------------------------
//
// The headline guarantee. Append returns a `durable` channel that closes only
// after the record is fsynced to disk — so blocking on it means "this record
// will survive a crash". This is how you gate a client ACK on durability.
func patternAppendAndFlush() {
	fmt.Println("Pattern 1: append + ack-after-flush")

	dir, cleanup := tempDir("flush")
	defer cleanup()

	w, err := wal.Open(wal.Options{Dir: dir, FlushInterval: 5 * time.Millisecond})
	if err != nil {
		panic(err)
	}
	defer w.Close()

	seq, durable, err := w.Append(KindOrder, []byte(`{"clOrdID":"A1","qty":100}`))
	if err != nil {
		panic(err)
	}
	<-durable // wait until fsynced

	fmt.Printf("  appended seq=%d, durableSeq=%d (record is now crash-safe)\n\n",
		seq, w.DurableSeqNo())
}

// --- Pattern 2: group commit --------------------------------------------
//
// Many appends inside one FlushInterval window share a single fsync. We push
// 1000 records, then a single Sync, and the fsync count stays far below the
// record count — that is what keeps throughput high without one syscall each.
func patternGroupCommit() {
	fmt.Println("Pattern 2: group commit (many appends, few fsyncs)")

	dir, cleanup := tempDir("group")
	defer cleanup()

	w, err := wal.Open(wal.Options{Dir: dir, FlushInterval: 10 * time.Millisecond})
	if err != nil {
		panic(err)
	}
	defer w.Close()

	const total = 1000
	begin := time.Now()
	for i := 0; i < total; i++ {
		if _, _, err := w.Append(KindOrder, []byte("order-payload")); err != nil {
			panic(err)
		}
	}
	if err := w.Sync(); err != nil {
		panic(err)
	}

	st := w.FsyncStats()
	fmt.Printf("  appended=%d  fsyncs=%d  avgFsync=%s  elapsed=%s\n\n",
		total, st.TotalFsyncs, st.AvgFsync.Round(time.Microsecond),
		time.Since(begin).Round(time.Millisecond))
}

// --- Pattern 3: replay after restart ------------------------------------
//
// Write a mix of records, close cleanly, then ScanAll reads every frame back
// in order. The caller switches on Frame.Kind to decode — exactly what a
// recovery routine does to rebuild in-memory state from the log.
func patternReplay() {
	fmt.Println("Pattern 3: replay after restart")

	dir, cleanup := tempDir("replay")
	defer cleanup()

	// Session 1: write some records, then shut down.
	w, err := wal.Open(wal.Options{Dir: dir, FlushInterval: 5 * time.Millisecond})
	if err != nil {
		panic(err)
	}
	_, _, _ = w.Append(KindOrder, []byte("order-1"))
	_, _, _ = w.Append(KindTrade, []byte("trade-1"))
	_, _, _ = w.Append(KindOrder, []byte("order-2"))
	_ = w.Sync()
	_ = w.Close()

	// Session 2: replay from the same directory.
	var orders, trades int
	last, _, err := wal.ScanAll(dir, func(f wal.Frame) error {
		switch f.Kind {
		case KindOrder:
			orders++
		case KindTrade:
			trades++
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("  replayed orders=%d trades=%d, highest seqNo=%d\n\n",
		orders, trades, last)
}

// --- Pattern 4: crash tolerance + resume sequence -----------------------
//
// Simulate `kill -9` mid-write by truncating the tail of the segment. ScanAll
// tolerates the torn tail (flagging it) instead of erroring, returns the last
// good seqNo, and we reopen with StartSeqNo so new records continue the
// sequence without collision.
func patternCrashToleranceAndResume() {
	fmt.Println("Pattern 4: crash tolerance + resume sequence")

	dir, cleanup := tempDir("crash")
	defer cleanup()

	w, err := wal.Open(wal.Options{Dir: dir, FlushInterval: 5 * time.Millisecond})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 5; i++ {
		_, _, _ = w.Append(KindOrder, []byte("payload"))
	}
	_ = w.Sync()
	_ = w.Close()

	// Corrupt the tail: drop the last 3 bytes of the only segment.
	files, _ := os.ReadDir(dir)
	seg := filepath.Join(dir, files[0].Name())
	fi, _ := os.Stat(seg)
	_ = os.Truncate(seg, fi.Size()-3)

	// Replay: the torn tail is tolerated, not fatal.
	good := 0
	last, results, err := wal.ScanAll(dir, func(wal.Frame) error {
		good++
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("  good frames=%d  lastGoodSeq=%d  tailTorn=%t\n",
		good, last, results[len(results)-1].IsTailFailure())

	// Resume numbering from the last durable seq so we never reissue a seqNo.
	w2, err := wal.Open(wal.Options{Dir: dir, StartSeqNo: last})
	if err != nil {
		panic(err)
	}
	defer w2.Close()
	seq, durable, _ := w2.Append(KindOrder, []byte("post-recovery"))
	<-durable
	fmt.Printf("  resumed: next record got seq=%d (continues past %d)\n\n", seq, last)
}
