package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

// segmentName builds the standard wal segment filename for index n —
// `wal-000017.log`. The 6-digit zero-padded index keeps directory listings in
// lexicographic = chronological order, which both humans and the recovery
// reader rely on.
func segmentName(index uint64) string {
	return fmt.Sprintf("wal-%06d.log", index)
}

var segmentRE = regexp.MustCompile(`^wal-(\d{6,})\.log$`)

// listSegments returns the segment indices present in dir, sorted ascending.
// It silently ignores any non-segment file so an operator backup (`wal-*.bak`)
// does not poison the list.
func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var idx []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := segmentRE.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		n, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			continue
		}
		idx = append(idx, n)
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i] < idx[j] })
	return idx, nil
}

// ensureDir creates dir (and any missing parents) with 0o755. Pre-existing
// directory is not an error — the WAL keeps writing into it.
func ensureDir(dir string) error {
	return os.MkdirAll(dir, 0o755)
}

// segmentPath joins dir + segmentName(index).
func segmentPath(dir string, index uint64) string {
	return filepath.Join(dir, segmentName(index))
}
